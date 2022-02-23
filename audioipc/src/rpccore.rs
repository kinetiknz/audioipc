// Copyright © 2021 Mozilla Foundation
//
// This program is made available under an ISC-style license.  See the
// accompanying file LICENSE for details

use std::collections::VecDeque;
use std::io::{self, Result};
use std::mem::ManuallyDrop;
use std::sync::{Arc, Weak};

use crossbeam::queue::ArrayQueue;
use mio::Token;
use parking_lot::{Condvar, Mutex};

use crate::ipccore::EventLoopHandle;

// RPC message handler.  Implemented by ClientHandler (for Client)
// and ServerHandler (for Server).
pub(crate) trait Handler {
    type In;
    type Out;

    // Consume a request
    fn consume(&mut self, request: Self::In) -> Result<()>;

    // Produce a response
    fn produce(&mut self) -> Result<Option<Self::Out>>;
}

// Client RPC definition.  This supplies the expected message
// request and response types.
pub trait Client {
    type ServerMessage;
    type ClientMessage;
}

// Server RPC definition.  This supplies the expected message
// request and response types.  `process` is passed inbound RPC
// requests by the ServerHandler to be responded to by the server.
pub trait Server {
    type ServerMessage;
    type ClientMessage;

    fn process(&mut self, req: Self::ServerMessage) -> Self::ClientMessage;
}

// Each RPC Proxy `call` returns a blocking waitable ProxyResponse.
// `wait` produces the response received over RPC from the associated
// Proxy `call`.
pub struct ProxyResponse<Response> {
    response: Option<ResponseSlot<Response>>,
}

impl<Response> ProxyResponse<Response> {
    pub fn wait(self) -> Result<Response> {
        if let Some(slot) = self.response {
            if let Some(response) = slot.get() {
                return Ok(response);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "proxy recv error",
        ))
    }
}

// RPC Proxy that may be `clone`d for use by multiple owners/threads.
// A Proxy `call` arranges for the supplied request to be transmitted
// to the associated Server via RPC.  The response can be retrieved by
// `wait`ing on the returned ProxyResponse.
#[derive(Debug)]
pub struct Proxy<Request, Response> {
    inner: ManuallyDrop<Arc<Inner<Request, Response>>>,
    handle: Option<(EventLoopHandle, Token)>,
}

impl<Request, Response> Proxy<Request, Response> {
    pub fn call(&self, request: Request) -> ProxyResponse<Response> {
        // If inner's weak_count is zero, the ClientHandler has dropped its reference.
        if Arc::weak_count(&self.inner) == 0 {
            return ProxyResponse { response: None };
        }
        let slot = self.inner.send(request);
        self.wake_connection();
        ProxyResponse {
            response: Some(slot),
        }
    }

    pub(crate) fn connect_event_loop(&mut self, handle: EventLoopHandle, token: Token) {
        self.handle = Some((handle, token));
    }

    fn wake_connection(&self) {
        let (handle, token) = self
            .handle
            .as_ref()
            .expect("proxy not connected to event loop");
        handle.wake_connection(*token);
    }
}

impl<Request, Response> Clone for Proxy<Request, Response> {
    fn clone(&self) -> Self {
        Proxy {
            handle: self.handle.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<Request, Response> Drop for Proxy<Request, Response> {
    fn drop(&mut self) {
        trace!("Proxy drop, waking EventLoop");
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        if Arc::try_unwrap(inner).is_ok() {
            // Last Proxy alive, wake connection to clean up ClientHandler.
            self.wake_connection();
        }
    }
}

struct ResponseSlot<Response> {
    slot: Arc<(Mutex<Option<Response>>, Condvar)>,
}

impl<Response> ResponseSlot<Response> {
    fn new() -> Self {
        ResponseSlot {
            slot: Arc::new((Mutex::new(None), Condvar::new())),
        }
    }

    fn set(&self, response: Response) {
        *self.slot.0.lock() = Some(response);
        self.slot.1.notify_one();
    }

    fn get(&self) -> Option<Response> {
        // TODO: calling `get` twice deadlocks instead of errors.
        let mut slot = self.slot.0.lock();
        if slot.is_none() {
            self.slot.1.wait(&mut slot);
        }
        slot.take()
    }
}

impl<Response> Clone for ResponseSlot<Response> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

impl<Response> Drop for ResponseSlot<Response> {
    fn drop(&mut self) {
        self.slot.1.notify_one();
    }
}

#[derive(Debug)]
struct Inner<Request, Response> {
    outbound: ArrayQueue<(Request, ResponseSlot<Response>)>,
    inbound_responses: ArrayQueue<ResponseSlot<Response>>,
}

impl<Request, Response> Inner<Request, Response> {
    fn send(&self, request: Request) -> ResponseSlot<Response> {
        let slot = self.inbound_responses.pop().unwrap();
        if self.outbound.push((request, slot.clone())).is_err() {
            panic!()
        }
        slot
    }
}

// Client-specific Handler implementation.
// The IPC EventLoop Driver calls this to execute client-specific
// RPC handling.  Serialized messages sent via a Proxy are queued
// for transmission when `produce` is called.
// Deserialized messages are passed via `consume` to
// trigger response completion by sending the response via a channel
// connected to a ProxyResponse.
pub(crate) struct ClientHandler<C: Client> {
    outbound: Weak<Inner<C::ServerMessage, C::ClientMessage>>,
    inbound: ArrayQueue<ResponseSlot<C::ClientMessage>>,
}

impl<C: Client> Handler for ClientHandler<C> {
    type In = C::ClientMessage;
    type Out = C::ServerMessage;

    fn consume(&mut self, response: Self::In) -> Result<()> {
        trace!("ClientHandler::consume");
        if let Some(inbound) = self.inbound.pop() {
            inbound.set(response);
            match self.outbound.upgrade() {
                Some(outbound) => {
                    if outbound.inbound_responses.push(inbound).is_err() {
                        panic!();
                    }
                }
                None => error!("  --> no live proxies"),
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request/response mismatch",
            ));
        }

        Ok(())
    }

    fn produce(&mut self) -> Result<Option<Self::Out>> {
        trace!("ClientHandler::produce");

        match self.outbound.upgrade() {
            Some(outbound) => {
                // Try to get a new message
                if let Some((outbound, waiter)) = outbound.outbound.pop() {
                    trace!("  --> received request");
                    if self.inbound.push(waiter).is_err() {
                        panic!();
                    }
                    Ok(Some(outbound))
                } else {
                    // NOTE(kinetik): this happens once per flush_outbound to detect empty queue
                    trace!("  --> no request");
                    Ok(None)
                }
            }
            None => {
                trace!("  --> no live proxies, destroying ClientHandler");
                Err(io::ErrorKind::ConnectionAborted.into())
            }
        }
    }
}

pub(crate) fn make_client<C: Client>(
    cap: usize,
) -> (ClientHandler<C>, Proxy<C::ServerMessage, C::ClientMessage>) {
    let inbound_responses = ArrayQueue::new(cap);
    for _ in 0..cap {
        let slot = ResponseSlot::new();
        if inbound_responses.push(slot).is_err() {
            panic!();
        }
    }

    let inner = Arc::new(Inner {
        outbound: ArrayQueue::new(cap),
        inbound_responses,
    });

    let handler = ClientHandler::<C> {
        outbound: Arc::downgrade(&inner),
        inbound: ArrayQueue::new(cap),
    };

    let proxy = Proxy {
        handle: None,
        inner: ManuallyDrop::new(inner),
    };

    (handler, proxy)
}

// Server-specific Handler implementation.
// The IPC EventLoop Driver calls this to execute server-specific
// RPC handling.  Deserialized messages are passed via `consume` to the
// associated `server` for processing.  Server responses are then queued
// for RPC to the associated client when `produce` is called.
pub(crate) struct ServerHandler<S: Server> {
    server: S,
    in_flight: VecDeque<S::ClientMessage>,
}

impl<S: Server> Handler for ServerHandler<S> {
    type In = S::ServerMessage;
    type Out = S::ClientMessage;

    fn consume(&mut self, message: Self::In) -> Result<()> {
        trace!("ServerHandler::consume");
        let response = self.server.process(message);
        self.in_flight.push_back(response);
        Ok(())
    }

    fn produce(&mut self) -> Result<Option<Self::Out>> {
        trace!("ServerHandler::produce");

        // Return the ready response
        match self.in_flight.pop_front() {
            Some(res) => {
                trace!("  --> received response");
                Ok(Some(res))
            }
            None => {
                trace!("  --> no response ready");
                Ok(None)
            }
        }
    }
}

pub(crate) fn make_server<S: Server>(server: S) -> ServerHandler<S> {
    ServerHandler::<S> {
        server,
        in_flight: VecDeque::with_capacity(32),
    }
}
