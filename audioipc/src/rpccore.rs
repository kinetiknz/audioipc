// Copyright © 2021 Mozilla Foundation
//
// This program is made available under an ISC-style license.  See the
// accompanying file LICENSE for details

use std::cell::RefCell;
use std::io::{self, Result};
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex};
use std::{collections::VecDeque, sync::mpsc};

use mio::Token;

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

// RPC Client Proxy implementation.  ProxyRequest's Sender is connected to ProxyReceiver's Receiver,
// allowing the ProxyReceiver to wait on a response from the proxy.
type ProxyRequest<Request, Response> = (Request, ProxySender<Response>);
type ProxyReceiver<Request, Response> = mpsc::Receiver<ProxyRequest<Request, Response>>;

enum ProxySender<Response> {
    Channel(mpsc::Sender<Response>),
    DirectChannel(
        RefCell<Option<mpsc::Sender<Response>>>,
        Arc<Mutex<Option<mpsc::Sender<Response>>>>,
    ),
}

impl<Response> ProxySender<Response> {
    fn new(tx: mpsc::Sender<Response>) -> Self {
        Self::Channel(tx)
    }

    fn new_direct(
        tx: mpsc::Sender<Response>,
        used_slot: Arc<Mutex<Option<mpsc::Sender<Response>>>>,
    ) -> Self {
        Self::DirectChannel(RefCell::new(Some(tx)), used_slot)
    }

    fn send(&self, resp: Response) {
        match self {
            Self::Channel(tx) => {
                if let Err(e) = tx.send(resp) {
                    debug!("ProxySender::send failed: {:?}", e);
                }
            }
            Self::DirectChannel(tx, p) => {
                if let Err(e) = tx.borrow().as_ref().unwrap().send(resp) {
                    debug!("ProxySender::send failed: {:?}", e);
                }
                *p.lock().unwrap() = tx.borrow_mut().take();
            }
        }
    }
}

// Each RPC Proxy `call` returns a blocking waitable ProxyResponse.
// `wait` produces the response received over RPC from the associated
// Proxy `call`.
pub struct ProxyResponse<Response> {
    inner: RefCell<Option<mpsc::Receiver<Response>>>,
    used_slot: Option<Arc<Mutex<Option<mpsc::Receiver<Response>>>>>,
}

impl<Response> ProxyResponse<Response> {
    pub fn wait(&self) -> Result<Response> {
        match self.used_slot.as_ref() {
            Some(p) => {
                let inner = self.inner.take();
                let r = match inner.as_ref().unwrap().recv() {
                    Ok(resp) => Ok(resp),
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "proxy recv error",
                    )),
                };
                *p.lock().unwrap() = inner;
                r
            }
            None => match self.inner.borrow().as_ref().unwrap().recv() {
                Ok(resp) => Ok(resp),
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "proxy recv error",
                )),
            },
        }
    }
}

// RPC Proxy that may be `clone`d for use by multiple owners/threads.
// A Proxy `call` arranges for the supplied request to be transmitted
// to the associated Server via RPC.  The response can be retrieved by
// `wait`ing on the returned ProxyResponse.
#[derive(Debug)]
pub struct Proxy<Request, Response> {
    handle: Option<(EventLoopHandle, Token)>,
    tx: ManuallyDrop<mpsc::Sender<ProxyRequest<Request, Response>>>,
}

impl<Request, Response> Proxy<Request, Response> {
    pub fn call(&self, request: Request) -> ProxyResponse<Response> {
        let (tx, rx) = mpsc::channel();
        match self.tx.send((request, ProxySender::new(tx))) {
            Ok(_) => self.wake_connection(),
            Err(e) => debug!("Proxy::call error={:?}", e),
        }
        ProxyResponse {
            inner: RefCell::new(Some(rx)),
            used_slot: None,
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
            tx: self.tx.clone(),
        }
    }
}

impl<Request, Response> Drop for Proxy<Request, Response> {
    fn drop(&mut self) {
        trace!("Proxy drop, waking EventLoop");
        // Must drop Sender before waking the connection, otherwise
        // the wake may be processed before Sender is closed.
        unsafe { ManuallyDrop::drop(&mut self.tx) }
        self.wake_connection();
    }
}

// XXX: Docs
#[derive(Debug)]
pub struct DirectProxy<Request, Response> {
    handle: Option<(EventLoopHandle, Token)>,
    tx: ManuallyDrop<mpsc::Sender<ProxyRequest<Request, Response>>>,
    chan_tx: Arc<Mutex<Option<mpsc::Sender<Response>>>>,
    chan_rx: Arc<Mutex<Option<mpsc::Receiver<Response>>>>,
}

impl<Request, Response> DirectProxy<Request, Response> {
    pub fn call(&self, request: Request) -> ProxyResponse<Response> {
        let used_slot = self.chan_tx.clone();
        let tx = self.chan_tx.lock().unwrap().take().unwrap();
        match self
            .tx
            .send((request, ProxySender::new_direct(tx, used_slot)))
        {
            Ok(_) => self.wake_connection(),
            Err(e) => debug!("Proxy::call error={:?}", e),
        }
        let used_slot = Some(self.chan_rx.clone());
        let rx = self.chan_rx.lock().unwrap().take().unwrap();
        ProxyResponse {
            inner: RefCell::new(Some(rx)),
            used_slot,
        }
    }

    pub fn call_indirect(&self, request: Request) -> ProxyResponse<Response> {
        let (tx, rx) = mpsc::channel();
        match self.tx.send((request, ProxySender::new(tx))) {
            Ok(_) => self.wake_connection(),
            Err(e) => debug!("Proxy::call error={:?}", e),
        }
        ProxyResponse {
            inner: RefCell::new(Some(rx)),
            used_slot: None,
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

// impl<Request, Response> Clone for DirectProxy<Request, Response> {
//     fn clone(&self) -> Self {
//         Proxy {
//             handle: self.handle.clone(),
//             tx: self.tx.clone(),
//         }
//     }
// }

impl<Request, Response> Drop for DirectProxy<Request, Response> {
    fn drop(&mut self) {
        trace!("DirectProxy drop, waking EventLoop");
        // Must drop Sender before waking the connection, otherwise
        // the wake may be processed before Sender is closed.
        unsafe { ManuallyDrop::drop(&mut self.tx) }
        self.wake_connection();
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
    messages: ProxyReceiver<C::ServerMessage, C::ClientMessage>,
    in_flight: VecDeque<ProxySender<C::ClientMessage>>,
}

impl<C: Client> Handler for ClientHandler<C> {
    type In = C::ClientMessage;
    type Out = C::ServerMessage;

    fn consume(&mut self, response: Self::In) -> Result<()> {
        trace!("ClientHandler::consume");
        if let Some(complete) = self.in_flight.pop_front() {
            complete.send(response);
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

        // Try to get a new message
        match self.messages.try_recv() {
            Ok((request, response_tx)) => {
                trace!("  --> received request");
                self.in_flight.push_back(response_tx);
                Ok(Some(request))
            }
            Err(mpsc::TryRecvError::Empty) => {
                trace!("  --> no request");
                Ok(None)
            }
            Err(e) => {
                trace!("  --> client disconnected");
                Err(io::Error::new(io::ErrorKind::ConnectionAborted, e))
            }
        }
    }
}

pub(crate) fn make_client<C: Client>(
) -> (ClientHandler<C>, Proxy<C::ServerMessage, C::ClientMessage>) {
    let (tx, rx) = mpsc::channel();

    let handler = ClientHandler::<C> {
        messages: rx,
        in_flight: VecDeque::with_capacity(32),
    };

    // Sender is Send, but !Sync, so it's not safe to move between threads
    // without cloning it first.  Force a clone here, since we use Proxy in
    // native code and it's possible to move it between threads without Rust's
    // type system noticing.
    #[allow(clippy::redundant_clone)]
    let tx = tx.clone();
    let proxy = Proxy {
        handle: None,
        tx: ManuallyDrop::new(tx),
    };

    (handler, proxy)
}

pub(crate) fn make_callback_client<C: Client>() -> (
    ClientHandler<C>,
    DirectProxy<C::ServerMessage, C::ClientMessage>,
) {
    let (tx, rx) = mpsc::channel();

    let handler = ClientHandler::<C> {
        messages: rx,
        in_flight: VecDeque::with_capacity(1),
    };

    let (chan_tx, chan_rx) = mpsc::channel();

    // Sender is Send, but !Sync, so it's not safe to move between threads
    // without cloning it first.  Force a clone here, since we use Proxy in
    // native code and it's possible to move it between threads without Rust's
    // type system noticing.
    #[allow(clippy::redundant_clone)]
    let tx = tx.clone();
    let proxy = DirectProxy {
        handle: None,
        tx: ManuallyDrop::new(tx),
        chan_tx: Arc::new(Mutex::new(Some(chan_tx))),
        chan_rx: Arc::new(Mutex::new(Some(chan_rx))),
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
