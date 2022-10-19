// Copyright © 2017 Mozilla Foundation
//
// This program is made available under an ISC-style license.  See the
// accompanying file LICENSE for details
#![warn(unused_extern_crates)]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

use audio_thread_priority::promote_current_thread_to_real_time;
use audioipc::ipccore;
use audioipc::sys;
use audioipc::PlatformHandleType;
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;
use std::sync::Mutex;
use std::thread;

mod server;

struct CubebContextParams {
    context_name: CString,
    backend_name: Option<CString>,
}

static G_CUBEB_CONTEXT_PARAMS: Lazy<Mutex<CubebContextParams>> = Lazy::new(|| {
    Mutex::new(CubebContextParams {
        context_name: CString::new("AudioIPC Server").unwrap(),
        backend_name: None,
    })
});

#[allow(deprecated)]
pub mod errors {
    #![allow(clippy::upper_case_acronyms)]
    error_chain! {
        links {
            AudioIPC(::audioipc::errors::Error, ::audioipc::errors::ErrorKind);
        }
        foreign_links {
            Cubeb(cubeb_core::Error);
            Io(::std::io::Error);
        }
    }
}

use crate::errors::*;

struct ServerWrapper {
    rpc_thread: ipccore::EventLoopThread,
    callback_thread: ipccore::EventLoopThread,
    device_collection_thread: ipccore::EventLoopThread,
}

fn register_thread(callback: Option<extern "C" fn(*const ::std::os::raw::c_char)>) {
    if let Some(func) = callback {
        let name = CString::new(thread::current().name().unwrap()).unwrap();
        func(name.as_ptr());
    }
}

fn unregister_thread(callback: Option<extern "C" fn()>) {
    if let Some(func) = callback {
        func();
    }
}

enum ThreadPriority {
    Normal,
    RealTime,
}

fn on_thread_state_change(
    priority: ThreadPriority,
    state: ipccore::EventLoopState,
    thread_create_callback: Option<extern "C" fn(*const ::std::os::raw::c_char)>,
    thread_destroy_callback: Option<extern "C" fn()>,
) {
    match state {
        ipccore::EventLoopState::Start => register_thread(thread_create_callback),
        ipccore::EventLoopState::Stop => unregister_thread(thread_destroy_callback),
        _ => (),
    }
    if matches!(priority, ThreadPriority::RealTime) {
        thread_local! {
            static HANDLE: RefCell<Option<audio_thread_priority::RtPriorityHandle>>  = RefCell::new(None);
        }
        match state {
            ipccore::EventLoopState::Active => {
                HANDLE.with(|h| {
                    let r = promote_current_thread_to_real_time(256, 48000);
                    //eprintln!("promote_current_thread_to_real_time={:?}", r);
                    *h.borrow_mut() = r.ok();
                });
            }
            ipccore::EventLoopState::Idle => {
                HANDLE.with(|h| {
                    if let Some(h) = h.borrow_mut().take() {
                        let r = audio_thread_priority::demote_current_thread_from_real_time(h);
                        eprintln!("demote_current_thread_from_real_time={:?}", r);
                    }
                });
            }
            _ => (),
        }
    };
}

fn init_threads(
    thread_create_callback: Option<extern "C" fn(*const ::std::os::raw::c_char)>,
    thread_destroy_callback: Option<extern "C" fn()>,
) -> Result<ServerWrapper> {
    let rpc_thread =
        ipccore::EventLoopThread::new("AudioIPC Server RPC".to_string(), None, move |state| {
            on_thread_state_change(
                ThreadPriority::Normal,
                state,
                thread_create_callback,
                thread_destroy_callback,
            )
        })
        .map_err(|e| {
            debug!("Failed to start Server RPC thread: {:?}", e);
            e
        })?;

    let callback_thread =
        ipccore::EventLoopThread::new("AudioIPC Server Callback".to_string(), None, move |state| {
            on_thread_state_change(
                ThreadPriority::RealTime,
                state,
                thread_create_callback,
                thread_destroy_callback,
            )
        })
        .map_err(|e| {
            debug!("Failed to start Server Callback thread: {:?}", e);
            e
        })?;

    let device_collection_thread = ipccore::EventLoopThread::new(
        "AudioIPC DeviceCollection RPC".to_string(),
        None,
        move |state| {
            on_thread_state_change(
                ThreadPriority::Normal,
                state,
                thread_create_callback,
                thread_destroy_callback,
            )
        },
    )
    .map_err(|e| {
        debug!("Failed to start DeviceCollection RPC thread: {:?}", e);
        e
    })?;

    Ok(ServerWrapper {
        rpc_thread,
        callback_thread,
        device_collection_thread,
    })
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AudioIpcServerInitParams {
    // Fields only need to be public for ipctest.
    pub thread_create_callback: Option<extern "C" fn(*const ::std::os::raw::c_char)>,
    pub thread_destroy_callback: Option<extern "C" fn()>,
}

#[allow(clippy::missing_safety_doc)]
#[no_mangle]
pub unsafe extern "C" fn audioipc2_server_start(
    context_name: *const std::os::raw::c_char,
    backend_name: *const std::os::raw::c_char,
    init_params: *const AudioIpcServerInitParams,
) -> *mut c_void {
    assert!(!init_params.is_null());
    let mut params = G_CUBEB_CONTEXT_PARAMS.lock().unwrap();
    if !context_name.is_null() {
        params.context_name = CStr::from_ptr(context_name).to_owned();
    }
    if !backend_name.is_null() {
        let backend_string = CStr::from_ptr(backend_name).to_owned();
        params.backend_name = Some(backend_string);
    }
    match init_threads(
        (*init_params).thread_create_callback,
        (*init_params).thread_destroy_callback,
    ) {
        Ok(server) => Box::into_raw(Box::new(server)) as *mut _,
        Err(_) => ptr::null_mut() as *mut _,
    }
}

// A `shm_area_size` of 0 allows the server to calculate an appropriate shm size for each stream.
// A non-zero `shm_area_size` forces all allocations to the specified size.
#[no_mangle]
pub extern "C" fn audioipc2_server_new_client(
    p: *mut c_void,
    shm_area_size: usize,
) -> PlatformHandleType {
    let wrapper: &ServerWrapper = unsafe { &*(p as *mut _) };

    // We create a connected pair of anonymous IPC endpoints. One side
    // is registered with the event loop core, the other side is returned
    // to the caller to be remoted to the client to complete setup.
    let (server_pipe, client_pipe) = match sys::make_pipe_pair() {
        Ok((server_pipe, client_pipe)) => (server_pipe, client_pipe),
        Err(e) => {
            error!(
                "audioipc_server_new_client - make_pipe_pair failed: {:?}",
                e
            );
            return audioipc::INVALID_HANDLE_VALUE;
        }
    };

    let rpc_thread = wrapper.rpc_thread.handle();
    let callback_thread = wrapper.callback_thread.handle();
    let device_collection_thread = wrapper.device_collection_thread.handle();

    let server = server::CubebServer::new(
        callback_thread.clone(),
        device_collection_thread.clone(),
        shm_area_size,
    );
    if let Err(e) = rpc_thread.bind_server(server, server_pipe) {
        error!("audioipc_server_new_client - bind_server failed: {:?}", e);
        return audioipc::INVALID_HANDLE_VALUE;
    }

    unsafe { client_pipe.into_raw() }
}

#[no_mangle]
pub extern "C" fn audioipc2_server_stop(p: *mut c_void) {
    let wrapper = unsafe { Box::<ServerWrapper>::from_raw(p as *mut _) };
    drop(wrapper);
}
