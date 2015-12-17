extern crate libc;
extern crate hyper;
extern crate rustc_serialize;

use std::io;
use std::io::Read;
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time;
use std::process::Command;
use std::sync::Mutex;
use std::net::SocketAddr;
use std::net::TcpListener;
// use std::cell::RefCell;

use hyper::{Client, Server};
use hyper::header::Connection;
use hyper::server::Request;
use hyper::server::Response;
use hyper::uri::RequestUri;
use hyper::header;

use rustc_serialize::json;

#[derive(Debug,Clone,RustcDecodable,RustcEncodable,PartialEq)]
pub enum PWorkerState {
    JustLaunched,
    Connected(i32),
    Disconnected(u32), // time remaining in seconds before exit
}

#[derive(Debug,Clone,RustcDecodable,RustcEncodable,PartialEq)]
pub enum FollowerState {
    NotSpawned,
    Spawned(u32), // pid of spawned process
}

#[derive(Debug,Clone,RustcDecodable,RustcEncodable,PartialEq)]
pub enum PWorkerResponse {
    NoResponseYet,
    WorkerAlive(PWorkerState, FollowerState),
    AttachedToExistingWorker,
    GenericError(String),
}

pub fn spawn_follower(command: &mut Command) -> io::Result<u32> {
    println!("spawning follower");
    let child = command.spawn();
    let child = try!(child);
    Ok(child.id())
}

pub fn start_or_attach(command: &mut Command, port: u16, resp_tx: Option<Sender<PWorkerResponse>>) {
    let resp_tx = match resp_tx {
        None => {
            let (tx,_) = channel();
            tx
        },
        Some(tx) => tx
    };
    resp_tx.clone();

    // connect to existing pworker, pass my pid
    let mut ppid = unsafe { libc::getpid() };

    // do a two-part check for existing worker: first check if http port is bound
    // by trying to bind it here, and second try to set a new parent.
    // if the port is not bound, there is no worker, so start a new one.
    // if it is bound, but setparent fails, its an error.
    // if it is bound and setparent succeeds then existing worker is fine.

    let port_bound = {
        println!("checking port");
        let s_str = format!("127.0.0.1:{}", port);
        let addr: SocketAddr = s_str.parse().unwrap();
        match TcpListener::bind(addr) {
            Err(_) => true,
            Ok(l) => {
                drop(l);
                false
            }
        }
    };
    println!("port bound: {}", port_bound);

    let mut client = Client::new();
    client.set_write_timeout(Some(time::Duration::from_millis(250)));

    let set_new_parent =
        // only do this if we know port is bound, to avoid possible connection hang
        if port_bound {
        let pweb = format!("http://127.0.0.1:{}/?parent_pid={}", port, ppid);
        println!("pworker url: {}", pweb);

        match client.get(&pweb)
                    .header(Connection::close())
                    .send() {
            Err(e) => {
                // the port is bound but we couldn't set a new parent. error
                let msg = format!("Port bound but unable to connect to existing pworker: {}",
                                  e);
                let _ = resp_tx.send(PWorkerResponse::GenericError(msg));
                return;
            }
            Ok(_) => true,
        }
    } else {
        false
    };
    println!("set parent: {}", set_new_parent);

    // spawn thread to monitor pworker http server and replicate messages on
    // channel.  do this regardless of whether we are spawning a new pworker, since
    // we need the monitor thread in both cases.
    if set_new_parent || !port_bound {
        let resp_tx = resp_tx.clone();

        thread::spawn(move || {
            let max_fail = 5;
            let mut fail_count = 0;
            let mut last_resp = PWorkerResponse::NoResponseYet;

            loop {
                let pweb = format!("http://127.0.0.1:{}/?sitrep", port);
                let res = client.get(&pweb)
                                .header(Connection::close())
                                .send();
                match res {
                    Err(e) => {
                        fail_count += 1;
                        println!("fail: new count: {}; {}", fail_count, e);
                        if fail_count > max_fail {
                            let msg = format!("Unable to connect to pworker after {} attempts: {}",
                                              fail_count,
                                              e);
                            let _ = resp_tx.send(PWorkerResponse::GenericError(msg));
                            break;
                        }
                    }
                    Ok(res) => {
                        fail_count = 0;
                        let mut body = String::new();
                        let mut res = res;
                        res.read_to_string(&mut body).unwrap();
                        let new_state: PWorkerResponse = json::decode(&body).unwrap();

                        if new_state != last_resp {
                            // println!("hresp, newstate: {:?}", new_state);
                            last_resp = new_state;
                            let _ = resp_tx.send(last_resp.clone());
                        }
                    }

                }

                thread::sleep(time::Duration::from_millis(1000));
            }
        });
    }

    if port_bound {
        if set_new_parent {
            let _ = resp_tx.send(PWorkerResponse::AttachedToExistingWorker);
        }
        // don't need new worker
        println!("skipping worker start, port bound");
        return;
    }

    // start the pworker.
    // note: won't be able to use resp_tx after this due to the fork.
    // the http thread will need to poll for updates and replicate them on its resp_tx.

    let pid = unsafe { libc::fork() };
    match pid {
        -1 => panic!("fork error"),
        0 => {
            // fork again for poor-man's daemon
            let pid = unsafe {
                libc::setsid();
                libc::fork()
            };
            if pid == 0 {
                println!("pworker child spawned; parent pid: {}", ppid);

                // setup initial pworker state
                let state = Arc::new(Mutex::new(Box::new(PWorkerState::JustLaunched)));
                let fstate = Arc::new(Mutex::new(Box::new(FollowerState::NotSpawned)));

                // add channel for http handler to communicate back to this thread
                let (tx, rx) = channel();
                let h_tx = Mutex::new(tx.clone());
                let h_state = state.clone();
                let h_fstate = fstate.clone();
                let hb_handler = move |req: Request, mut res: Response| {
                    match req.uri {
                        RequestUri::AbsolutePath(p) => {
                            if p.contains("parent_pid=") {
                                let res = p.split("parent_pid=");
                                let vec: Vec<&str> = res.collect();
                                if vec.len() == 2 {
                                    let new_pid = vec[1].parse::<i32>().unwrap();
                                    let l_tx = h_tx.lock().unwrap();
                                    l_tx.send(new_pid).unwrap();
                                }
                            }
                        }
                        x => panic!("bizzaro request: {}", x),
                    }

                    {
                        // close conn to avoid handler overload
                        let headers = res.headers_mut();
                        headers.set(header::Connection(vec![header::ConnectionOption::Close]));
                    }

                    // send the worker state as the reploy for each request
                    // note the parent pid, if changed, won't reflect the new value until
                    // the pworker thread updates.
                    let state = h_state.lock().ok().expect("failed to lock state in handler");
                    let fstate = h_fstate.lock().ok().expect("failed to lock fstate in handler");

                    let ws = PWorkerResponse::WorkerAlive((*(*state)).clone(),
                                                          (*(*fstate)).clone());

                    let encoded = json::encode(&ws).unwrap();

                    // println!("handler ws: {:?}", ws);

                    let _ = res.send(encoded.as_bytes());
                };

                // make http server
                let mut server = {
                    let s_str = format!("127.0.0.1:{}", port);
                    let addr: SocketAddr = s_str.parse().unwrap();
                    let server = Server::http(addr).unwrap();
                    println!("starting pworker server");
                    match server.handle_threads(hb_handler, 2) {
                        Err(e) => panic!("failed to start http server: {}", e),
                        Ok(l) => l,
                    }
                };

                // setup termination handler
                let mut terminate = |fstate: &FollowerState| {
                    unsafe {
                        if let FollowerState::Spawned(pid) = *fstate {
                            println!("killing follower");
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                        println!("pworker complete");
                        // its important to close the port on the http server, otherwise
                        // we may not be able to rebind successfully if we restart (osx)
                        let _ = server.close();
                        thread::sleep(time::Duration::from_millis(1000));

                        libc::exit(0);
                    }
                };

                loop {
                    while let Ok(new_pid) = rx.try_recv() {
                        println!("got new pid: {}", new_pid);
                        ppid = new_pid;
                    }

                    // kill with signal 0 to check to see if ppid is alive
                    let par_alive = unsafe { libc::kill(ppid, 0) } == 0;
                    // println!("pworker state: {:?}; par alive: {}", state, par_alive);

                    // clone the states to make the check logic easier to deal with
                    let old_fstate = *(fstate.lock()
                                             .ok()
                                             .expect("Failed to lock fstate prior to state \
                                                      update"))
                                          .clone();
                    let old_state = *(state.lock()
                                           .ok()
                                           .expect("Failed to lock state prior to state update"))
                                         .clone();

                    let new_state = {
                        if par_alive {
                            match old_state {
                                // always reset to connected if parent alive
                                PWorkerState::Connected(_) |
                                PWorkerState::Disconnected(_) |
                                PWorkerState::JustLaunched => PWorkerState::Connected(ppid),
                            }
                        } else {
                            // parent disconnected
                            match old_state {
                                PWorkerState::JustLaunched |
                                PWorkerState::Connected(_) => PWorkerState::Disconnected(5),
                                PWorkerState::Disconnected(n) if n > 0 => {
                                    PWorkerState::Disconnected(n - 1)
                                }
                                PWorkerState::Disconnected(0) => terminate(&old_fstate),
                                // this is an illegal state, reset to something legal
                                PWorkerState::Disconnected(_) => PWorkerState::Disconnected(0),
                            }
                        }
                    };

                    // spawn the follower if its ok to do so
                    if let PWorkerState::Connected(_) = new_state {
                        if let FollowerState::NotSpawned = old_fstate {
                            match spawn_follower(command) {
                                Err(e) => println!("Unable to spawn follower: {}", e),
                                Ok(fpid) => {
                                    *(*(fstate.lock()
                                              .ok()
                                              .expect("failed to lock fstate for update"))) =
                                        FollowerState::Spawned(fpid)
                                }
                            }
                        }
                    }

                    *(*(state.lock().ok().expect("failed to lock state for update"))) = new_state;

                    thread::sleep(time::Duration::from_millis(1000));
                }
            } else {
                // exit on error, or remnant from double-fork
                unsafe { libc::exit(0) };
            }
        }
        n => {
            println!("pworker spawned: {}", n);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::path::PathBuf;
    use std::env;
    use std::time;
    use std::thread;
    use std::sync::mpsc::channel;

    extern crate libc;

    use super::PWorkerResponse;
    use super::PWorkerState;
    use super::FollowerState;

    fn get_testproc_path() -> PathBuf {
        let mut wd = env::current_dir().unwrap();
        wd.push("testproc");
        wd.push("target");
        wd.push("debug");
        wd.push("testproc");

        if !wd.is_file() {
            panic!("testproc not found, please build it: {:?}", wd);
        }
        wd
    }

    #[test]
    fn test_basic() {
        // make sure that we can spawn a pworker, it can spawn a follower, and we can
        // get messages from it.  this does NOT check that the pworker kills the follower
        // after we exit.  need an another executable that can exit to check that.
        let tp = get_testproc_path();

        let my_pid = unsafe { libc::getpid() };

        let mut tp_command = Command::new(&tp);
        let (tx, rx) = channel();
        super::start_or_attach(&mut tp_command, 16550, Some(tx));
        let scount = 10;
        let mut got_alive = false;
        for _ in 1..scount {
            while let Ok(m) = rx.try_recv() {
                if let PWorkerResponse::WorkerAlive(PWorkerState::Connected(conpid),
                                                    FollowerState::Spawned(fpid)) = m {
                    assert!(conpid == my_pid, "expected my pid but got a different one");

                    // kill the foller with sig 0 to make sure it is really alive
                    let f_alive = unsafe { libc::kill(fpid as i32, 0) } == 0;
                    assert!(f_alive,
                            format!("expected follower pid {} to be alive, but it isn't", fpid));

                    got_alive = true;
                }
                println!("pworker message: {:?}", m)
            }

            if got_alive {
                break;
            }
            thread::sleep(time::Duration::from_millis(1000));
        }

        assert!(got_alive, "should have received alive message");
    }
}
