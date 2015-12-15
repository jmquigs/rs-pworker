extern crate libc;
extern crate hyper;
extern crate civet;
extern crate conduit;

use std::collections::HashMap;
use std::io;
use std::sync::mpsc::channel;
use std::thread;
use std::time;
use std::process::Command;
use std::sync::Mutex;

use civet::{Config, response, Server};
use conduit::{Request, Response};

use self::hyper::Client;
use self::hyper::header::Connection;

#[derive(Debug)]
enum PWorkerState {
    JustLaunched,
    Connected,
    Disconnected (u32) // time remaining in seconds before exit
}

#[derive(Debug)]
enum FollowerState {
    NotSpawned,
    Spawned(u32) // pid of spawned process
}

pub fn spawn_follower(command:&mut Command) -> io::Result<u32> {
    println!("spawning follower");
    let child = command.spawn();
    let child = try!(child);
    Ok(child.id())
}

pub fn start_or_attach(command:&mut Command, port:u16) {
    // connect to existing pworker, pass my pid
    let mut ppid = unsafe { libc::getpid() };

    let pweb = format!("http://localhost:{}/?parent_pid={}", port, ppid);
    println!("pworker url: {}", pweb);
    let client = Client::new();

    let res = client.get(&pweb)
        .header(Connection::close())
        .send();

    match res {
        Ok(_) => return, // pworker alive, we're done here
        _ => ()
    }

    // start the pworker

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

                let (tx, rx) = channel();

                let h_tx = Mutex::new(tx.clone());

                let hb_handler = move |req: &mut Request| -> io::Result<Response> {
                    match req.query_string() {
                        None => (),
                        Some(qs) => {
                            if qs.contains("parent_pid=") {
                                let res = qs.split("parent_pid=");
                                let vec:Vec<&str> = res.collect();
                                if vec.len() == 2 {
                                    let new_pid = vec[1].parse::<i32>().unwrap();
                                    let l_tx = h_tx.lock().unwrap();
                                    l_tx.send(new_pid).unwrap();
                                }
                            }
                        }
                    }

                    let mut headers:HashMap<String,Vec<String>> = HashMap::new();
                    // close conn since we don't have many handlers
                    headers.insert("Connection".to_owned(), vec!["Close".to_owned()]);

                    Ok(response(200, headers, "PWorker running".as_bytes()))
                };

                let mut cfg = Config::new();
                cfg.port(port).threads(1);
                let _server = Server::start(cfg, hb_handler);

                let mut state = PWorkerState::JustLaunched;
                let mut fstate = FollowerState::NotSpawned;

                loop {
                    while let Ok(new_pid) = rx.try_recv() {
                        println!("got new pid: {}", new_pid);
                        ppid = new_pid;
                    };

                    let par_alive = unsafe { libc::kill(ppid, 0) } == 0;
                    println!("pworker state: {:?}; par alive: {}", state, par_alive);

                    match state {
                        PWorkerState::JustLaunched if par_alive => state = PWorkerState::Connected,
                        PWorkerState::Disconnected(_) if par_alive => state = PWorkerState::Connected,
                        PWorkerState::Disconnected(n) if n > 0 => state = PWorkerState::Disconnected(n-1),
                        PWorkerState::Disconnected(0) => {
                            unsafe {
                                if let FollowerState::Spawned(pid) = fstate {
                                    println!("killing follower");
                                    libc::kill(pid as i32, libc::SIGTERM);
                                }
                                println!("pworker complete");
                                libc::exit(0);
                            }
                        },
                        _ => if !par_alive { state = PWorkerState::Disconnected(10) },
                    };

                    if let PWorkerState::Connected = state {
                        if let FollowerState::NotSpawned = fstate {
                            match spawn_follower(command) {
                                Err(e) => println!("Unable to spawn follower: {}", e),
                                Ok(fpid) => fstate = FollowerState::Spawned(fpid)
                            }
                        }
                    }

                    thread::sleep(time::Duration::from_millis(1000));
                }
            }
            else {
                // exit remnant from double-fork
                unsafe {libc::exit(0)};
            }
        },
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
        let tp = get_testproc_path();

        let mut tp_command = Command::new(&tp);
        super::start_or_attach(&mut tp_command, 16550);

        // TODO: actually assert something once the backchannel is going

        thread::sleep(time::Duration::from_millis(5000));
    }
}
