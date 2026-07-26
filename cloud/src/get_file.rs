use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{eprintln, println, thread};

use blake3::{Hash, Hasher};
use uuid::Uuid;

use crate::errors::is_connection_broken;
use crate::file_transfer::CHUNK_SIZE;
use crate::mapper::{MapStore, with_file_mut};
use crate::response::{Code, ErrorTransfer};
const OVERHEAD: usize = 11;

struct Query {
    file_uuid: Uuid,
}

impl Query {
    fn from_bytes(bytes: &[u8], buf_len: usize) -> Option<Self> {
        debug_assert!(buf_len >= 1 && buf_len <= CHUNK_SIZE);
        let file_uuid = Uuid::from_bytes(bytes[0..16].try_into().unwrap());
        Some(Query { file_uuid })
    }
    fn get_path(&self, map: &MapStore, client_uuid: &Uuid) -> Result<PathBuf, ErrorTransfer> {
        match with_file_mut(&self.file_uuid, map, client_uuid, |fil| {
            if fil.lock() {
                Ok(fil.path.clone())
            } else {
                Err(ErrorTransfer::Locked)
            }
        }) {
            Ok(fil) => fil,
            Err(e) => return Err(e),
        }
    }
}

pub fn send_file(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    buf_len: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
) {
    println!("send_file called");
    let query = match Query::from_bytes(&first_message[offset..], buf_len) {
        Some(q) => q,
        None => {
            let buf = [48u8; 1];
            let _ = stream.write_all(&buf);
            return;
        }
    };
    println!("62");

    let path = match query.get_path(&map_store, client_uuid) {
        Ok(p) => p,
        Err(e) => {
            println!("error: {:?}", e);
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };

    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("failed to open {path:?}: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return; // or continue / propagate, depending on caller context
        }
    };

    let file_size = match file.metadata() {
        Ok(meta) => meta.len(),
        Err(e) => {
            eprintln!("failed to get metadata for {path:?}: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    println!("91");

    let chunks_len = get_chunks_len(file_size);
    let fil = Arc::new(file);

    let mut buf = [0u8; 5];
    buf[0] = 20;
    buf[1..5].copy_from_slice(&chunks_len.to_be_bytes());

    match stream.write_all(&buf) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            println!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected write error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    }

    println!("113");

    let mut resp = [0u8; CHUNK_SIZE];

    match stream.read(&mut resp) {
        Ok(0) => {
            println!("connection broken");
            return;
        }
        Ok(_) => {
            if resp[0] != 20 {
                return;
            }
        }
        Err(e) if is_connection_broken(&e) => {
            println!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    }

    println!("138");

    let arc_stream = Arc::new(Mutex::new(stream));

    workers_send(
        max_workers.min(chunks_len as usize),
        chunks_len,
        arc_stream,
        fil,
        file_size,
        None,
    );

    println!("153");

    match with_file_mut(&query.file_uuid, &map_store, client_uuid, |fil| {
        fil.unlock()
    }) {
        Ok(_) => (),
        Err(e) => eprintln!(
            "failed to unlock file: {:?}. error: {e:?}",
            &query.file_uuid
        ),
    }
}

pub fn reinit_send_file(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    buf_len: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
) {
    let query = match Query::from_bytes(&first_message[offset..], buf_len) {
        Some(q) => q,
        None => {
            let buf = [48u8; 1];
            let _ = stream.write_all(&buf);
            return;
        }
    };

    let path = match query.get_path(&map_store, client_uuid) {
        Ok(p) => p,
        Err(e) => {
            println!("error: {:?}", e);
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("failed to open {path:?}: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return; // or continue / propagate, depending on caller context
        }
    };
    let file_size = match file.metadata() {
        Ok(meta) => meta.len(),
        Err(e) => {
            eprintln!("failed to get metadata for {path:?}: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let chunks_len = get_chunks_len(file_size);
    let fil = Arc::new(file);

    let mut buf = [0u8; 5];
    buf[0] = 20;
    buf[1..5].copy_from_slice(&chunks_len.to_be_bytes());

    match stream.write_all(&buf) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            println!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    }

    let mut resp = [0u8; CHUNK_SIZE];

    match stream.read(&mut resp) {
        Ok(0) => {
            println!("connection broken");
            return;
        }
        Ok(_) => {
            if resp[0] != 20 {
                return;
            }
        }
        Err(e) if is_connection_broken(&e) => {
            println!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    }

    let arc_stream = Arc::new(Mutex::new(stream));
    let chunks_to_send = Arc::new(Mutex::new(Vec::new()));

    confirm_completion(&arc_stream, &chunks_to_send);

    workers_send(
        max_workers.min(chunks_len as usize),
        chunks_len,
        arc_stream,
        fil,
        file_size,
        Some(chunks_to_send),
    );

    match with_file_mut(&query.file_uuid, &map_store, client_uuid, |fil| {
        fil.unlock()
    }) {
        Ok(_) => (),
        Err(e) => eprintln!(
            "failed to unlock file: {:?}. error: {e:?}",
            &query.file_uuid
        ),
    }
}

pub fn workers_send(
    workers: usize,
    chunks_len: u32,
    arc_stream: Arc<Mutex<TcpStream>>,
    fil: Arc<File>,
    file_size: u64,
    chunks_to_send: Option<Arc<Mutex<Vec<u64>>>>,
) {
    let in_flight = Arc::new(Mutex::new(0));
    let dead_threads = Arc::new(Mutex::new(0));

    //u64 is id
    let chunks_in_flight: Arc<Mutex<HashMap<u64, Duration>>> = Arc::new(Mutex::new(HashMap::new()));

    let chunks_to_send: Arc<Mutex<Vec<u64>>> = match chunks_to_send {
        Some(c) => c,
        None => {
            let c_to_send = Arc::new(Mutex::new(Vec::new()));
            {
                let mut lock = c_to_send.lock().unwrap_or_else(|lock| {
                    eprintln!("c_to_send was poisoned: {lock:?}");
                    lock.into_inner()
                });
                for i in 1..chunks_len {
                    lock.push(i as u64);
                }
            }
            c_to_send
        }
    };

    {
        let guard = arc_stream.lock().unwrap_or_else(|e| {
            eprintln!("arc_stream was poisoned: {e:?}");
            e.into_inner()
        });

        if let Err(e) = guard.set_nonblocking(true) {
            eprintln!("failed to set non-blocking mode: {e}");
            return;
        }
    }

    loop {
        let mut handles = Vec::new();

        for _ in 0..workers {
            let in_flight = in_flight.clone();
            let chunks_in_flight = chunks_in_flight.clone();
            let fil = fil.clone();
            let chunks = chunks_to_send.clone();
            let arc_stream = arc_stream.clone();
            let dead_threads = dead_threads.clone();
            handles.push(thread::spawn(move || {
                let mut counter = 0;
                loop {
                    counter = check_timeout_in_flight(&in_flight, &chunks_in_flight, counter);
                    match {
                        chunks
                            .lock()
                            .unwrap_or_else(|e| {
                                eprintln!("chunks was poisoned: {e:?}");
                                e.into_inner()
                            })
                            .pop()
                    } {
                        Some(c) => send_chunk(
                            &chunks_in_flight,
                            &arc_stream,
                            &in_flight,
                            &fil,
                            c,
                            file_size,
                        ),
                        None => {
                            *dead_threads.lock().unwrap_or_else(|e| {
                                eprintln!("dead_threads was poisoned: {e:?}");
                                e.into_inner()
                            }) += 1;
                            break;
                        }
                    };
                }
            }));
        }
        match reader(
            &arc_stream,
            &chunks_in_flight,
            &in_flight,
            &dead_threads,
            workers,
        ) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("reader failed: {e:?}");
                return;
            }
        };

        for handle in handles {
            match handle.join() {
                Ok(_) => (),
                Err(panic_payload) => {
                    let msg = panic_payload
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic payload".to_string());
                    eprintln!("a thread panicked: {msg}");
                }
            }
        }

        if confirm_completion(&arc_stream, &chunks_to_send) {
            break;
        };
    }

    let mut stream = arc_stream.lock().unwrap_or_else(|e| {
        eprintln!("arc_stream was poisoned: {e:?}");
        e.into_inner()
    });

    loop {
        let mut buf = [0u8; 1];
        match stream.read_exact(&mut buf) {
            Ok(_) => match buf[0] {
                21 => {
                    println!("client ready");
                    break;
                }
                val => {
                    println!("44 header: {} not found", val);
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                println!("44 header: {} not found", buf[0]);
                let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                return;
            }
        };
    }

    let hash = match hash_file(fil) {
        Ok(hash) => hash,
        Err(e) => {
            eprintln!("hashing failed: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let mut file_hash_buf: [u8; 32] = match hash.try_into() {
        Ok(h) => h,
        Err(e) => {
            eprintln!("invalid hash: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let mut buf = vec![0u8; 33];
    buf[1..].copy_from_slice(&mut file_hash_buf);
    buf[0] = 4;

    let _ = stream.write_all(&buf);

    println!("sent {:?}", buf);

    let mut attempts = 1;
    loop {
        let mut buf = [0u8; 1];
        match stream.read_exact(&mut buf) {
            Ok(_) => match buf[0] {
                24 => {
                    println!("success");
                    break;
                }
                44 => {
                    println!("44 header: {} not found", 44);
                    if attempts < 5 {
                        println!("trying to send a completion check again, attempt: {attempts}");
                        match stream.write_all(&file_hash_buf) {
                            Ok(_) => (),
                            Err(e) if is_connection_broken(&e) => {
                                println!("connection broken");
                                return;
                            }
                            Err(e) => {
                                eprintln!("unexpected read error waiting for ready signal: {e}");
                            }
                        }
                        println!("sent: {:?}", &file_hash_buf);
                        attempts += 1;
                    } else {
                        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                        return;
                    }
                }
                val => {
                    println!("44 header: {} not found", val);
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                println!("44 header: {} not found. Leaving", buf[0]);
                return;
            }
        };
    }
}

/// in other words: send 3
fn confirm_completion(
    arc_stream: &Arc<Mutex<TcpStream>>,
    chunks_to_send: &Arc<Mutex<Vec<u64>>>,
) -> bool {
    let mut buf = vec![0u8; 1];
    buf[0] = 3;
    let mut stream = arc_stream.lock().unwrap_or_else(|e| {
        eprintln!("arc_stream was poisoned: {e:?}");
        e.into_inner()
    });

    match stream.write_all(&buf) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            println!("connection broken");
            return false;
        }
        Err(e) => {
            eprintln!("unexpected write error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return false;
        }
    }

    let mut response_code = [0u8; 1];
    loop {
        match stream.read_exact(&mut response_code) {
            Ok(_) => {
                if response_code[0] == 23 {
                    println!("response for 3 is {:?}", response_code[0]);
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
            Err(e) => {
                eprintln!("{}", e);
                let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                return false;
            }
        };
    }

    let mut count_buf = vec![0u8; 8];

    match stream.read_exact(&mut count_buf).map_err(|e| return e) {
        Ok(_) => (),
        Err(e) => {
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            println!("unexpected fail to read: {e:?}");
            return false;
        }
    };

    let count = u64::from_be_bytes(count_buf.try_into().unwrap());

    if count == 0 {
        return true;
    }

    let mut missing_buf = vec![0u8; count as usize * 8];

    match stream.read_exact(&mut missing_buf).map_err(|e| return e) {
        Ok(_) => (),
        Err(e) => {
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            println!("unexpected fail to read: {e:?}");
            return false;
        }
    };

    let mut missing = Vec::new();
    for chunk in missing_buf.chunks_exact(8) {
        missing.push(u64::from_be_bytes(chunk.try_into().unwrap()));
    }
    chunks_to_send
        .lock()
        .unwrap_or_else(|e| {
            eprintln!("chunks_to_send was poisoned: {e:?}");
            e.into_inner()
        })
        .append(&mut missing);

    false
}

fn reader(
    arc_stream: &Arc<Mutex<TcpStream>>,
    chunks_in_flight: &Arc<Mutex<HashMap<u64, Duration>>>,
    in_flight: &Arc<Mutex<isize>>,
    dead_threads: &Arc<Mutex<usize>>,
    workers: usize,
) -> Result<(), ErrorTransfer> {
    let mut in_f: isize = {
        in_flight
            .lock()
            .unwrap_or_else(|e| {
                eprintln!("arc_stream was poisoned: {e:?}");
                e.into_inner()
            })
            .clone()
    };
    while workers
        > *dead_threads.lock().unwrap_or_else(|e| {
            eprintln!("arc_stream was poisoned: {e:?}");
            e.into_inner()
        })
        || in_f > 0
    {
        let mut resp = [0u8; 16];

        let n = {
            let mut stream = arc_stream.lock().unwrap_or_else(|e| {
                eprintln!("arc_stream was poisoned: {e:?}");
                e.into_inner()
            });
            stream.read(&mut resp)
        };

        match n {
            Ok(0) => {
                println!("closed");
                return Err(ErrorTransfer::Closed);
            } // connection closed
            Ok(_) => {
                if resp[0] != 0 {
                    println!("{:?}", resp[0]);
                    if resp[0] == 20 {
                        let id = u64::from_be_bytes(resp[8..].try_into().unwrap());
                        chunks_in_flight
                            .lock()
                            .unwrap_or_else(|e| {
                                eprintln!("chunks_in_flight was poisoned: {e:?}");
                                e.into_inner()
                            })
                            .remove(&id);
                    }

                    *in_flight.lock().unwrap_or_else(|e| {
                        eprintln!("in_flight was poisoned: {e:?}");
                        e.into_inner()
                    }) -= 1;
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
            Err(e) => {
                eprintln!("{}", e);
                return Err(ErrorTransfer::Closed);
            }
        }

        in_f = in_flight
            .lock()
            .unwrap_or_else(|e| {
                eprintln!("in_flight was poisoned: {e:?}");
                e.into_inner()
            })
            .clone();
    }
    Ok(())
}

fn check_timeout_in_flight(
    in_flight: &Arc<Mutex<isize>>,
    chunks_in_flight: &Arc<Mutex<HashMap<u64, Duration>>>,
    mut counter: usize,
) -> usize {
    if in_flight
        .lock()
        .unwrap_or_else(|e| {
            eprintln!("in_flight was poisoned: {e:?}");
            e.into_inner()
        })
        .clone()
        > 5
    {
        counter += 1;
        thread::sleep(Duration::from_millis(50));
        if counter >= 10 {
            let mut now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let removed: Vec<(u64, Duration)> = chunks_in_flight
                .lock()
                .unwrap_or_else(|e| {
                    eprintln!("chunks_in_flight was poisoned: {e:?}");
                    e.into_inner()
                })
                .extract_if(|_k, value| value < &mut now)
                .collect();
            counter = 0;
            let mut in_f = in_flight.lock().unwrap_or_else(|e| {
                eprintln!("in_flight was poisoned: {e:?}");
                e.into_inner()
            });
            *in_f -= removed.len() as isize;
        }
    }
    counter
}

fn send_chunk(
    chunks_in_flight: &Arc<Mutex<HashMap<u64, Duration>>>,
    stream: &Arc<Mutex<TcpStream>>,
    in_flight: &Arc<Mutex<isize>>,
    fil: &Arc<File>,
    id: u64,
    file_size: u64,
) {
    let remaining = file_size - (CHUNK_SIZE - OVERHEAD) as u64 * id;
    let chunk_size = remaining.min((CHUNK_SIZE - OVERHEAD) as u64) as usize;

    let mut buf = vec![0u8; chunk_size];
    match fil.read_at(&mut buf, (CHUNK_SIZE - OVERHEAD) as u64 * id) {
        Ok(_) => (),
        Err(e) => {
            eprintln!(
                "failed to read chunk {id} at offset {}: {e}",
                (CHUNK_SIZE - OVERHEAD) as u64 * id
            );
            return;
        }
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .saturating_add(Duration::from_secs(10));

    let transfer_id: u64 = id;

    let chunk_size: u16 = buf.len() as u16;

    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    buffer.extend_from_slice(&[2]);
    buffer.extend_from_slice(&transfer_id.to_be_bytes());
    buffer.extend_from_slice(&chunk_size.to_be_bytes());
    buffer.extend_from_slice(&buf);

    {
        let mut lock = stream.lock().unwrap_or_else(|e| {
            eprintln!("stream was poisoned: {e:?}");
            e.into_inner()
        });
        match lock.write_all(&buffer) {
            Ok(_) => (),
            Err(e) if is_connection_broken(&e) => {
                println!("connection broken");
                return;
            }
            Err(e) => {
                eprintln!("unexpected write error: {e}");
                let _ = lock.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                return;
            }
        };
    }

    let _ = chunks_in_flight
        .lock()
        .unwrap_or_else(|lock| {
            eprintln!("chunks_in_flight was poisoned: {lock:?}");
            lock.into_inner()
        })
        .insert(id, timestamp);
    *in_flight.lock().unwrap_or_else(|lock| {
        eprintln!("in_flight was poisoned: {lock:?}");
        lock.into_inner()
    }) += 1;
}

pub fn get_file_size(path: &Path) -> Result<u64, ErrorTransfer> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(_) => return Err(ErrorTransfer::NotFound),
    };

    let size = match file.metadata() {
        Ok(md) => md.len(),
        Err(_) => return Err(ErrorTransfer::NotFound),
    };
    Ok(size)
}

pub fn get_chunks_len(file_size: u64) -> u32 {
    let payload = (CHUNK_SIZE - OVERHEAD) as u64;
    file_size.div_ceil(payload) as u32
}

fn hash_file(file: Arc<File>) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 65536];
    let mut file = match Arc::try_unwrap(file) {
        Ok(a) => a,
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "arc unwrap failed".to_string(),
            ));
        }
    };

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}
