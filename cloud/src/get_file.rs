use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::iter::Map;
use std::net::TcpStream;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use blake3::{Hash, Hasher};
use uuid::Uuid;

use crate::auth::login_api;
use crate::file_transfer::CHUNK_SIZE;
use crate::mapper::{Fil, MapStore};
use crate::response::ErrorTransfer;
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
        match Fil::find_mut(&self.file_uuid, map, client_uuid) {
            Ok(mut fil) => {
                if fil.lock() {
                    Ok(fil.path)
                } else {
                    Err(ErrorTransfer::Locked)
                }
            }
            Err(e) => Err(e),
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
    let query = match Query::from_bytes(&first_message[offset..], buf_len) {
        Some(q) => q,
        None => {
            let buf = [48u8; 1];
            stream.write_all(&buf);
            return;
        }
    };
    let path = match query.get_path(&map_store, client_uuid) {
        Ok(p) => p,
        Err(e) => {
            println!("error: {:?}", e);
            let buf = [48u8; 1];
            stream.write_all(&buf);
            return;
        }
    };
    println!("path: {:?}", path);
    let file_size = get_file_size(&path).unwrap();
    let chunks_len = get_chunks_len(file_size);
    let fil = Arc::new(File::open(&path).unwrap());

    println!("sending {:?}", chunks_len);
    let mut buf = [0u8; 5];
    buf[0] = 20;
    buf[1..5].copy_from_slice(&chunks_len.to_be_bytes());
    stream.write_all(&buf).unwrap();

    let mut resp = [0u8; CHUNK_SIZE];

    stream.read(&mut resp);

    if resp[0] != 20 {
        return;
    }

    let arc_stream = Arc::new(Mutex::new(stream));

    workers_send(
        max_workers.min(chunks_len as usize),
        chunks_len,
        arc_stream,
        fil,
        file_size,
        None,
    );

    Fil::find_mut(&query.file_uuid, &map_store, client_uuid)
        .unwrap()
        .unlock();
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
            stream.write_all(&buf);
            return;
        }
    };
    let path = match query.get_path(&map_store, client_uuid) {
        Ok(p) => p,
        Err(e) => {
            println!("error: {:?}", e);
            let buf = [48u8; 1];
            stream.write_all(&buf);
            return;
        }
    };
    println!("path: {:?}", path);
    let file_size = get_file_size(&path).unwrap();
    let chunks_len = get_chunks_len(file_size);
    let fil = Arc::new(File::open(&path).unwrap());

    println!("sending {:?}", chunks_len);
    let mut buf = [0u8; 5];
    buf[0] = 20;
    buf[1..5].copy_from_slice(&chunks_len.to_be_bytes());
    stream.write_all(&buf).unwrap();

    let mut resp = [0u8; CHUNK_SIZE];

    stream.read(&mut resp);

    if resp[0] != 20 {
        return;
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

    Fil::find_mut(&query.file_uuid, &map_store, client_uuid)
        .unwrap()
        .unlock();
}

fn workers_send(
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
                let mut lock = c_to_send.lock().unwrap();
                for i in 1..chunks_len {
                    lock.push(i as u64);
                }
            }
            c_to_send
        }
    };

    {
        arc_stream.lock().unwrap().set_nonblocking(true).unwrap();
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
                    match { chunks.lock().unwrap().pop() } {
                        Some(c) => send_chunk(
                            &chunks_in_flight,
                            &arc_stream,
                            &in_flight,
                            &fil,
                            c,
                            file_size,
                        ),
                        None => {
                            *dead_threads.lock().unwrap() += 1;
                            break;
                        }
                    };
                }
            }));
        }
        reader(
            &arc_stream,
            &chunks_in_flight,
            &in_flight,
            &dead_threads,
            workers,
        );

        handles.into_iter().for_each(|handle| {
            handle.join();
        });

        if confirm_completion(&arc_stream, &chunks_to_send) {
            break;
        };
    }

    let mut stream = arc_stream.lock().unwrap();

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
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                println!("44 header: {} not found", buf[0]);
            }
        };
    }

    let mut file_hash_buf: [u8; 32] = hash_file(fil).unwrap().try_into().unwrap();
    let mut buf = vec![0u8; 33];
    buf[1..].copy_from_slice(&mut file_hash_buf);
    buf[0] = 4;

    stream.write_all(&buf).unwrap();

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
                        stream.write_all(&file_hash_buf).unwrap();
                        println!("sent: {:?}", &file_hash_buf);
                        attempts += 1;
                    }
                }
                val => {
                    println!("44 header: {} not found", val);
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                println!("44 header: {} not found", buf[0]);
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
    let mut stream = arc_stream.lock().unwrap();

    stream.write_all(&buf).unwrap();

    println!("sent");

    // client: read count first

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
            }
        };
    }

    let mut count_buf = vec![0u8; 8];

    stream
        .read_exact(&mut count_buf)
        .map_err(|e| return e)
        .unwrap();

    let count = u64::from_be_bytes(count_buf.try_into().unwrap());

    println!("total missing: {:?}", count);

    if count == 0 {
        return true;
    }

    // then read exactly count * 8 bytes
    let mut missing_buf = vec![0u8; count as usize * 8];
    stream
        .read_exact(&mut missing_buf)
        .map_err(|e| return e)
        .unwrap();

    let mut missing = Vec::new();
    for chunk in missing_buf.chunks_exact(8) {
        missing.push(u64::from_be_bytes(chunk.try_into().unwrap()));
    }
    chunks_to_send.lock().unwrap().append(&mut missing);

    false
}

fn reader(
    arc_stream: &Arc<Mutex<TcpStream>>,
    chunks_in_flight: &Arc<Mutex<HashMap<u64, Duration>>>,
    in_flight: &Arc<Mutex<isize>>,
    dead_threads: &Arc<Mutex<usize>>,
    workers: usize,
) {
    let mut in_f: isize = { in_flight.lock().unwrap().clone() };
    while workers > *dead_threads.lock().unwrap() || in_f > 0 {
        let mut resp = [0u8; 16];

        let n = {
            let mut stream = arc_stream.lock().unwrap();
            stream.read(&mut resp)
        };

        match n {
            Ok(0) => {
                println!("closed");
                break;
            } // connection closed
            Ok(_) => {
                if resp[0] != 0 {
                    println!("{:?}", resp[0]);
                    if resp[0] == 20 {
                        let id = u64::from_be_bytes(resp[8..].try_into().unwrap());
                        chunks_in_flight.lock().unwrap().remove(&id);
                    }

                    *in_flight.lock().unwrap() -= 1;
                    println!("subtracted");
                } else {
                    println!("{:?}", resp);
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
            }
        }

        in_f = in_flight.lock().unwrap().clone();
    }
}

fn check_timeout_in_flight(
    in_flight: &Arc<Mutex<isize>>,
    chunks_in_flight: &Arc<Mutex<HashMap<u64, Duration>>>,
    mut counter: usize,
) -> usize {
    if in_flight.lock().unwrap().clone() > 5 {
        counter += 1;
        thread::sleep(Duration::from_millis(50));
        if counter >= 10 {
            let mut now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let removed: Vec<(u64, Duration)> = chunks_in_flight
                .lock()
                .unwrap()
                .extract_if(|_k, value| value < &mut now)
                .collect();
            counter = 0;
            let mut in_f = in_flight.lock().unwrap();
            *in_f -= removed.len() as isize;
            println!("in flight: {in_f}");
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
    fil.read_at(&mut buf, (CHUNK_SIZE - OVERHEAD) as u64 * id)
        .unwrap();

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

    println!(
        "sending: 2, {:?}, {:?}, {chunk_size}",
        &transfer_id.to_be_bytes(),
        &chunk_size.to_be_bytes(),
    );
    {
        println!("started writing");
        let mut lock = stream.lock().unwrap();
        lock.write_all(&buffer).unwrap();
        println!("stopped writing");
    }

    let _ = chunks_in_flight.lock().unwrap().insert(id, timestamp);
    *in_flight.lock().unwrap() += 1;
    println!("in_flight is now: {:?}", in_flight.lock().unwrap());
}

fn get_file_size(path: &Path) -> Result<u64, ErrorTransfer> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(_) => return Err(ErrorTransfer::NotFound),
    };

    let size = match file.metadata() {
        Ok(md) => md.len(),
        Err(_) => return Err(ErrorTransfer::InternalServerError),
    };
    println!("size: {size}");
    Ok(size)
}

fn get_chunks_len(file_size: u64) -> u32 {
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
