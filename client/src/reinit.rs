use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::FileExt,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::{CHUNK_SIZE, MAX_THREADS, OVERHEAD, hash_file, send_chunk};

#[derive(Deserialize, Serialize, Debug)]
pub struct Parts {
    pub send: Vec<PartSend>,
    pub acc: Vec<PartAcc>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PartSend {
    pub uuid: Uuid,
    pub filename: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PartAcc {
    pub path: String,
    pub server_uuid: String,
}

pub fn reinit(mut stream: TcpStream, uuid: &Uuid, filename: &str) -> std::io::Result<()> {
    let file_size = crate::get_file_size(Path::new(filename)).unwrap();
    stream.write_all(&first_message(uuid))?;
    let mut buf = [0u8; CHUNK_SIZE];
    stream.read(&mut buf)?;
    println!("buf: {}", buf[0]);

    let fil = Arc::new(File::open(filename).unwrap());

    let chunks_len = (file_size / (CHUNK_SIZE - OVERHEAD) as u64) + 1;

    let chunks_to_send: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let arc_stream: Arc<Mutex<TcpStream>> = Arc::new(Mutex::new(stream));

    //u64 is id
    let chunks_in_flight: Arc<Mutex<HashMap<u64, Duration>>> = Arc::new(Mutex::new(HashMap::new()));

    {
        arc_stream.lock().unwrap().set_nonblocking(true).unwrap();
    }

    loop {
        let mut handles = Vec::new();

        let in_flight = Arc::new(Mutex::new(0));
        let threads = chunks_len.min(MAX_THREADS);
        let dead_threads = Arc::new(Mutex::new(0));

        for i in 0..threads {
            let dead_threads = dead_threads.clone();
            let in_flight = in_flight.clone();
            let chunks = chunks_to_send.clone();
            let stream_clone = arc_stream.clone();
            let file_clone = fil.clone();
            let chunks_in_flight = chunks_in_flight.clone();
            handles.push(thread::spawn(move || {
                println!("worker #{} initialized", i);
                let mut counter = 0;
                loop {
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
                        continue;
                    }
                    let chunk = { chunks.lock().unwrap().pop() };

                    if let Some(index) = chunk {
                        counter = 0;
                        println!("worker #{} took chunk #{:?}", i, chunk);
                        let remaining = file_size - (CHUNK_SIZE - OVERHEAD) as u64 * index;
                        let chunk_size = remaining.min((CHUNK_SIZE - OVERHEAD) as u64) as usize;

                        let mut buf = vec![0u8; chunk_size];
                        file_clone
                            .read_at(&mut buf, (CHUNK_SIZE - OVERHEAD) as u64 * index)
                            .unwrap();
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .saturating_add(Duration::from_secs(10));
                        match send_chunk(&stream_clone, index, &buf) {
                            Ok(_) => {
                                let _ = chunks_in_flight.lock().unwrap().insert(index, timestamp);
                                *in_flight.lock().unwrap() += 1;
                            }
                            Err(_) => {}
                        };
                        println!("in_flight is now: {:?}", in_flight.lock().unwrap());
                    } else {
                        println!("#{i} died");
                        *dead_threads.lock().unwrap() += 1;
                        break;
                    }
                }
            }));
        }

        let mut in_f: isize = { in_flight.lock().unwrap().clone() };
        while threads as usize > *dead_threads.lock().unwrap() || in_f > 0 {
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
        println!("here");
        handles.into_iter().for_each(|handle| {
            handle.join();
        });
        println!("there");

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

        println!("code: {:?}", response_code);

        let code = response_code[0];

        println!("response_code: {code}");

        let mut count_buf = vec![0u8; 8];
        stream.read_exact(&mut count_buf).map_err(|e| return e)?;

        let count = u64::from_be_bytes(count_buf.try_into().unwrap());

        println!("total missing: {:?}", count);

        if count == 0 {
            break;
        }

        // then read exactly count * 8 bytes
        let mut missing_buf = vec![0u8; count as usize * 8];
        stream.read_exact(&mut missing_buf).map_err(|e| return e)?;

        let mut missing = Vec::new();
        for chunk in missing_buf.chunks_exact(8) {
            missing.push(u64::from_be_bytes(chunk.try_into().unwrap()));
        }
        chunks_to_send.lock().unwrap().append(&mut missing);
    }

    let mut stream = arc_stream.lock().unwrap();

    println!("waiting for server");

    loop {
        let mut buf = [0u8; 1];
        match stream.read_exact(&mut buf) {
            Ok(_) => match buf[0] {
                21 => {
                    println!("success");
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

    println!("sending hash");

    let mut file_hash_buf: [u8; 32] = hash_file(fil).unwrap().try_into().unwrap();
    let mut buf = vec![0u8; 33];
    buf[1..].copy_from_slice(&mut file_hash_buf);
    buf[0] = 4;

    stream.write_all(&buf).unwrap();

    println!("sent {:?}", buf);

    loop {
        let mut buf = [0u8; 1];
        match stream.read_exact(&mut buf) {
            Ok(_) => match buf[0] {
                24 => {
                    println!("success");
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

    Ok(())
}

fn first_message(uuid: &Uuid) -> [u8; 17] {
    let mut buf = [0u8; 17];
    buf[0] = 10;
    buf[1..].copy_from_slice(uuid.as_bytes());
    buf
}
