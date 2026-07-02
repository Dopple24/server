use blake3::{Hash, Hasher};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Error, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const CHUNK_SIZE: usize = 32768;
const OVERHEAD: usize = 11;
const MAX_THREADS: u64 = 5;

#[derive(Debug)]
enum TransferError {
    InvalidLength,
    InvalidUuid,
    Overflow,
    FileNotFound,
    MetadataNotFound,
}

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:6543")?;

    let file_size = get_file_size(Path::new("./test.txt")).unwrap();
    let resp = send(&mut stream, file_size, "test.txt".as_bytes())?;
    println!("response code: {:?}", &resp.clone()[0]);
    println!("response: {}", String::from_utf8_lossy(&resp[1..]));

    if &resp.clone()[0] != &20 {
        println!("{}", &resp.clone()[0]);
        return Ok(());
    }

    let fil = Arc::new(File::open("./test.txt").unwrap());

    let chunks_len = (file_size / (CHUNK_SIZE - OVERHEAD) as u64) + 1;

    let chunks_to_send: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let arc_stream: Arc<Mutex<TcpStream>> = Arc::new(Mutex::new(stream));

    //u64 is id
    let chunks_in_flight: Arc<Mutex<HashMap<u64, Duration>>> = Arc::new(Mutex::new(HashMap::new()));

    {
        let mut lock = chunks_to_send.lock().unwrap();
        for i in 1..chunks_len {
            lock.push(i);
        }
    }

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

fn get_file_size(path: &Path) -> Result<u64, TransferError> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(_) => return Err(TransferError::FileNotFound),
    };

    let size = match file.metadata() {
        Ok(md) => md.len(),
        Err(_) => return Err(TransferError::MetadataNotFound),
    };
    println!("size: {size}");
    Ok(size)
}

fn encode_file_size(mut value: u64) -> [u8; 7] {
    let mut out = [0u8; 7];

    for i in 0..7 {
        out[i] = (value & 0x7F) as u8; // take 7 bits
        value >>= 7;
    }

    out
}

fn send(stream: &mut TcpStream, data: u64, file_name: &[u8]) -> Result<[u8; 128], Error> {
    let transfer_uuid = Uuid::new_v4();
    let file_size = data;

    println!("{:?}", transfer_uuid);

    let size = encode_file_size(file_size);

    let mut buffer = Vec::with_capacity(24);
    buffer.extend_from_slice(&[1]);
    buffer.extend_from_slice(&transfer_uuid.to_bytes_le());
    buffer.extend_from_slice(&size);
    buffer.extend_from_slice(&[file_name.len() as u8]);
    buffer.extend_from_slice(&file_name);
    //buffer.extend_from_slice(&msg);

    println!(
        "{:?}, {:?}, {:?}",
        buffer,
        &transfer_uuid.to_bytes_le(),
        &file_size
    );

    match stream.write_all(&buffer) {
        Ok(_) => (),
        Err(y) => return Err(y),
    };
    let mut resp = [0u8; 128];
    match stream.read(&mut resp) {
        Ok(_) => (),
        Err(res) => return Err(res),
    };
    Ok(resp)
}

fn send_chunk(stream: &Arc<Mutex<TcpStream>>, id: u64, data: &[u8]) -> Result<(), Error> {
    let transfer_id: u64 = id;
    let msg = data;

    let chunk_size: u16 = msg.len() as u16;

    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    buffer.extend_from_slice(&[2]);
    buffer.extend_from_slice(&transfer_id.to_be_bytes());
    buffer.extend_from_slice(&chunk_size.to_be_bytes());
    buffer.extend_from_slice(&msg);

    println!(
        "sending: 2, {:?}, {:?}, {chunk_size}",
        &transfer_id.to_be_bytes(),
        &chunk_size.to_be_bytes(),
    );
    {
        println!("started writing");
        let mut lock = stream.lock().unwrap();
        match lock.write_all(&buffer) {
            Ok(_) => (),
            Err(y) => return Err(y),
        };
        println!("stopped writing");
    }
    Ok(())
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
