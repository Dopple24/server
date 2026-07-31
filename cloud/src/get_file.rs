use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{eprintln, println, thread};

use blake3::{Hash, Hasher};
use uuid::Uuid;

use crate::file_transfer::CHUNK_SIZE;
use crate::mapper::{MapStore, with_file_mut};
use crate::response::{Code, ErrorTransfer};

const OVERHEAD: usize = 17;
const MAX_IN_FLIGHT: usize = 10;

type InFlight = Arc<Mutex<HashMap<u64, Instant>>>;

struct Query {
    file_uuid: Uuid,
}

impl Query {
    fn from_bytes(bytes: &[u8], _buf_len: usize) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
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
    reinit: bool,
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
    let chunks_len_bytes = chunks_len.to_be_bytes();

    let mut buf = Vec::new();
    buf.push(20u8);
    buf.extend_from_slice(&chunks_len_bytes);

    match stream.write_all(&mut buf) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("connection failed: {e:?}");
            return;
        }
    };

    let initial_chunks: Vec<u64> = if reinit {
        match request_missing_chunks(&mut stream, chunks_len) {
            Ok(Some(missing)) => missing,
            Ok(None) => Vec::new(), // receiver already has everything — go straight to hash check
            Err(()) => return,
        }
    } else {
        (0..chunks_len).collect()
    };

    let arc_file = Arc::new(file);

    if !initial_chunks.is_empty() {
        // --- shared state ---
        let chunks_to_send = Arc::new((Mutex::new(initial_chunks), Condvar::new()));
        let in_flight: InFlight = Arc::new(Mutex::new(HashMap::new()));
        let aborted = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let stop_transfer_phase = Arc::new(AtomicBool::new(false));
        let timeout = Duration::from_secs(100);

        let mut attempt_count = 0;
        loop {
            done.store(false, Ordering::SeqCst);
            stop_transfer_phase.store(false, Ordering::SeqCst);
            attempt_count += 1;
            // --- writer ---
            let stream_clone = match stream.try_clone() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("failed to clone stream: {e:?}");
                    let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                    return;
                }
            };
            let (tx, writer_handle) = init_writer(stream_clone);

            // --- reader (acks) ---
            let reader_stream = match stream.try_clone() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("failed to clone stream: {e:?}");
                    let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                    return;
                }
            };
            let reader_handle = init_reader(
                reader_stream,
                in_flight.clone(),
                chunks_to_send.clone(),
                aborted.clone(),
                stop_transfer_phase.clone(),
            );

            // --- monitor (timeouts) ---
            let monitor_handle = init_monitor(
                in_flight.clone(),
                chunks_to_send.clone(),
                timeout,
                done.clone(),
            );

            let mut handles = Vec::new();

            for i in 0..(max_workers as u64).min({
                let guard = chunks_to_send.0.lock().unwrap_or_else(|e| {
                    eprintln!("chunks_to_send was poisoned: {e:?}");
                    e.into_inner()
                });
                guard.len() as u64
            }) {
                let arc_file = arc_file.clone();
                let chunks_to_send = chunks_to_send.clone();
                let tx = tx.clone();
                let in_flight = in_flight.clone();
                let aborted = aborted.clone();

                handles.push(thread::spawn(move || {
                    worker(arc_file, chunks_to_send, in_flight, tx, aborted);
                }));
                println!("worker {i:?} initialized");
            }
            drop(tx);

            for h in handles {
                let _ = h.join();
            }

            done.store(true, Ordering::SeqCst);
            stop_transfer_phase.store(true, Ordering::SeqCst);

            writer_handle.join().unwrap();
            reader_handle.join().unwrap();
            monitor_handle.join().unwrap();

            let missing_opt = match request_missing_chunks(&mut stream, chunks_len) {
                Ok(opt) => opt,
                Err(()) => return,
            };

            match missing_opt {
                None => {
                    break;
                }
                Some(mut missing) => {
                    if attempt_count > 5 {
                        eprintln!("failed to finish in 5 attempts, shutting down.");
                        return;
                    }
                    let mut guard = chunks_to_send.0.lock().unwrap_or_else(|e| {
                        eprintln!("chunks_to_send was poisoned");
                        e.into_inner()
                    });
                    guard.append(&mut missing);
                }
            }
        }
    }
    match stream.write_all(&[4u8]) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to write_all: {e:?}");
            return;
        }
    }
    println!("SENT 4");
    let hash_here = match hash_file(arc_file) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("failed to hash a file: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let mut hash_buf = [0u8; 33];
    match stream.read_exact(&mut hash_buf) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to read: {e:?}");
            return;
        }
    }
    match hash_buf[0] {
        24 => (),
        e => {
            eprintln!("hash check failed with code {e}");
            return;
        }
    }
    let hash = Hash::from_bytes(hash_buf[1..33].try_into().unwrap());
    if hash_here == hash {
        let _ = stream.write_all(&[20]);
    } else {
        let _ = stream.write_all(&[44]);
    }

    match with_file_mut(&query.file_uuid, &map_store, client_uuid, |fil| {
        fil.unlock()
    }) {
        Ok(fil) => fil,
        Err(e) => {
            eprintln!("failed to unlock: {e:?}");
            return;
        }
    };
}

/// Sends a completion-check request (byte `3`) and reads back which chunk ids
/// are still missing. Returns `None` if nothing is missing, `Some(ids)` otherwise.
/// Returns `Err` on any I/O failure (caller should treat as fatal).
fn request_missing_chunks(stream: &mut TcpStream, chunks_len: u64) -> Result<Option<Vec<u64>>, ()> {
    if let Err(e) = stream.write_all(&[3u8]) {
        eprintln!("failed to write: {e:?}");
        return Err(());
    }
    println!("sent 3");

    loop {
        let mut buf = [0u8; 9];
        if let Err(e) = stream.set_read_timeout(None) {
            eprintln!("failed to set read timeout to None: {e:?}");
            return Err(());
        }
        if let Err(e) = stream.read_exact(&mut buf) {
            eprintln!("failed to read: {e:?}");
            return Err(());
        }
        println!("{:?}", buf);
        match buf[0] {
            23 => {
                println!("23 came");
                let count = u64::from_be_bytes(buf[1..=8].try_into().unwrap());
                if count == 0 {
                    return Ok(None);
                }
                if count > chunks_len {
                    eprintln!("peer reported implausible missing count {count} > {chunks_len}");
                    return Err(());
                }
                let mut misbuf = vec![0u8; (count as usize) * 8];
                if let Err(e) = stream.read_exact(&mut misbuf) {
                    eprintln!("failed to read missing ids: {e:?}");
                    return Err(());
                }
                let missing: Vec<u64> = misbuf
                    .chunks_exact(8)
                    .map(|chunk| u64::from_be_bytes(chunk.try_into().unwrap()))
                    .collect();
                return Ok(Some(missing));
            }
            _ => continue,
        }
    }
}

fn init_writer(mut stream: TcpStream) -> (SyncSender<Vec<u8>>, JoinHandle<()>) {
    let (tx, rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(MAX_IN_FLIGHT);
    let writer_handle = thread::spawn(move || {
        for bytes in rx {
            // ends automatically once all SyncSenders are dropped
            if let Err(e) = stream.write_all(&bytes) {
                eprintln!("write failed: {e:?}");
                break;
            }
        }
    });
    (tx, writer_handle)
}

fn init_monitor(
    in_flight: InFlight,
    chunks_to_send: Arc<(Mutex<Vec<u64>>, Condvar)>,
    timeout: Duration,
    done: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            if done.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(Duration::from_millis(250));
            let expired: Vec<u64> = {
                let lock = in_flight.lock().unwrap_or_else(|e| e.into_inner());
                lock.iter()
                    .filter(|(_, t)| t.elapsed() > timeout)
                    .map(|(&id, _)| id)
                    .collect()
            };
            if expired.is_empty() {
                continue;
            }
            {
                let mut lock = in_flight.lock().unwrap_or_else(|e| e.into_inner());
                for id in &expired {
                    lock.remove(id);
                }
            }
            let (mtx, cvar) = &*chunks_to_send;
            let mut q = mtx.lock().unwrap_or_else(|e| e.into_inner());
            q.extend(expired);
            cvar.notify_all();
        }
    })
}

fn init_reader(
    mut stream: TcpStream,
    in_flight: InFlight,
    chunks_to_send: Arc<(Mutex<Vec<u64>>, Condvar)>,
    aborted: Arc<AtomicBool>,
    stop_transfer_phase: Arc<AtomicBool>,
) -> JoinHandle<()> {
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    thread::spawn(move || {
        let mut header = [0u8; 9];
        let mut filled = 0;
        loop {
            if aborted.load(Ordering::SeqCst) || stop_transfer_phase.load(Ordering::SeqCst) {
                return;
            }
            match stream.read(&mut header[filled..]) {
                Ok(0) => {
                    eprintln!("reader: connection closed");
                    return;
                }
                Ok(n) => {
                    filled += n;
                    if filled == header.len() {
                        let msg_type = header[0];
                        let chunk_id = u64::from_be_bytes(header[1..9].try_into().unwrap());

                        match msg_type {
                            20 => {
                                // success: just stop waiting on it
                                let mut lock = in_flight.lock().unwrap_or_else(|e| e.into_inner());
                                lock.remove(&chunk_id);
                            }
                            44 => {
                                // explicit failure from receiver: requeue immediately
                                {
                                    let mut lock =
                                        in_flight.lock().unwrap_or_else(|e| e.into_inner());
                                    lock.remove(&chunk_id);
                                }
                                let (mtx, cvar) = &*chunks_to_send;
                                let mut q = mtx.lock().unwrap_or_else(|e| e.into_inner());
                                q.push(chunk_id);
                                cvar.notify_one(); // wake a sleeping worker, see below
                            }
                            other => eprintln!("reader: unexpected type {other}"),
                        }
                        filled = 0;
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(e) => {
                    eprintln!("reader error: {e:?}");
                    return;
                }
            }
        }
    })
}

fn read_from_file_to_send(arc_file: &Arc<File>, chunk_id: u64) -> Result<Vec<u8>, ErrorTransfer> {
    let mut payload = vec![0u8; CHUNK_SIZE - OVERHEAD];
    let read_from = chunk_id * (CHUNK_SIZE - OVERHEAD) as u64;

    let read_bytes = arc_file.read_at(&mut payload, read_from).map_err(|e| {
        eprintln!("failed to read file at {chunk_id}: {e:?}");
        ErrorTransfer::InternalServerError
    })?;

    let mut out = Vec::with_capacity(OVERHEAD + read_bytes);
    out.push(2u8); // protocol header
    out.extend_from_slice(&chunk_id.to_be_bytes());
    out.extend_from_slice(&(read_bytes as u64).to_be_bytes());
    out.extend_from_slice(&payload[..read_bytes]);

    Ok(out)
}

fn worker(
    arc_file: Arc<File>,
    chunks_to_send: Arc<(Mutex<Vec<u64>>, Condvar)>,
    in_flight: InFlight,
    tx: SyncSender<Vec<u8>>,
    aborted: Arc<AtomicBool>,
) -> Result<(), ErrorTransfer> {
    let (mtx, cvar) = &*chunks_to_send;
    const MAX_RETRIES: u32 = 3;
    let mut retry_counts: HashMap<u64, u32> = HashMap::new();

    loop {
        if aborted.load(Ordering::SeqCst) {
            return Ok(()); // someone else already killed the transfer
        }

        let chunk_id = {
            let mut lock = mtx.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if aborted.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if let Some(id) = lock.pop() {
                    break Some(id);
                }
                let still_pending = !in_flight
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty();
                if !still_pending {
                    break None;
                }
                lock = cvar
                    .wait_timeout(lock, Duration::from_millis(500))
                    .unwrap()
                    .0;
            }
        };

        let Some(chunk_id) = chunk_id else { break };

        match read_from_file_to_send(&arc_file, chunk_id) {
            Ok(bytes) => {
                in_flight
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(chunk_id, Instant::now());
                if tx.send(bytes).is_err() {
                    break; // writer/connection gone
                }
            }
            Err(e) => {
                let count = retry_counts.entry(chunk_id).or_insert(0);
                *count += 1;
                if *count >= MAX_RETRIES {
                    eprintln!("chunk {chunk_id} failed permanently after {count} retries");
                    aborted.store(true, Ordering::SeqCst);
                    cvar.notify_all();
                    return Err(e); // fatal: local disk read is broken, not worth continuing
                }
                mtx.lock().unwrap_or_else(|e| e.into_inner()).push(chunk_id);
                cvar.notify_one();
            }
        }
    }
    Ok(())
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

pub fn get_chunks_len(file_size: u64) -> u64 {
    let payload = (CHUNK_SIZE - OVERHEAD) as u64;
    file_size.div_ceil(payload)
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
