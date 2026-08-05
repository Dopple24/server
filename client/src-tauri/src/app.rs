use crate::reinit::{PartSend, Parts};
use crate::request_file::ProgressPayload;
use crate::{guest_request_file, TransferStatus, UploadItem, UploadQueue};
use blake3::{Hash, Hasher};
use core::time;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::RwLock;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, State};
use tiny_http::Server;
use uuid::Uuid;

// --- protocol constants: must match the server exactly ---
pub const CHUNK_SIZE: usize = 32768;
pub const OVERHEAD: usize = 17; // 1 (type) + 8 (chunk_id) + 8 (payload_len)
pub const MAX_THREADS: usize = 5;
pub const PARTS_PATH: &str = "./parts.json";
pub const NEW_PARTS_PATH: &str = "./parts.json.new";
pub const SOCKET: &str = "127.0.0.1:6543";

const MSG_ACK_OK: u8 = 20;
const MSG_ACK_FAIL: u8 = 44;
const MSG_CHUNK: u8 = 2;
const MSG_COMPLETION_REQUEST: u8 = 3;
const MSG_COMPLETION_RESPONSE: u8 = 23;
const MSG_HASH_REQUEST: u8 = 4;
const MSG_HASH_RESPONSE: u8 = 24;

const MAX_IN_FLIGHT: usize = 32;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_COMPLETION_ATTEMPTS: u32 = 5;
const PROGRESS_EMIT_EVERY: u64 = 8;

#[derive(Debug)]
pub enum TransferError {
    InvalidLength,
    InvalidUuid,
    Overflow,
    FileNotFound,
    MetadataNotFound,
    Fatal,
}

type ChunkQueue = Arc<(Mutex<Vec<u64>>, Condvar)>;
type InFlight = Arc<Mutex<HashMap<u64, Instant>>>;

pub fn serve_public() {
    let server = Server::http("0.0.0.0:8080").unwrap();
    println!("Listening on port 8080");

    for req in server.incoming_requests() {
        let url = req.url().to_string();
        let parts: Vec<&str> = url.split('/').collect();

        // expects URLs like /dl/61b3bd2e-b5a1-40cd-a5d1-53214e9e6a73
        match parts.as_slice() {
            ["", "dl", uuid_str] => match Uuid::from_str(uuid_str) {
                Ok(uuid) => guest_request_file::handle_download(req, &uuid),
                Err(_) => {
                    let response =
                        tiny_http::Response::from_string("invalid uuid").with_status_code(400);
                    req.respond(response).unwrap();
                }
            },
            _ => {
                let response = tiny_http::Response::from_string("not found").with_status_code(404);
                req.respond(response).unwrap();
            }
        }
    }
}

pub fn get_parts_rw_lock() -> Arc<RwLock<Parts>> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(PARTS_PATH)
        .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    if contents.trim().is_empty() {
        let default = Parts {
            send: Vec::new(),
            acc: Vec::new(),
        };
        let serialized = serde_json::to_string_pretty(&default).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
        return Arc::new(RwLock::new(default));
    }

    Arc::new(RwLock::new(
        serde_json::from_str(&contents).expect("Failed to parse JSON"),
    ))
}

pub fn upload_batch(
    username: String,
    password: String,
    folder_uuid: String,
    upload_q: State<'_, UploadQueue>,
    paths: Vec<(String, String)>,
    handle: AppHandle,
) {
    paths.into_iter().for_each(|(path, frontend_uuid)| {
        let item = UploadItem {
            folder_uuid: folder_uuid.clone(),
            path,
            username: username.clone(),
            password: password.clone(),
            frontend_uuid: frontend_uuid.clone(),
            is_reinit: false,
        };
        let guard = upload_q.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = guard.send(item) {
            let status = TransferStatus {
                transfer_id: frontend_uuid.clone(),
                success: false,
                error: Some(e.to_string()),
            };
            let _ = handle.emit("transfer-complete", status);
        };
    });
}

pub fn sending(
    mut stream: TcpStream,
    path_str: &str,
    parts: Arc<RwLock<Parts>>,
    username: &str,
    password: &str,
    folder_uuid: &str,
    frontend_uuid: &str,
    app: &mut AppHandle,
) -> io::Result<()> {
    let path = Path::new(path_str);
    let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    let file_size = get_file_size(path).map_err(|_| Error::last_os_error())?;
    let transfer_uuid = Uuid::new_v4();
    let folder_uuid = Uuid::from_str(folder_uuid).map_err(|e| {
        eprintln!("failed to get uuid from str: {e:?}");
        Error::last_os_error()
    })?;

    let resp = send_handshake(
        &mut stream,
        file_size,
        path_str.as_bytes(),
        username,
        password,
        &transfer_uuid,
        &folder_uuid,
    )?;

    if resp[0] != MSG_ACK_OK {
        eprintln!("handshake rejected, code {}", resp[0]);
        return Ok(());
    }

    {
        let mut parts_write = parts.write().unwrap();
        parts_write.send.push(PartSend {
            path: path_str.to_string(),
            uuid: transfer_uuid,
            filename: filename.to_string(),
        });
        let _ = parts_write.save();
    }

    let file = Arc::new(File::open(path_str).map_err(|_| Error::last_os_error())?);
    let chunks_len = get_chunks_len(file_size);

    let result = run_send_loop(
        stream,
        file,
        file_size,
        chunks_len,
        (0..chunks_len).collect(),
        app,
        frontend_uuid.to_string(),
    );

    match result {
        Ok(()) => {
            let mut parts_write = parts.write().unwrap();
            if let Some(pos) = parts_write
                .send
                .iter()
                .position(|item| item.uuid == transfer_uuid)
            {
                parts_write.send.remove(pos);
            }
            let _ = parts_write.save();
            Ok(())
        }
        Err(e) => {
            eprintln!("transfer failed: {e:?}");
            // deliberately leave the parts.json entry in place so the transfer can be reinit'd
            Ok(())
        }
    }
}

/// Core transfer: sends `initial_chunks`, handles the completion-check retry
/// loop, then does the final hash exchange. Shared by fresh sends and reinit.
pub fn run_send_loop(
    mut stream: TcpStream,
    file: Arc<File>,
    file_size: u64,
    chunks_len: u64,
    initial_chunks: Vec<u64>,
    app: &mut AppHandle,
    frontend_uuid: String,
) -> Result<(), TransferError> {
    let chunks_to_send: ChunkQueue = Arc::new((Mutex::new(initial_chunks), Condvar::new()));
    let in_flight: InFlight = Arc::new(Mutex::new(HashMap::new()));
    let aborted = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let stop_transfer_phase = Arc::new(AtomicBool::new(false));
    let acked_count = Arc::new(AtomicU64::new(
        chunks_len - initial_queue_len(&chunks_to_send) as u64,
    ));

    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if attempt > MAX_COMPLETION_ATTEMPTS {
            eprintln!("gave up after {MAX_COMPLETION_ATTEMPTS} completion-check attempts");
            return Err(TransferError::Fatal);
        }

        done.store(false, Ordering::SeqCst);
        stop_transfer_phase.store(false, Ordering::SeqCst);

        let writer_stream = stream.try_clone().map_err(|_| TransferError::Fatal)?;
        let (tx, writer_handle) = init_writer(writer_stream);

        let reader_stream = stream.try_clone().map_err(|_| TransferError::Fatal)?;
        let reader_handle = init_reader(
            reader_stream,
            in_flight.clone(),
            chunks_to_send.clone(),
            aborted.clone(),
            stop_transfer_phase.clone(),
            app.clone(),
            chunks_len,
            &frontend_uuid,
        );

        let monitor_handle = init_monitor(
            in_flight.clone(),
            chunks_to_send.clone(),
            CHUNK_TIMEOUT,
            done.clone(),
        );

        let round_workers = {
            let (mtx, _) = &*chunks_to_send;
            (mtx.lock().unwrap().len() as u64)
                .min(MAX_THREADS as u64)
                .max(1)
        };

        let mut handles = Vec::new();
        for _ in 0..round_workers {
            let file = file.clone();
            let chunks_to_send = chunks_to_send.clone();
            let in_flight = in_flight.clone();
            let tx = tx.clone();
            let aborted = aborted.clone();
            let acked_count = acked_count.clone();
            let app = app.clone();
            let frontend_uuid = frontend_uuid.clone();
            handles.push(thread::spawn(move || {
                worker(
                    file,
                    file_size,
                    chunks_to_send,
                    in_flight,
                    tx,
                    aborted,
                    acked_count,
                    chunks_len,
                    app,
                    frontend_uuid,
                )
            }));
        }
        drop(tx);

        let mut worker_err = None;
        for h in handles {
            match h.join() {
                Ok(Err(e)) => worker_err = Some(e),
                Err(_) => worker_err = Some(TransferError::Fatal),
                Ok(Ok(())) => {}
            }
        }

        done.store(true, Ordering::SeqCst);
        stop_transfer_phase.store(true, Ordering::SeqCst);
        let _ = writer_handle.join();
        let _ = reader_handle.join();
        let _ = monitor_handle.join();

        if aborted.load(Ordering::SeqCst) {
            return Err(worker_err.unwrap_or(TransferError::Fatal));
        }

        match request_missing_chunks(&mut stream, chunks_len)? {
            None => break,
            Some(missing) => {
                let (mtx, _) = &*chunks_to_send;
                mtx.lock().unwrap().extend(missing);
            }
        }
    }

    // --- hash-check phase ---
    stream
        .write_all(&[MSG_HASH_REQUEST])
        .map_err(|_| TransferError::Fatal)?;

    let file = Arc::try_unwrap(file).map_err(|_| TransferError::Fatal)?;
    let file_hash = hash_file(file).map_err(|_| TransferError::Fatal)?;

    let mut hash_buf = [0u8; 33];
    stream
        .read_exact(&mut hash_buf)
        .map_err(|_| TransferError::Fatal)?;
    if hash_buf[0] != MSG_HASH_RESPONSE {
        eprintln!("expected hash response, got {}", hash_buf[0]);
        return Err(TransferError::Fatal);
    }

    let peer_hash = Hash::from_bytes(hash_buf[1..33].try_into().unwrap());

    if file_hash == peer_hash {
        stream
            .write_all(&[MSG_ACK_OK])
            .map_err(|_| TransferError::Fatal)?;
        let _ = app.emit(
            "transfer-progress",
            ProgressPayload {
                transfer_id: frontend_uuid,
                percent: 100,
            },
        );
        println!("success");
        Ok(())
    } else {
        eprintln!("hash mismatch after transfer");
        let _ = stream.write_all(&[MSG_ACK_FAIL]);
        Err(TransferError::Fatal)
    }
}

fn initial_queue_len(queue: &ChunkQueue) -> usize {
    let (mtx, _) = &**queue;
    mtx.lock().unwrap().len()
}

fn worker(
    file: Arc<File>,
    file_size: u64,
    chunks_to_send: ChunkQueue,
    in_flight: InFlight,
    tx: SyncSender<Vec<u8>>,
    aborted: Arc<AtomicBool>,
    acked_count: Arc<AtomicU64>,
    chunks_len: u64,
    app: AppHandle,
    frontend_uuid: String,
) -> Result<(), TransferError> {
    let (mtx, cvar) = &*chunks_to_send;
    const MAX_RETRIES: u32 = 3;
    let mut retry_counts: HashMap<u64, u32> = HashMap::new();

    loop {
        if aborted.load(Ordering::SeqCst) {
            return Ok(());
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

        match read_chunk(&file, file_size, chunk_id) {
            Ok(bytes) => {
                in_flight
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(chunk_id, Instant::now());
                if tx.send(bytes).is_err() {
                    break;
                }
            }
            Err(e) => {
                let count = retry_counts.entry(chunk_id).or_insert(0);
                *count += 1;
                if *count >= MAX_RETRIES {
                    eprintln!("chunk {chunk_id} failed permanently after {count} retries");
                    aborted.store(true, Ordering::SeqCst);
                    cvar.notify_all();
                    return Err(e);
                }
                mtx.lock().unwrap_or_else(|e| e.into_inner()).push(chunk_id);
                cvar.notify_one();
            }
        }
    }

    let _ = (acked_count, chunks_len, app, frontend_uuid); // progress is emitted from the reader on ack, see below
    Ok(())
}

fn read_chunk(file: &Arc<File>, file_size: u64, chunk_id: u64) -> Result<Vec<u8>, TransferError> {
    let payload_cap = (CHUNK_SIZE - OVERHEAD) as u64;
    let offset = chunk_id * payload_cap;
    let remaining = file_size.saturating_sub(offset);
    let this_len = remaining.min(payload_cap) as usize;

    let mut payload = vec![0u8; this_len];
    file.read_at(&mut payload, offset).map_err(|e| {
        eprintln!("failed to read chunk {chunk_id}: {e:?}");
        TransferError::Fatal
    })?;

    let mut out = Vec::with_capacity(OVERHEAD + this_len);
    out.push(MSG_CHUNK);
    out.extend_from_slice(&chunk_id.to_be_bytes());
    out.extend_from_slice(&(this_len as u64).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn init_writer(mut stream: TcpStream) -> (SyncSender<Vec<u8>>, JoinHandle<()>) {
    let (tx, rx) = sync_channel::<Vec<u8>>(MAX_IN_FLIGHT);
    let handle = thread::spawn(move || {
        for bytes in rx {
            if let Err(e) = stream.write_all(&bytes) {
                eprintln!("writer: write failed: {e:?}");
                break;
            }
        }
    });
    (tx, handle)
}

fn init_monitor(
    in_flight: InFlight,
    chunks_to_send: ChunkQueue,
    timeout: Duration,
    done: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || loop {
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
        mtx.lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(expired);
        cvar.notify_all();
    })
}

fn init_reader(
    mut stream: TcpStream,
    in_flight: InFlight,
    chunks_to_send: ChunkQueue,
    aborted: Arc<AtomicBool>,
    stop_transfer_phase: Arc<AtomicBool>,
    app: AppHandle,
    total_chunks: u64,
    frontend_uuid: &str,
) -> JoinHandle<()> {
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let frontend_uuid = frontend_uuid.to_string().clone();
    thread::spawn(move || {
        let mut not_emited_counter = 0;
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
                            MSG_ACK_OK => {
                                in_flight
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .remove(&chunk_id);
                                if not_emited_counter > PROGRESS_EMIT_EVERY {
                                    let count = { chunks_to_send.0.lock().unwrap().len() };

                                    let percent = 100
                                        - ((count as f64 / total_chunks as f64) * 100.0).min(100.0)
                                            as u8;
                                    println!("emiting {percent}%");
                                    let _ = app.emit(
                                        "transfer-progress",
                                        ProgressPayload {
                                            transfer_id: frontend_uuid.to_string(),
                                            percent,
                                        },
                                    );
                                    not_emited_counter = 0;
                                } else {
                                    not_emited_counter += 1;
                                }
                            }
                            MSG_ACK_FAIL => {
                                in_flight
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .remove(&chunk_id);
                                let (mtx, cvar) = &*chunks_to_send;
                                mtx.lock().unwrap_or_else(|e| e.into_inner()).push(chunk_id);
                                cvar.notify_one();
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
                    continue
                }
                Err(e) => {
                    eprintln!("reader error: {e:?}");
                    return;
                }
            }
        }
    })
}

/// Sends a completion-check request and reads back which chunk ids are missing.
pub fn request_missing_chunks(
    stream: &mut TcpStream,
    chunks_len: u64,
) -> Result<Option<Vec<u64>>, TransferError> {
    stream
        .write_all(&[MSG_COMPLETION_REQUEST])
        .map_err(|_| TransferError::Fatal)?;
    stream
        .set_read_timeout(None)
        .map_err(|_| TransferError::Fatal)?;

    loop {
        let mut buf = [0u8; 9];
        stream
            .read_exact(&mut buf)
            .map_err(|_| TransferError::Fatal)?;
        match buf[0] {
            MSG_COMPLETION_RESPONSE => {
                let count = u64::from_be_bytes(buf[1..=8].try_into().unwrap());
                if count == 0 {
                    return Ok(None);
                }
                if count > chunks_len {
                    eprintln!("peer reported implausible missing count {count} > {chunks_len}");
                    return Err(TransferError::Fatal);
                }
                let mut misbuf = vec![0u8; (count as usize) * 8];
                stream
                    .read_exact(&mut misbuf)
                    .map_err(|_| TransferError::Fatal)?;
                let missing: Vec<u64> = misbuf
                    .chunks_exact(8)
                    .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
                    .collect();
                return Ok(Some(missing));
            }
            _ => continue,
        }
    }
}

pub fn get_chunks_len(file_size: u64) -> u64 {
    let payload = (CHUNK_SIZE - OVERHEAD) as u64;
    file_size.div_ceil(payload)
}

pub fn get_file_size(path: &Path) -> Result<u64, TransferError> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|_| TransferError::FileNotFound)?;
    file.metadata()
        .map(|md| md.len())
        .map_err(|_| TransferError::MetadataNotFound)
}

fn send_handshake(
    stream: &mut TcpStream,
    file_size: u64,
    file_name: &[u8],
    username: &str,
    password: &str,
    transfer_uuid: &Uuid,
    folder_uuid: &Uuid,
) -> io::Result<[u8; 9]> {
    let mut buffer = Vec::new();
    buffer.push(1u8);

    let username_bytes = username.as_bytes();
    buffer.push(username_bytes.len() as u8);
    buffer.extend_from_slice(username_bytes);

    let password_bytes = password.as_bytes();
    buffer.push(password_bytes.len() as u8);
    buffer.extend_from_slice(password_bytes);

    buffer.extend_from_slice(&transfer_uuid.to_bytes_le());
    buffer.extend_from_slice(&file_size.to_be_bytes()); // plain u64, matches server's decode
    buffer.push(file_name.len() as u8);
    buffer.extend_from_slice(file_name);
    buffer.extend_from_slice(&folder_uuid.to_bytes_le());

    stream.write_all(&buffer)?;

    let mut resp = [0u8; 9];
    stream.read_exact(&mut resp)?;
    Ok(resp)
}

pub fn hash_file(mut file: File) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}
