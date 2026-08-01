use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::fs::{create_dir_all, read_dir, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use tauri::{AppHandle, Emitter, State};
use uuid::Uuid;

use crate::reinit::{first_message, PartAcc, Parts};
use crate::response::{self, Code, ErrorTransfer, TransferSuccess};

// --- protocol constants: must match the sender exactly ---
pub const CHUNK_SIZE: usize = 32768;
const OVERHEAD: usize = 17; // 1 (type) + 8 (chunk_id) + 8 (payload_len)
const MAX_IN_FLIGHT_JOBS: usize = 64; // bound on chunks buffered between reader and disk-writer workers

const TEMP_FOLDER_LOCATION: &str = "./temp";
const STORAGE_FOLDER_LOCATION: &str = "./storage";

// message types
const MSG_ACK_OK: u8 = 20;
const MSG_ACK_FAIL: u8 = 44;
const MSG_CHUNK: u8 = 2;
const MSG_COMPLETION_REQUEST: u8 = 3;
const MSG_COMPLETION_RESPONSE: u8 = 23;
const MSG_HASH_REQUEST: u8 = 4;
const MSG_HASH_RESPONSE: u8 = 24;

#[derive(serde::Serialize, Clone)]
pub struct ProgressPayload {
    pub transfer_id: String,
    pub percent: u8,
}

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    last_changed_at: u64,
    file_size_chunks: u64,
    transfered_chunks: HashSet<u64>,
    owner: Vec<Uuid>,
    is_public: bool,
}

#[derive(Debug)]
struct TransferedFile {
    file_size_chunks: u64,
    storage_path: PathBuf,
    temp_path: PathBuf,
    config_path: Mutex<PathBuf>,
    file: Arc<File>,
}

/// A chunk that's been fully read off the wire and is ready to be written to disk.
struct ChunkJob {
    chunk_id: u64,
    payload: Vec<u8>,
}

/// Chunk ids already durably written, shared between disk-writer workers and
/// the reader thread (which needs it to answer completion-check requests).
type ChunkLog = Arc<Mutex<HashSet<u64>>>;

// =====================================================================
// Public entry points
// =====================================================================

pub fn request(
    mut stream: TcpStream,
    max_workers: usize,
    parts: State<Arc<RwLock<Parts>>>,
    username: &str,
    password: &str,
    fil_uuid: &str,
    path_for_the_requested_file: &str,
    frontend_uuid: &str,
    app: AppHandle,
) -> io::Result<()> {
    let path = Path::new(path_for_the_requested_file);
    let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    let file_uuid = Uuid::from_str(fil_uuid)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad uuid: {e}")))?;

    stream.write_all(&first_message(5, &file_uuid, username, password))?;

    let (code, chunks_len) = read_handshake_response(&mut stream)?;
    match code {
        MSG_ACK_OK => {
            let temp_path = format!("{TEMP_FOLDER_LOCATION}/{filename}");
            {
                let mut parts_write = parts.write().unwrap();
                parts_write.acc.push(PartAcc {
                    uuid: Uuid::new_v4(),
                    temp_path: temp_path.clone(),
                    real_path: path_for_the_requested_file.to_string(),
                    server_uuid: file_uuid.to_string(),
                });
                let res = parts_write.save();
            }

            receive_file(
                stream,
                &temp_path,
                path_for_the_requested_file,
                max_workers,
                chunks_len,
                app,
                frontend_uuid,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e:?}")))?;

            let mut parts_write = parts.write().unwrap();
            if let Some(pos) = parts_write
                .acc
                .iter()
                .position(|item| item.server_uuid == file_uuid.to_string())
            {
                parts_write.acc.remove(pos);
            }
            let _ = parts_write.save();
            Ok(())
        }
        48 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "forbidden")),
        other => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("handshake failed with code {other}"),
        )),
    }
}

pub fn reinitialize(
    mut stream: TcpStream,
    parts: State<Arc<RwLock<Parts>>>,
    max_workers: usize,
    acc_uuid: &str,
    username: &str,
    password: &str,
    frontend_uuid: &str,
    app: AppHandle,
) -> io::Result<()> {
    let (real_path, temp_path, file_uuid) = {
        let uuid = Uuid::from_str(acc_uuid).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("invalid uuid: {e}"))
        })?;
        let parts_read = parts.read().unwrap();
        let part = parts_read
            .acc
            .iter()
            .find(|p| p.uuid == uuid)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "uuid not found"))?;
        let server_uuid = Uuid::from_str(&part.server_uuid).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad server uuid: {e}"))
        })?;
        (part.real_path.clone(), part.temp_path.clone(), server_uuid)
    };

    stream.write_all(&first_message(6, &file_uuid, username, password))?;

    let (code, chunks_len) = read_handshake_response(&mut stream)?;
    match code {
        MSG_ACK_OK => {}
        48 => return Err(io::Error::new(io::ErrorKind::PermissionDenied, "forbidden")),
        other => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("handshake failed with code {other}"),
            ))
        }
    }

    ensure_dirs_exist()?;

    let existing_path_string = format!("{temp_path}.config");
    let existing_path = Path::new(&existing_path_string);
    let contents = fs::read_to_string(existing_path)?;
    let config_file: ConfigFile = serde_json::from_str(&contents)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad config: {e}")))?;

    let file_name = existing_path
        .file_stem()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad config filename"))?;
    let temp_at = Path::new(TEMP_FOLDER_LOCATION).join(file_name);

    let file = OpenOptions::new().write(true).create(true).open(&temp_at)?;

    let transfered_file = Arc::new(TransferedFile {
        file_size_chunks: config_file.file_size_chunks,
        file: Arc::new(file),
        temp_path: temp_at,
        storage_path: Path::new(&real_path).to_path_buf(),
        config_path: Mutex::new(existing_path.to_path_buf()),
    });

    // chunks_len from the handshake should agree with what our own config recorded;
    // if it doesn't, something is inconsistent between client and server bookkeeping.
    if chunks_len != config_file.file_size_chunks {
        eprintln!(
            "warning: server reports {chunks_len} chunks but local config expects {}",
            config_file.file_size_chunks
        );
    }

    let chunk_log: ChunkLog = Arc::new(Mutex::new(config_file.transfered_chunks.clone()));

    run_transfer_loop(
        stream,
        &transfered_file,
        max_workers,
        chunk_log,
        app,
        frontend_uuid,
    )
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e:?}")))?;

    finalize_transfer(&transfered_file)?;

    let mut parts_write = parts.write().unwrap();
    if let Some(pos) = parts_write
        .acc
        .iter()
        .position(|item| item.server_uuid == file_uuid.to_string())
    {
        parts_write.acc.remove(pos);
    }
    let _ = parts_write.save();

    Ok(())
}

fn receive_file(
    stream: TcpStream,
    temp_path: &str,
    real_path: &str,
    max_workers: usize,
    file_size_chunks: u64,
    app: AppHandle,
    frontend_uuid: &str,
) -> Result<(), ErrorTransfer> {
    let transfered_file = init_transfer(temp_path, real_path, file_size_chunks)?;
    setup_config(&transfered_file).map_err(|_| ErrorTransfer::InternalServerError)?;

    let transfered_file = Arc::new(transfered_file);
    let chunk_log: ChunkLog = Arc::new(Mutex::new(HashSet::new()));

    run_transfer_loop(
        stream,
        &transfered_file,
        max_workers,
        chunk_log,
        app,
        frontend_uuid,
    )?;
    finalize_transfer(&transfered_file).map_err(|_| ErrorTransfer::InternalServerError)?;
    Ok(())
}

// =====================================================================
// Core transfer loop, shared by fresh transfers and reinit
// =====================================================================

fn run_transfer_loop(
    mut stream: TcpStream,
    transfered_file: &Arc<TransferedFile>,
    max_workers: usize,
    chunk_log: ChunkLog,
    app: AppHandle,
    frontend_uuid: &str,
) -> Result<(), ErrorTransfer> {
    println!("started run transfer loop");
    let writer_stream = stream
        .try_clone()
        .map_err(|_| ErrorTransfer::InternalServerError)?;
    let (tx, writer_handle) = init_writer(writer_stream);

    let (job_tx, job_rx): (SyncSender<ChunkJob>, Receiver<ChunkJob>) =
        sync_channel(MAX_IN_FLIGHT_JOBS);
    let job_rx = Arc::new(Mutex::new(job_rx));

    let mut worker_handles = Vec::new();
    for _ in 0..max_workers.max(1) {
        let job_rx = job_rx.clone();
        let tx = tx.clone();
        let chunk_log = chunk_log.clone();
        let file = transfered_file.clone();
        let app = app.clone();
        let frontend_uuid = frontend_uuid.to_string().clone();
        worker_handles.push(thread::spawn(move || {
            disk_writer_worker(job_rx, tx, chunk_log, file, app, &frontend_uuid)
        }));
    }
    // drop our own senders so the channel closes once the reader loop stops feeding it
    drop(job_tx.clone());

    // --- main reader loop: drives the whole exchange sequentially ---
    let outcome = reader_loop(&mut stream, &job_tx, &tx, &chunk_log, transfered_file);
    drop(job_tx); // let workers drain remaining jobs, then exit
    drop(tx); // let the writer thread flush and exit once workers are done acking

    for h in worker_handles {
        let _ = h.join();
    }
    let _ = writer_handle.join();

    let outcome = outcome?;

    // --- hash-check phase: reader loop already exited, socket read/write is ours alone now ---
    let local_hash =
        hash_file(&transfered_file.temp_path).map_err(|_| ErrorTransfer::InternalServerError)?;
    let mut hash_msg = Vec::with_capacity(33);
    hash_msg.push(MSG_HASH_RESPONSE);
    hash_msg.extend_from_slice(local_hash.as_bytes());
    stream
        .write_all(&hash_msg)
        .map_err(|_| ErrorTransfer::InternalServerError)?;

    let mut verdict = [0u8; 1];
    stream
        .read_exact(&mut verdict)
        .map_err(|_| ErrorTransfer::InternalServerError)?;
    match verdict[0] {
        MSG_ACK_OK => {}
        MSG_ACK_FAIL => return Err(ErrorTransfer::HashesDoNotMatch),
        other => {
            eprintln!("unexpected final verdict byte: {other}");
            return Err(ErrorTransfer::InternalServerError);
        }
    }

    let _ = outcome;
    Ok(())
}

/// Reads messages off the wire until a hash-check request (`4`) arrives.
/// Chunk data (`2`) is handed off to worker threads; completion-check
/// requests (`3`) are answered directly against the shared chunk log.
fn reader_loop(
    stream: &mut TcpStream,
    job_tx: &SyncSender<ChunkJob>,
    resp_tx: &SyncSender<Vec<u8>>,
    chunk_log: &ChunkLog,
    transfered_file: &Arc<TransferedFile>,
) -> Result<(), ErrorTransfer> {
    loop {
        let mut msg_type = [0u8; 1];
        stream
            .read_exact(&mut msg_type)
            .map_err(|_| ErrorTransfer::InternalServerError)?;

        match msg_type[0] {
            MSG_CHUNK => {
                let mut header = [0u8; OVERHEAD - 1]; // chunk_id (8) + payload_len (8)
                stream
                    .read_exact(&mut header)
                    .map_err(|_| ErrorTransfer::InternalServerError)?;
                let chunk_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
                let payload_len = u64::from_be_bytes(header[8..16].try_into().unwrap()) as usize;

                if payload_len > CHUNK_SIZE - OVERHEAD {
                    eprintln!("peer sent implausible payload_len {payload_len}, aborting");
                    return Err(ErrorTransfer::InternalServerError);
                }

                let mut payload = vec![0u8; payload_len];
                stream
                    .read_exact(&mut payload)
                    .map_err(|_| ErrorTransfer::InternalServerError)?;

                // backpressure: this blocks if MAX_IN_FLIGHT_JOBS workers are all busy,
                // which naturally throttles how fast we read off the socket
                if job_tx.send(ChunkJob { chunk_id, payload }).is_err() {
                    // worker side is gone; nothing more we can do
                    return Err(ErrorTransfer::InternalServerError);
                }
            }
            MSG_COMPLETION_REQUEST => {
                let response =
                    build_completion_response(chunk_log, transfered_file.file_size_chunks);
                if resp_tx.send(response).is_err() {
                    return Err(ErrorTransfer::InternalServerError);
                }
            }
            MSG_HASH_REQUEST => {
                return Ok(()); // hand control back to run_transfer_loop for the hash exchange
            }
            other => {
                eprintln!("reader: unexpected message type {other}");
                // don't kill the transfer over one stray/unknown byte; keep going
            }
        }
    }
}

fn build_completion_response(chunk_log: &ChunkLog, file_size_chunks: u64) -> Vec<u8> {
    let present = chunk_log.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let missing: Vec<u64> = (0..file_size_chunks)
        .filter(|id| !present.contains(id))
        .collect();

    let mut buf = Vec::with_capacity(9 + missing.len() * 8);
    buf.push(MSG_COMPLETION_RESPONSE);
    buf.extend_from_slice(&(missing.len() as u64).to_be_bytes());

    for id in missing {
        buf.extend_from_slice(&id.to_be_bytes());
    }
    buf
}

fn disk_writer_worker(
    job_rx: Arc<Mutex<Receiver<ChunkJob>>>,
    tx: SyncSender<Vec<u8>>,
    chunk_log: ChunkLog,
    transfered_file: Arc<TransferedFile>,
    app: AppHandle,
    frontend_uuid: &str,
) {
    loop {
        let job = {
            let rx = job_rx.lock().unwrap_or_else(|e| e.into_inner());
            rx.recv()
        };
        let Ok(job) = job else { break }; // channel closed: no more work, ever

        let offset = job.chunk_id * (CHUNK_SIZE - OVERHEAD) as u64;
        let write_result = transfered_file.file.write_at(&job.payload, offset);

        let ack = match write_result {
            Ok(_) => {
                let mut log = chunk_log.lock().unwrap_or_else(|e| e.into_inner());
                log.insert(job.chunk_id);
                let count = log.len();
                drop(log);
                if count % 32 == 0 {
                    let _ = update_config(&transfered_file.config_path, &chunk_log);
                    let count = { chunk_log.lock().unwrap().len() };

                    let percent = ((count as f64 / transfered_file.file_size_chunks as f64) * 100.0)
                        .min(100.0) as u8;
                    println!("emiting {percent}%");
                    let _ = app.emit(
                        "transfer-progress",
                        ProgressPayload {
                            transfer_id: frontend_uuid.to_string(),
                            percent,
                        },
                    );
                }
                build_ack(MSG_ACK_OK, job.chunk_id)
            }
            Err(e) => {
                eprintln!("failed to write chunk {}: {e:?}", job.chunk_id);
                build_ack(MSG_ACK_FAIL, job.chunk_id)
            }
        };

        if tx.send(ack).is_err() {
            break; // writer/connection gone
        }
    }
}

fn build_ack(code: u8, chunk_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(code);
    buf.extend_from_slice(&chunk_id.to_be_bytes());
    buf
}

fn init_writer(mut stream: TcpStream) -> (SyncSender<Vec<u8>>, JoinHandle<()>) {
    let (tx, rx) = sync_channel::<Vec<u8>>(MAX_IN_FLIGHT_JOBS);
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

// =====================================================================
// Setup / teardown helpers
// =====================================================================

fn ensure_dirs_exist() -> io::Result<()> {
    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);
    if !temp_location.exists() {
        create_dir_all(temp_location)?;
    }
    if !stor_location.exists() {
        create_dir_all(stor_location)?;
    }
    Ok(())
}

fn init_transfer(
    temp_path: &str,
    real_path: &str,
    file_size_chunks: u64,
) -> Result<TransferedFile, ErrorTransfer> {
    ensure_dirs_exist().map_err(|_| ErrorTransfer::InternalServerError)?;

    let config_file_path = format!("{temp_path}.config");
    let path = Path::new(temp_path);
    let storage_path = Path::new(real_path);
    let config_path = Path::new(&config_file_path);

    if path.exists() || storage_path.exists() || config_path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }

    let file = File::create(path).map_err(|e| {
        eprintln!("file creation failed: {e:?}");
        ErrorTransfer::InternalServerError
    })?;

    Ok(TransferedFile {
        file_size_chunks,
        file: Arc::new(file),
        temp_path: path.to_path_buf(),
        storage_path: storage_path.to_path_buf(),
        config_path: Mutex::new(config_path.to_path_buf()),
    })
}

fn setup_config(transfered_file: &TransferedFile) -> io::Result<()> {
    let config_path = transfered_file.config_path.lock().unwrap().clone();
    let mut config_file = File::create(config_path)?;

    let config = ConfigFile {
        last_changed_at: now_nanos(),
        file_size_chunks: transfered_file.file_size_chunks,
        transfered_chunks: HashSet::new(),
        is_public: false,
        owner: Vec::new(),
    };

    let json = serde_json::to_string_pretty(&config)?;
    config_file.write_all(json.as_bytes())?;
    Ok(())
}

fn update_config(config_path: &Mutex<PathBuf>, chunk_log: &ChunkLog) -> io::Result<()> {
    let path = config_path.lock().unwrap().clone();
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

    let reader = BufReader::new(&file);
    let mut config: ConfigFile = serde_json::from_reader(reader)?;

    let current = chunk_log.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if config.transfered_chunks == current {
        return Ok(());
    }

    config.last_changed_at = now_nanos();
    config.transfered_chunks = current;

    let json = serde_json::to_string_pretty(&config)?;
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

fn finalize_transfer(transfered_file: &Arc<TransferedFile>) -> io::Result<()> {
    fs::copy(&transfered_file.temp_path, &transfered_file.storage_path)?;
    fs::remove_file(&transfered_file.temp_path)?;
    let cfg_path = transfered_file.config_path.lock().unwrap().clone();
    fs::remove_file(&cfg_path)?;
    Ok(())
}

fn find_temp_files(dir: &Path, results: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            find_temp_files(&path, results)?;
        } else if path.extension().map_or(false, |ext| ext == "config") {
            results.push(path);
        }
    }
    Ok(())
}

/// Reads the 9-byte handshake ack: `[code: u8][chunks_len: u64]`.
/// `chunks_len` is only meaningful when `code == MSG_ACK_OK`.
fn read_handshake_response(stream: &mut TcpStream) -> io::Result<(u8, u64)> {
    let mut buf = [0u8; 9];
    stream.read_exact(&mut buf)?;
    let code = buf[0];
    let chunks_len = u64::from_be_bytes(buf[1..9].try_into().unwrap());
    Ok((code, chunks_len))
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn hash_file(path: &Path) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 65536];
    let mut file = File::open(path)?;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}
