use crate::map_tracker::notify_change;
use crate::mapper::Fil;
use crate::mapper::MapStore;
use crate::response;
use crate::response::{Code, ErrorTransfer, TransferSuccess};
use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::fs::read_dir;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Condvar;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// --- protocol constants: must match the sender exactly ---
pub const CHUNK_SIZE: usize = 32768;
pub const OVERHEAD: usize = 17; // 1 (type) + 8 (chunk_id) + 8 (payload_len)
const MAX_IN_FLIGHT_JOBS: usize = 64;

const TEMP_FOLDER_LOCATION: &str = "./temp";
const STORAGE_FOLDER_LOCATION: &str = "./storage";

const MSG_ACK_OK: u8 = 20;
const MSG_ACK_FAIL: u8 = 44;
const MSG_CHUNK: u8 = 2;
const MSG_COMPLETION_REQUEST: u8 = 3;
const MSG_COMPLETION_RESPONSE: u8 = 23;
const MSG_HASH_REQUEST: u8 = 4;
const MSG_HASH_RESPONSE: u8 = 24;

#[derive(Serialize, Deserialize, Debug)]
struct ConfigFile {
    last_changed_at: u64,
    uuid: Uuid,
    parent_folder_uuid: Uuid,
    file_size_chunks: u64,
    transfered_chunks: HashSet<u64>,
    owner: Vec<Uuid>,
    is_public: bool,
}

#[derive(Debug)]
struct TransferedFile {
    uuid: Uuid,
    file_size_chunks: u64,
    parent_folder_uuid: Uuid,
    storage_path: PathBuf,
    temp_path: PathBuf,
    config_path: Mutex<PathBuf>,
    file: Arc<File>,
}

struct ChunkJob {
    chunk_id: u64,
    payload: Vec<u8>,
}

type ChunkLog = Arc<Mutex<HashSet<u64>>>;

// =====================================================================
// Public entry points
// =====================================================================

pub fn reinitialize(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    signal: &(Mutex<u64>, Condvar),
) {
    println!("reinitialization called by client: {client_uuid:?}");

    let uuid = Uuid::from_bytes(first_message[offset..16 + offset].try_into().unwrap());

    let mut files: Vec<PathBuf> = Vec::new();
    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);

    if let Err(e) = ensure_dirs_exist(temp_location, stor_location) {
        eprintln!("failed to create required directories: {e}");
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }

    find_temp_files(temp_location, &mut files);

    let existing_path = match files.iter().find(|path| {
        let contents = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skipping {path:?}: failed to read: {e}");
                return false;
            }
        };
        let config: ConfigFile = match serde_json::from_str(&contents) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skipping {path:?}: invalid config: {e}");
                return false;
            }
        };
        config.uuid == uuid
    }) {
        Some(p) => p,
        None => {
            eprintln!("didn't find a temp file matching sent uuid");
            let _ = stream.write_all(&[ErrorTransfer::NotFound.get_code()]);
            return;
        }
    };

    let contents = match fs::read_to_string(existing_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed at {existing_path:?}: failed to read: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let config_file: ConfigFile = match serde_json::from_str(&contents) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed at {existing_path:?}: invalid config: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let file_name = match existing_path.file_stem() {
        Some(f) => f,
        None => {
            eprintln!("couldn't derive file stem from {existing_path:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let temp_at = temp_location.join(file_name);

    let file = match OpenOptions::new().write(true).create(true).open(&temp_at) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("failed to open {temp_at:?}: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let storage_path = match temp_to_storage(temp_at.as_path()) {
        Some(p) => p,
        None => {
            eprintln!("{temp_at:?} not in ./temp");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let transfered_file = Arc::new(TransferedFile {
        file_size_chunks: config_file.file_size_chunks,
        file: Arc::new(file),
        temp_path: temp_at,
        parent_folder_uuid: config_file.parent_folder_uuid,
        storage_path,
        config_path: Mutex::new(existing_path.to_path_buf()),
        uuid,
    });

    if let Err(e) = stream.write_all(response::TransferSuccess::Ok.respond(Vec::new()).as_slice()) {
        eprintln!("stream write failed: {e:?}");
        return;
    }

    let chunk_log: ChunkLog = Arc::new(Mutex::new(config_file.transfered_chunks.clone()));

    if run_transfer_loop(stream, &transfered_file, max_workers, chunk_log).is_err() {
        return; // errors already logged inside; peer already informed where possible
    }

    finish_and_register(
        &transfered_file,
        &map_store,
        client_uuid,
        Some(transfered_file.parent_folder_uuid),
        signal,
    );
}

pub fn recieve(
    mut stream: TcpStream,
    init_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    signal: &(Mutex<u64>, Condvar),
) {
    let transfered_file = match init_transfer(&init_message[offset - 1..], &map_store) {
        Ok(f) => Arc::new(f),
        Err(e) => {
            eprintln!("init_transfer failed: {e:?}");
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };

    let mut buf = [0; 9];
    buf[0] = TransferSuccess::Ok.get_code();
    if stream.write_all(&buf).is_err() {
        return;
    }

    if let Err(e) = setup_config(&transfered_file) {
        eprintln!("failed to setup config: {e:?}");
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }

    let chunk_log: ChunkLog = Arc::new(Mutex::new(HashSet::new()));

    if run_transfer_loop(stream, &transfered_file, max_workers, chunk_log).is_err() {
        return;
    }

    finish_and_register(
        &transfered_file,
        &map_store,
        client_uuid,
        Some(transfered_file.parent_folder_uuid),
        signal,
    );
}

// =====================================================================
// Core transfer loop
// =====================================================================

fn run_transfer_loop(
    mut stream: TcpStream,
    transfered_file: &Arc<TransferedFile>,
    max_workers: usize,
    chunk_log: ChunkLog,
) -> Result<(), ()> {
    let writer_stream = stream.try_clone().map_err(|e| {
        eprintln!("failed to clone stream for writer: {e:?}");
    })?;
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
        worker_handles.push(thread::spawn(move || {
            disk_writer_worker(job_rx, tx, chunk_log, file)
        }));
    }

    let outcome = reader_loop(&mut stream, &job_tx, &tx, &chunk_log, transfered_file);

    drop(job_tx); // let workers drain remaining jobs, then exit
    drop(tx); // let the writer flush and exit once workers stop acking

    for h in worker_handles {
        if let Err(panic_payload) = h.join() {
            let msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic payload".to_string());
            eprintln!("a worker thread panicked: {msg}");
        }
    }
    let _ = writer_handle.join();

    outcome.map_err(|e| {
        eprintln!("transfer loop failed: {e:?}");
        let _ = stream.write_all(&[e.get_code()]);
    })?;

    // --- hash-check phase: reader loop already returned, socket is ours alone now ---
    let local_hash = hash_file(&transfered_file.temp_path).map_err(|e| {
        eprintln!("hashing failed: {e:?}");
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
    })?;

    let mut hash_msg = Vec::with_capacity(33);
    hash_msg.push(MSG_HASH_RESPONSE);
    hash_msg.extend_from_slice(local_hash.as_bytes());
    stream.write_all(&hash_msg).map_err(|e| {
        eprintln!("failed to send hash: {e:?}");
    })?;

    let mut verdict = [0u8; 1];
    stream.read_exact(&mut verdict).map_err(|e| {
        eprintln!("failed to read final verdict: {e:?}");
    })?;
    match verdict[0] {
        MSG_ACK_OK => Ok(()),
        MSG_ACK_FAIL => {
            eprintln!("sender reports hash mismatch");
            Err(())
        }
        other => {
            eprintln!("unexpected final verdict byte: {other}");
            Err(())
        }
    }
}

/// Reads messages until a hash-check request (`4`) arrives. Chunk data (`2`)
/// is handed to worker threads; completion-check requests (`3`) are answered
/// directly against the shared chunk log.
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

                if job_tx.send(ChunkJob { chunk_id, payload }).is_err() {
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
                return Ok(());
            }
            other => {
                eprintln!("reader: unexpected message type {other}");
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
) {
    loop {
        let job = {
            let rx = job_rx.lock().unwrap_or_else(|e| e.into_inner());
            rx.recv()
        };
        let Ok(job) = job else { break };

        let offset = job.chunk_id * (CHUNK_SIZE - OVERHEAD) as u64;
        let ack = match transfered_file.file.write_at(&job.payload, offset) {
            Ok(_) => {
                let count = {
                    let mut log = chunk_log.lock().unwrap_or_else(|e| e.into_inner());
                    log.insert(job.chunk_id);
                    log.len()
                };
                if count % 10 == 8 {
                    if let Err(e) = update_config(&transfered_file.config_path, &chunk_log) {
                        eprintln!("failed to update config: {e:?}");
                    }
                }
                build_ack(MSG_ACK_OK, job.chunk_id)
            }
            Err(e) => {
                eprintln!(
                    "failed to write chunk {} at offset {offset}: {e:?}",
                    job.chunk_id
                );
                build_ack(MSG_ACK_FAIL, job.chunk_id)
            }
        };

        if tx.send(ack).is_err() {
            break;
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

fn ensure_dirs_exist(temp_location: &Path, stor_location: &Path) -> io::Result<()> {
    if !temp_location.exists() {
        fs::create_dir_all(temp_location)?;
    }
    if !stor_location.exists() {
        fs::create_dir_all(stor_location)?;
    }
    Ok(())
}

fn temp_to_storage(temp_path: &Path) -> Option<PathBuf> {
    let relative = temp_path.strip_prefix("./temp").ok()?;
    Some(Path::new("./storage").join(relative))
}

pub fn find_temp_files(dir: &Path, results: &mut Vec<PathBuf>) {
    let entries = match read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("skipping unreadable directory {dir:?}: {e}");
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("skipping unreadable entry in {dir:?}: {e}");
                continue;
            }
        };
        let path = entry.path();
        if path.is_dir() {
            find_temp_files(&path, results);
        } else if path.extension().map_or(false, |ext| ext == "config") {
            results.push(path);
        }
    }
}

fn init_transfer(
    init_message: &[u8],
    map_store: &MapStore,
) -> Result<TransferedFile, ErrorTransfer> {
    if init_message.len() < 25 {
        return Err(ErrorTransfer::InvalidLength);
    }

    let mut uuid_bytes: [u8; 16] = [0; 16];
    uuid_bytes.copy_from_slice(&init_message[1..17]);

    // bytes 17..25: file size in chunks, plain u64 big-endian
    let file_size_chunks = u64::from_be_bytes(init_message[17..25].try_into().unwrap());

    let name_len = init_message[25] as usize;
    let name_start: usize = 26;
    let name_end = name_start
        .checked_add(name_len)
        .ok_or(ErrorTransfer::InvalidLength)?;
    let folder_uuid_end = name_end
        .checked_add(16)
        .ok_or(ErrorTransfer::InvalidLength)?;
    if init_message.len() < folder_uuid_end {
        return Err(ErrorTransfer::InvalidLength);
    }

    let file_name = String::from_utf8_lossy(&init_message[name_start..name_end]).to_string();
    let folder_uuid =
        Uuid::from_bytes_le(init_message[name_end..folder_uuid_end].try_into().unwrap());

    let path = map_store.get_path(folder_uuid).map_err(|e| {
        eprintln!("failed to find path from folder_uuid: {folder_uuid:?}, e: {e:?}");
        ErrorTransfer::NotFound
    })?;

    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);
    ensure_dirs_exist(temp_location, stor_location).map_err(|e| {
        eprintln!("failed to create required directories: {e}");
        ErrorTransfer::InternalServerError
    })?;

    let file_name = Path::new(&file_name)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ErrorTransfer::InvalidFileName)?;

    let file_path = format!("{TEMP_FOLDER_LOCATION}/{file_name}");
    let storage_file_path = path.join(file_name);
    let config_file_path = format!("{file_path}.config");

    let path = Path::new(&file_path);
    let storage_path = Path::new(&storage_file_path);
    let config_path = Path::new(&config_file_path);

    if path.exists() || storage_path.exists() || config_path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }

    let file = File::create(path).map_err(|e| {
        eprintln!("file creation failed: {e:?}");
        ErrorTransfer::InternalServerError
    })?;

    Ok(TransferedFile {
        file_size_chunks: file_size_chunks.div_ceil((CHUNK_SIZE - OVERHEAD) as u64),
        parent_folder_uuid: folder_uuid,
        file: Arc::new(file),
        temp_path: path.to_path_buf(),
        storage_path: storage_path.to_path_buf(),
        config_path: Mutex::new(config_path.to_path_buf()),
        uuid: Uuid::from_bytes_le(uuid_bytes),
    })
}

fn setup_config(transfered_file: &TransferedFile) -> io::Result<()> {
    let config_path = transfered_file
        .config_path
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let mut config_file = File::create(config_path)?;

    let config = ConfigFile {
        last_changed_at: now_nanos(),
        uuid: transfered_file.uuid,
        file_size_chunks: transfered_file.file_size_chunks,
        parent_folder_uuid: transfered_file.parent_folder_uuid,
        transfered_chunks: HashSet::new(),
        is_public: false,
        owner: Vec::new(),
    };

    let json = serde_json::to_string_pretty(&config)?;
    config_file.write_all(json.as_bytes())?;
    Ok(())
}

fn update_config(config_path: &Mutex<PathBuf>, chunk_log: &ChunkLog) -> io::Result<()> {
    let path = config_path
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
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

fn finish_and_register(
    transfered_file: &Arc<TransferedFile>,
    map_store: &MapStore,
    client_uuid: &Uuid,
    parent_folder_uuid: Option<Uuid>,
    signal: &(Mutex<u64>, Condvar),
) {
    let file_name = match transfered_file
        .storage_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
    {
        Some(name) => name,
        None => {
            eprintln!(
                "couldn't extract a valid filename from {:?}",
                transfered_file.storage_path
            );
            return;
        }
    };

    let mapped_file = Fil::new(
        file_name,
        transfered_file.storage_path.to_path_buf(),
        *client_uuid,
        true,
        true,
        Vec::new(),
        Vec::new(),
    );

    if let Err(e) = map_store.add_file(parent_folder_uuid, mapped_file) {
        eprintln!("map store couldn't add a file: {e:?}");
        return;
    }

    if let Err(e) = fs::copy(&transfered_file.temp_path, &transfered_file.storage_path) {
        eprintln!(
            "failed to copy {:?} to {:?}: {e}",
            transfered_file.temp_path, transfered_file.storage_path
        );
        return;
    }
    if let Err(e) = fs::remove_file(&transfered_file.temp_path) {
        eprintln!(
            "failed to remove temp file {:?}: {e}",
            transfered_file.temp_path
        );
        return;
    }
    let cfg_path = transfered_file
        .config_path
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    if let Err(e) = fs::remove_file(&cfg_path) {
        eprintln!("failed to remove config file {cfg_path:?}: {e}");
    }
    notify_change(signal);
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
