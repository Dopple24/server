use crate::errors::is_connection_broken;
use crate::mapper::Fil;
use crate::mapper::MapStore;
use crate::response;
use crate::response::{Code, ErrorTransfer, TransferSuccess};
use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::eprintln;
use std::fs;
use std::fs::read_dir;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::io::{BufReader, Error};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::println;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};
use std::{
    thread,
    time::{self, UNIX_EPOCH},
};
use uuid::Uuid;

pub const CHUNK_SIZE: usize = 32768;
pub const OVERHEAD: usize = 11;
pub const MAX_STORED: usize = 20;
const TEMP_FOLDER_LOCATION: &str = "./temp";
const STORAGE_FOLDER_LOCATION: &str = "./storage";

#[derive(Serialize, Deserialize, Debug)]
struct ConfigFile {
    last_changed_at: u64,
    uuid: Uuid,
    parent_folder_uuid: Uuid,
    file_size_chunks: usize,
    transfered_chunks: HashSet<usize>,
    owner: Vec<Uuid>,
    is_public: bool,
}

#[derive(Debug)]
struct TransferedFile {
    uuid: Uuid,
    file_size_chunks: usize,
    parent_folder_uuid: Uuid,
    storage_path: PathBuf,
    temp_path: PathBuf,
    config_path: Mutex<PathBuf>,
    file: Arc<File>,
}

#[derive(Clone)]
struct Transfer {
    chunks: Vec<[u8; CHUNK_SIZE]>,
    responses: Vec<[u8; 16]>,
    should_die: bool,
    max_workers: usize,
    dead_workers: usize,
    chunk_log: HashSet<usize>,
}

impl Transfer {
    fn new(max_workers: usize) -> Self {
        Transfer {
            chunk_log: HashSet::new(),
            chunks: Vec::new(),
            responses: Vec::new(),
            should_die: false,
            max_workers,
            dead_workers: 0,
        }
    }
}

pub fn reinitialize(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
) {
    println!("reinitialization called");

    let uuid = Uuid::from_bytes(first_message[offset..16 + offset].try_into().unwrap());

    let mut files: Vec<PathBuf> = Vec::new();

    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);

    std::fs::create_dir_all(&temp_location)
        .map_err(|e| eprintln!("Failed to create required directory {temp_location:?}: {e}"))
        .expect("temp_location folder creation failed");

    std::fs::create_dir_all(&stor_location)
        .map_err(|e| eprintln!("Failed to create required directory {temp_location:?}: {e}"))
        .expect("storage_location folder creation failed");

    find_temp_files(temp_location, &mut files);

    let file = files.iter().find(|path| {
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
    });

    let existing_path = match file {
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

    let file_name = existing_path.file_stem().unwrap();

    let temp_at = temp_location.join(file_name);

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(Path::new(&temp_at))
        .unwrap();

    let transfered_file = Arc::new(TransferedFile {
        file_size_chunks: config_file.file_size_chunks,
        file: Arc::new(file),
        temp_path: temp_at.to_path_buf(),
        parent_folder_uuid: config_file.parent_folder_uuid,
        storage_path: temp_to_storage(temp_at.as_path()).expect("{temp_at:?} not in ./temp"),
        config_path: Mutex::new(existing_path.to_path_buf()),
        uuid,
    });

    match stream.write_all(response::TransferSuccess::Ok.respond(Vec::new()).as_slice()) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("stream write failed: {e:?}\n ending transfer");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    // Reinitialized, initializing workers

    let transfer = Arc::new(Mutex::new(Transfer::new(max_workers)));

    {
        let mut transf = match transfer.lock() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("transfer lock failed: {e:?}");
                e.into_inner()
            }
        };
        config_file.transfered_chunks.iter().for_each(|chunk_id| {
            transf.chunk_log.insert(*chunk_id);
        });
    }

    // WORKERS
    let handles = init_workers_reciever(max_workers, &transfer, &transfered_file);

    //READER
    println!("reader initialized");
    let tran = Arc::clone(&transfer);

    match stream.set_nonblocking(true) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to set nonblocking: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    init_stream_reader(&mut stream, &tran, &transfered_file);

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

    let mut ready_buf = [21u8; 1];
    match stream.write_all(&mut ready_buf) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("ready message failed to be sent: {e:?}");
            return;
        }
    };

    execute_final_completion_check(&mut stream, &transfered_file);

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

    match map_store.add_file(None, mapped_file) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("map store couldn't add a file: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InvalidRequest.get_code()]);
            return;
        }
    };

    if let Err(e) = std::fs::copy(&transfered_file.temp_path, &transfered_file.storage_path) {
        eprintln!(
            "Failed to copy file from {:?} to {:?}: {}",
            transfered_file.temp_path, transfered_file.storage_path, e
        );
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
    if let Err(e) = std::fs::remove_file(&transfered_file.temp_path) {
        eprintln!(
            "Failed to remove temp file {:?}: {}.",
            transfered_file.temp_path, e
        );
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
    let cfg_path = match transfered_file.config_path.lock() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("config_path poisoned: {e:?}");
            e.into_inner()
        }
    }
    .clone();
    if let Err(e) = std::fs::remove_file(&cfg_path) {
        eprintln!("Failed to remove config file {:?}: {}.", cfg_path, e);
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
}

fn temp_to_storage(temp_path: &Path) -> Option<PathBuf> {
    let relative = temp_path.strip_prefix("./temp").ok()?;
    Some(Path::new("./storage").join(relative))
}

fn find_temp_files(dir: &Path, results: &mut Vec<PathBuf>) {
    let entries = match read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("skipping unreadable directory {dir:?}: {e}");
            return; // don't propagate — just skip this whole subdir
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("skipping unreadable entry in {dir:?}: {e}");
                continue; // skip just this one entry, keep going
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

pub fn recieve(
    mut stream: TcpStream,
    init_message: [u8; CHUNK_SIZE],
    max_workers: usize,
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
) {
    let transfer = Arc::new(Mutex::new(Transfer::new(max_workers)));

    let file = match init_transfer(&init_message[offset - 1..], &map_store) {
        Ok(f) => Some(f),
        Err(e) => {
            eprintln!("init_transfer failed: {e:?}");
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };
    let mut buf = [0; 32];
    buf[0] = TransferSuccess::Ok.get_code();
    for (index, byte) in TransferSuccess::Ok.get_message().into_iter().enumerate() {
        buf[index + 1] = byte
    }
    let _ = stream.write_all(&buf);
    let lock_file = Arc::new(file.unwrap());

    match setup_config(&lock_file) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to setup config: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    // WORKERS
    let handles = init_workers_reciever(max_workers, &transfer, &lock_file);

    //READER
    let tran = Arc::clone(&transfer);

    match stream.set_nonblocking(true) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to set nonblocking: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    init_stream_reader(&mut stream, &tran, &lock_file);

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

    let mut ready_buf = [21u8; 1];
    match stream.write_all(&mut ready_buf) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("ready message failed to be sent: {e:?}");
            return;
        }
    };

    execute_final_completion_check(&mut stream, &lock_file);

    let file_name = match lock_file
        .storage_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
    {
        Some(name) => name,
        None => {
            eprintln!(
                "couldn't extract a valid filename from {:?}",
                lock_file.storage_path
            );
            return;
        }
    };

    let mapped_file = Fil::new(
        file_name,
        lock_file.storage_path.to_path_buf(),
        *client_uuid,
        true,
        true,
        Vec::new(),
        Vec::new(),
    );

    match map_store.add_file(Some(lock_file.parent_folder_uuid), mapped_file) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("map store couldn't add a file: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InvalidRequest.get_code()]);
            return;
        }
    };

    if let Err(e) = std::fs::copy(&lock_file.temp_path, &lock_file.storage_path) {
        eprintln!(
            "Failed to copy file from {:?} to {:?}: {}",
            lock_file.temp_path, lock_file.storage_path, e
        );
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
    if let Err(e) = std::fs::remove_file(&lock_file.temp_path) {
        eprintln!(
            "Failed to remove temp file {:?}: {}.",
            lock_file.temp_path, e
        );
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
    let cfg_path = match lock_file.config_path.lock() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("config_path poisoned: {e:?}");
            e.into_inner()
        }
    }
    .clone();
    if let Err(e) = std::fs::remove_file(&cfg_path) {
        eprintln!("Failed to remove config file {:?}: {}.", cfg_path, e);
        let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
        return;
    }
}

fn init_transfer(
    init_message: &[u8],
    map_store: &MapStore,
) -> Result<TransferedFile, ErrorTransfer> {
    let mut uuid_bytes: [u8; 16] = [0; 16];
    for i in 0..=15 {
        uuid_bytes[i] = init_message[i + 1];
    }

    let name_len = init_message[24] as usize;
    let name_end = 25 + name_len;
    let folder_uuid_start = name_end;
    let folder_uuid_end = folder_uuid_start + 16;

    let file_name = String::from_utf8_lossy(&init_message[25..name_end]).to_string();
    let folder_uuid = Uuid::from_bytes_le(
        init_message[folder_uuid_start..folder_uuid_end]
            .try_into()
            .unwrap(),
    );

    let path = match map_store.get_path(folder_uuid) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to find path from folder_uuid: {folder_uuid:?}, e: {e:?}");
            return Err(ErrorTransfer::NotFound);
        }
    };

    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);

    std::fs::create_dir_all(&temp_location)
        .map_err(|e| eprintln!("Failed to create required directory {temp_location:?}: {e}"))
        .expect("temp_location folder creation failed");

    std::fs::create_dir_all(&stor_location)
        .map_err(|e| eprintln!("Failed to create required directory {temp_location:?}: {e}"))
        .expect("storage_location folder creation failed");

    let file_name = match Path::new(&file_name)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some(f) => f,
        None => {
            eprintln!("failed to get file_name from path");
            return Err(ErrorTransfer::InvalidFileName);
        }
    };

    let file_path = format!("{TEMP_FOLDER_LOCATION}/{}", file_name);
    let storage_file_path = path.join(&file_name);
    let config_file_path = format!("{}.config", file_path);

    let path = Path::new(&file_path);
    let storage_path = Path::new(&storage_file_path);
    let config_path = Path::new(&config_file_path);

    if path.exists() || storage_path.exists() || config_path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }

    println!("path: {path:?}");
    println!("path: {storage_path:?}");
    println!("path: {config_path:?}");

    let file = match File::create(path) {
        Ok(val) => val,
        Err(y) => {
            println!("{:?}", y);
            return Err(ErrorTransfer::InternalServerError);
        }
    };
    println!("size bytes: {:?}", &init_message[17..=23]);
    Ok(TransferedFile {
        file_size_chunks: match decode_size(&init_message[17..=23]) {
            Ok(val) => val.div_ceil(CHUNK_SIZE - OVERHEAD),
            Err(err) => {
                return Err(err);
            }
        },
        parent_folder_uuid: folder_uuid,
        file: Arc::new(file),
        temp_path: path.to_path_buf(),
        storage_path: storage_path.to_path_buf(),
        config_path: Mutex::new(config_path.to_path_buf()),
        uuid: Uuid::from_bytes_le(uuid_bytes),
    })
}

fn setup_config(lock_file: &Arc<TransferedFile>) -> Result<(), Error> {
    let config_path = {
        let lock = lock_file.config_path.lock().unwrap_or_else(|e| {
            eprintln!("config path is poisones: {e:?}");
            e.into_inner()
        });
        lock.clone()
    };
    let mut config_file = File::create(config_path)?;

    let config = ConfigFile {
        last_changed_at: time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        uuid: lock_file.uuid,
        file_size_chunks: lock_file.file_size_chunks,
        parent_folder_uuid: lock_file.parent_folder_uuid,
        transfered_chunks: HashSet::new(),
        is_public: false,  //is_public is todo!()
        owner: Vec::new(), //owner is todo!()
    };

    let json = serde_json::to_string_pretty(&config)?;
    config_file.write_all(json.as_bytes())?;
    Ok(())
}

fn init_workers_reciever(
    max_workers: usize,
    transfer: &Arc<Mutex<Transfer>>,
    lock_file: &Arc<TransferedFile>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for i in 0..max_workers {
        println!("worker #{} initialized", i);
        let transfer_clone = Arc::clone(transfer);
        let file_clone = Arc::clone(lock_file);
        handles.push(thread::spawn(move || {
            let transf = transfer_clone;
            let fil = file_clone;
            loop {
                let (chunk, should_die): (Option<Vec<u8>>, bool) = {
                    let mut lock = transf.lock().unwrap_or_else(|e| {
                        eprintln!("transf is poisoned: {e:?}");
                        e.into_inner()
                    });
                    if lock.chunks.len() > 0 {
                        (Some(lock.chunks.pop().unwrap().to_vec()), false)
                    } else if lock.should_die {
                        (None, true)
                    } else {
                        (None, false)
                    }
                };
                if let Some(c) = chunk {
                    {
                        let resp = match recieve_chunk(c, &fil) {
                            Ok(id) => {
                                let response =
                                    TransferSuccess::Ok.respond((id as u64).to_be_bytes().to_vec());
                                let mut arr = [0u8; 16];
                                arr[8..].copy_from_slice(&response[1..]);
                                arr[0] = response[0];
                                let log_len = {
                                    let mut lock = transf.lock().unwrap_or_else(|e| {
                                        eprintln!("transf was poisoned: {e:?}");
                                        e.into_inner()
                                    });
                                    lock.chunk_log.insert(id);
                                    lock.chunk_log.len()
                                };
                                if log_len % 10 == 8 {
                                    if let Err(e) = update_config(&fil.config_path, &transf) {
                                        eprintln!("failed to update config: {e:?}");
                                    };
                                }

                                arr
                            }
                            Err(e) => {
                                let mut arr = [0u8; 16];
                                arr[0] = e.get_code();
                                arr
                            }
                        };
                        let mut lock = transf.lock().unwrap_or_else(|e| {
                            eprintln!("transf was poisoned: {e:?}");
                            e.into_inner()
                        });
                        lock.responses.push(resp);
                    }
                } else if should_die {
                    let mut lock = transf.lock().unwrap_or_else(|e| {
                        eprintln!("transf was poisoned: {e:?}");
                        e.into_inner()
                    });
                    lock.dead_workers += 1;
                    println!("{i} died");
                    break;
                } else {
                    thread::sleep(time::Duration::from_millis(10));
                }
            }
        }));
    }
    handles
}

fn recieve_chunk(contents: Vec<u8>, file: &Arc<TransferedFile>) -> Result<usize, ErrorTransfer> {
    let mut id_b = [0; 8];
    for i in 0..8 {
        id_b[i] = contents[i + 1];
    }
    let chunk_id = u64::from_be_bytes(id_b);
    let mut size_b = [0; 2];
    size_b[0] = contents[9];
    size_b[1] = contents[10];
    let chunk_size = u16::from_be_bytes(size_b);
    let mut trimed: Vec<u8> = Vec::new();
    for i in 0..chunk_size {
        trimed.push(contents[(i + 11) as usize])
    }
    let location = chunk_id * (CHUNK_SIZE - OVERHEAD) as u64;
    match file.file.write_at(&trimed[..], location) {
        Ok(_) => Ok(chunk_id as usize),
        Err(y) => {
            eprintln!("failed to write_at at location {location}:{y}");
            Err(ErrorTransfer::InternalServerError)
        }
    }
}

fn init_stream_reader(
    stream: &mut TcpStream,
    tran: &Arc<Mutex<Transfer>>,
    lock_file: &Arc<TransferedFile>,
) {
    loop {
        let mut header = [0u8; 1];
        match stream.read_exact(&mut header) {
            Ok(_) => {
                if header[0] == 2 {
                    let mut header_buf = [0u8; 10];
                    match stream.read_exact(&mut header_buf) {
                        Ok(_) => {}
                        Err(y) => {
                            eprintln!("{:?}", y);
                            continue;
                        }
                    };
                    let size = u16::from_be_bytes(header_buf[8..10].try_into().unwrap());
                    let mut body_buf = vec![0u8; size as usize];

                    match stream.read_exact(&mut body_buf) {
                        Ok(_) => {}
                        Err(y) => {
                            eprintln!("{:?}", y);
                            continue;
                        }
                    };
                    let mut reconstructed = [0u8; CHUNK_SIZE];
                    reconstructed[0] = 2;
                    reconstructed[1..11].copy_from_slice(&mut header_buf);
                    reconstructed[11..11 + size as usize].copy_from_slice(&mut body_buf);
                    let transfered = {
                        let mut transf = tran.lock().unwrap_or_else(|e| {
                            eprintln!("transfer poisoned {e:?}");
                            e.into_inner()
                        });
                        if transf.chunks.len() < MAX_STORED {
                            transf.chunks.push(reconstructed);
                            true
                        } else {
                            false
                        }
                    };
                    if !transfered {
                        let _ = stream.write_all(&ErrorTransfer::TooFast.respond(vec![0u8; 15]));
                    }
                } else if header[0] == 3 {
                    let mut lock = tran.lock().unwrap_or_else(|e| {
                        eprintln!("transfer poisoned {e:?}");
                        e.into_inner()
                    });

                    let chunks_number = { lock_file.file_size_chunks };
                    if lock.chunk_log.len() == chunks_number {
                        lock.should_die = true;
                        let mut buf = vec![0u8; 9];
                        buf[0] = 23u8.to_be_bytes()[0];
                        match stream.write_all(&buf) {
                            Ok(_) => (),
                            Err(e) => {
                                eprintln!("stream write failed: {e:?}");
                                return;
                            }
                        };
                    } else {
                        let present: HashSet<usize> = lock.chunk_log.clone();
                        let missing: Vec<usize> = (0..chunks_number)
                            .filter(|x| !present.contains(x))
                            .collect();
                        let mut buf = Vec::new();
                        buf.extend_from_slice(&mut vec![23u8]);
                        let size_bytes = (missing.len() as u64).to_be_bytes();
                        buf.extend_from_slice(&size_bytes);
                        missing.iter().for_each(|miss| {
                            buf.extend_from_slice(&(*miss as u64).to_be_bytes());
                        });
                        match stream.write_all(&buf) {
                            Ok(_) => println!("write ok"),
                            Err(e) => println!("write error: {e}"),
                        }
                    }
                } else {
                    println!("44 header: {} not found", header[0]);
                    let _ = stream.write_all(&[ErrorTransfer::NotFound.get_code()]);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
        {
            let mut lock = tran.lock().unwrap_or_else(|e| {
                eprintln!("transfer poisoned {e:?}");
                e.into_inner()
            });
            while !lock.responses.is_empty() {
                let response_to_send = lock.responses.pop().unwrap();
                match stream.write_all(&response_to_send) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("stream write failed: {e:?}");
                        return;
                    }
                };
                thread::sleep(Duration::from_millis(5));
            }
            if lock.dead_workers == lock.max_workers {
                break;
            }
        };
    }
}

fn execute_final_completion_check(stream: &mut TcpStream, lock_file: &Arc<TransferedFile>) {
    {
        loop {
            loop {
                let mut header_buf = [0u8; 1];
                match stream.read_exact(&mut header_buf) {
                    Ok(_) => match header_buf[0] {
                        4 => {
                            break;
                        }
                        0 => {}
                        val => {
                            println!("44 conf header: {} not found", val);
                            let _ =
                                stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                        }
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) if is_connection_broken(&e) => {
                        eprintln!("connection broken: {e}");
                        return;
                    }
                    Err(e) => {
                        println!("header read failed: {e}");
                        let _ = stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                    }
                };
            }
            let mut hash_buf = [0u8; 32];
            stream.read_exact(&mut hash_buf).unwrap_or_else(|e| {
                eprintln!("stream failed to read: {e:?}");
                return;
            });
            let server_hash = match hash_file(lock_file.temp_path.clone()) {
                Ok(hash) => hash,
                Err(e) => {
                    eprintln!("hashing failed: {e:?}");
                    let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
                    return;
                }
            };
            let client_hash: Hash = match hash_buf.try_into() {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("invalid hash: {e:?}");
                    let _ = stream.write_all(&[ErrorTransfer::InvalidHash.get_code()]);
                    return;
                }
            };
            if server_hash == client_hash {
                let _ = stream.write_all(&[24]);
                break;
            } else {
                eprintln!("hashes do not match");
                let _ = stream.write_all(&ErrorTransfer::HashesDoNotMatch.respond(vec![0u8; 17]));
                return;
            }
        }
    }
    println!("file transfer complete");
}

fn decode_size(bytes: &[u8]) -> Result<usize, ErrorTransfer> {
    if bytes.len() != 7 {
        eprintln!("{:?}", bytes.len());
        return Err(ErrorTransfer::InvalidLength);
    }

    let mut value = 0usize;
    println!("{:?}", bytes);

    for (i, &b) in bytes.iter().enumerate() {
        let shift = 7 * i;

        // Prevent shifting beyond usize capacity
        if shift >= usize::BITS as usize {
            return Err(ErrorTransfer::Overflow);
        }

        let part = ((b & 0x7F) as usize)
            .checked_shl(shift as u32)
            .ok_or(ErrorTransfer::Overflow)?;

        value = value.checked_add(part).ok_or(ErrorTransfer::Overflow)?;
    }
    println!("{value}");

    Ok(value)
}

fn update_config(path: &Mutex<PathBuf>, transf: &Arc<Mutex<Transfer>>) -> Result<(), Error> {
    let path = path.lock().unwrap_or_else(|e| {
        eprintln!("path poisoned: {e:?}");
        e.into_inner()
    });

    let mut file = OpenOptions::new().read(true).write(true).open(&*path)?;

    let reader = BufReader::new(&file);
    let mut config: ConfigFile = serde_json::from_reader(reader)?;

    // Convert Vec to HashSet for comparison
    let existing: HashSet<usize> = config.transfered_chunks.iter().copied().collect();

    config.last_changed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    {
        let lock = transf.lock().unwrap_or_else(|e| {
            eprintln!("transf poisoned: {e:?}");
            e.into_inner()
        });

        if existing == lock.chunk_log {
            return Ok(());
        }

        // Merge new chunks in
        config.transfered_chunks = existing.union(&lock.chunk_log).copied().collect();
    }

    // Overwrite from the start, truncate leftover bytes
    let json = serde_json::to_string_pretty(&config)?;
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    file.write_all(json.as_bytes())?;

    Ok(())
}

fn hash_file(file: PathBuf) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 65536];
    let mut file = File::open(file)?;

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}
