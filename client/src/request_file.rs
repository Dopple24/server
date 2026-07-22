use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::format;
use std::fs;
use std::fs::create_dir_all;
use std::fs::read_dir;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::io::{BufReader, Error};
use std::net::TcpStream;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};
use std::{
    thread,
    time::{self, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::reinit::PartAcc;
use crate::reinit::Parts;
use crate::reinit::first_message;
use crate::response::ErrorTransfer;
use crate::response::TransferSuccess;
use crate::response::{self, Code};

pub const CHUNK_SIZE: usize = 32768;
const OVERHEAD: usize = 11;
const MAX_STORED: usize = 20;
const TEMP_FOLDER_LOCATION: &str = "./temp";
const STORAGE_FOLDER_LOCATION: &str = "./storage";

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    last_changed_at: u64,
    file_size_chunks: usize,
    transfered_chunks: HashSet<usize>,
    owner: Vec<Uuid>,
    is_public: bool,
}

struct SendChunk {
    request_tape: u8,
    header: Vec<u8>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct TransferedFile {
    file_size_chunks: usize,
    storage_path: PathBuf,
    temp_path: PathBuf,
    config_path: Mutex<PathBuf>,
    file: Arc<File>,
}

#[derive(Clone)]
pub struct Transfer {
    pub chunks: Vec<[u8; CHUNK_SIZE]>,
    pub responses: Vec<[u8; 16]>,
    pub should_die: bool,
    pub max_workers: usize,
    pub dead_workers: usize,
    pub chunk_log: HashSet<usize>,
}

impl Transfer {
    pub fn new(max_workers: usize) -> Self {
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

pub fn request(
    mut stream: TcpStream,
    max_workers: usize,
    parts: &Arc<RwLock<Parts>>,
    username: &str,
    password: &str,
    file_uuid: &Uuid,
    path_for_the_requested_file: &str,
    filename: &str,
) -> std::io::Result<()> {
    stream.write_all(&first_message(5, file_uuid, username, password))?;

    let mut buf = [0u8; 100];
    stream.read(&mut buf)?;

    let temp_path = format!("./temp/{:?}", filename);

    match buf[0] {
        20 => {
            let mut parts_write = parts.write().unwrap();
            parts_write.acc.push(PartAcc {
                temp_path: temp_path.clone(),
                real_path: path_for_the_requested_file.to_string(),
                server_uuid: file_uuid.to_string(),
            });
            parts_write.save();
        }
        48 => {
            eprintln!("forbidden");
            return Err(Error::last_os_error());
        }
        e => {
            eprintln!("failed: {:?}", e);
            return Err(Error::last_os_error());
        }
    }

    let chunks_len = u32::from_be_bytes(buf[1..5].try_into().unwrap());
    println!("chunks len: {:?}", chunks_len);

    stream.write_all(&[20u8; 1]);

    recieve(
        stream,
        &temp_path,
        path_for_the_requested_file,
        max_workers,
        chunks_len as usize,
    );

    {
        let mut parts = parts.write().unwrap();
        if let Some(pos) = parts
            .acc
            .iter()
            .position(|item| item.server_uuid == file_uuid.to_string())
        {
            parts.acc.remove(pos);
        }
        parts.save();
    };

    Ok(())
}

pub fn reinitialize(
    mut stream: TcpStream,
    parts: &Arc<RwLock<Parts>>,
    max_workers: usize,
    username: &str,
    password: &str,
) -> Result<(), Error> {
    let (real_path, temp_path, file_uuid) = {
        let parts_read = parts.read().unwrap();
        if parts_read.acc.is_empty() {
            eprintln!("acc in parts.json is empty, therefor there is nothing to reinit");
            return Err(Error::last_os_error());
        }
        match Uuid::from_str(&parts_read.acc[0].server_uuid) {
            Ok(u) => (
                parts_read.acc[0].real_path.clone(),
                parts_read.acc[0].temp_path.clone(),
                u,
            ),
            Err(e) => {
                eprintln!("failed parsing string into uuid: {:?}", e);
                return Err(Error::last_os_error());
            }
        }
    };

    stream.write_all(&first_message(5, &file_uuid, username, password))?;

    let mut buf = [0u8; 100];
    stream.read(&mut buf)?;

    match buf[0] {
        20 => (),
        48 => {
            eprintln!("forbiden");
            return Err(Error::last_os_error());
        }
        e => {
            eprintln!("failed: {:?}", e);
            return Err(Error::last_os_error());
        }
    }

    let chunks_len = u32::from_be_bytes(buf[1..5].try_into().unwrap());
    println!("chunks len: {:?}", chunks_len);

    let mut files: Vec<PathBuf> = Vec::new();

    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);

    if !temp_location.exists() {
        create_dir_all(temp_location);
        return Err(Error::other("temp location didnt exist"));
    }

    if !stor_location.exists() {
        create_dir_all(stor_location);
        return Err(Error::other("stor location didnt exist"));
    }

    find_temp_files(temp_location, &mut files);

    let existing_path_string = format!("{temp_path}.config");
    let existing_path = Path::new(&existing_path_string);

    println!("path: {:?}", existing_path);

    let contents = fs::read_to_string(existing_path).unwrap();
    let config_file: ConfigFile = serde_json::from_str(&contents).unwrap();

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
        storage_path: Path::new(&real_path).to_path_buf(),
        config_path: Mutex::new(existing_path.to_path_buf()),
    });

    stream.write_all(&response::TransferSuccess::Ok.respond(Vec::new()).as_slice());

    // Reinitialized, initializing workers

    let transfer = Arc::new(Mutex::new(Transfer::new(max_workers)));

    {
        let mut transf = transfer.lock().unwrap();
        config_file.transfered_chunks.iter().for_each(|chunk_id| {
            transf.chunk_log.insert(*chunk_id);
        });
    }

    // WORKERS
    let handles = init_workers_reciever(max_workers, &transfer, &transfered_file);

    //READER
    println!("reader initialized");
    let tran = Arc::clone(&transfer);
    stream.set_nonblocking(true).unwrap();

    init_stream_reader(&mut stream, &tran, &transfered_file);

    println!("loop broken");
    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());

    let mut ready_buf = [21u8; 1];
    stream.write_all(&mut ready_buf).unwrap();

    println!("sent {:?}", ready_buf);

    println!("awaiting hash confirmation");

    execute_final_completion_check(&mut stream, &transfered_file);

    std::fs::copy(&transfered_file.temp_path, &transfered_file.storage_path).unwrap();
    std::fs::remove_file(&transfered_file.temp_path).unwrap();
    let cfg_path = transfered_file.config_path.lock().unwrap().clone();
    std::fs::remove_file(&cfg_path).unwrap();
    {
        let mut parts = parts.write().unwrap();
        if let Some(pos) = parts
            .acc
            .iter()
            .position(|item| item.server_uuid == file_uuid.to_string())
        {
            parts.acc.remove(pos);
        }
        parts.save();
    };
    Ok(())
}

fn find_temp_files(dir: &Path, results: &mut Vec<PathBuf>) -> std::io::Result<()> {
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

pub fn recieve(
    mut stream: TcpStream,
    temp_path: &str,
    real_path: &str,
    max_workers: usize,
    file_size_chunks: usize,
) {
    let transfer = Arc::new(Mutex::new(Transfer::new(max_workers)));

    let file = Some(init_transfer(temp_path, real_path, file_size_chunks).unwrap());
    let mut buf = [0; 32];
    buf[0] = TransferSuccess::Ok.get_code();
    for (index, byte) in TransferSuccess::Ok.get_message().into_iter().enumerate() {
        buf[index + 1] = byte
    }
    let _ = stream.write_all(&buf);
    let lock_file = Arc::new(file.unwrap());

    setup_config(&lock_file);

    // WORKERS
    let handles = init_workers_reciever(max_workers, &transfer, &lock_file);

    //READER
    println!("reader initialized");
    let tran = Arc::clone(&transfer);
    stream.set_nonblocking(true).unwrap();

    init_stream_reader(&mut stream, &tran, &lock_file);

    println!("loop broken");
    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());

    let mut ready_buf = [21u8; 1];
    stream.write_all(&mut ready_buf).unwrap();

    println!("sent {:?}", ready_buf);

    println!("awaiting hash confirmation");

    execute_final_completion_check(&mut stream, &lock_file);

    std::fs::copy(&lock_file.temp_path, &lock_file.storage_path).unwrap();
    std::fs::remove_file(&lock_file.temp_path).unwrap();
    let cfg_path = lock_file.config_path.lock().unwrap().clone();
    std::fs::remove_file(&cfg_path).unwrap();
}

fn init_transfer(
    temp_path: &str,
    real_path: &str,
    file_size_chunks: usize,
) -> Result<TransferedFile, ErrorTransfer> {
    let temp_location = Path::new(TEMP_FOLDER_LOCATION);
    let stor_location = Path::new(STORAGE_FOLDER_LOCATION);

    if !temp_location.exists() {
        create_dir_all(temp_location);
    }

    if !stor_location.exists() {
        create_dir_all(stor_location);
    }

    let config_file_path = format!("{}.config", temp_path);

    let path = Path::new(&temp_path);
    let storage_path = Path::new(&real_path);
    let config_path = Path::new(&config_file_path);

    if path.exists() || storage_path.exists() || config_path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }

    println!("path: {:?}", path);
    let file = match File::create(path) {
        Ok(val) => val,
        Err(y) => {
            eprintln!("file creation failed: {:?}", y);
            return Err(ErrorTransfer::InternalServerError);
        }
    };
    Ok(TransferedFile {
        file_size_chunks,
        file: Arc::new(file),
        temp_path: path.to_path_buf(),
        storage_path: storage_path.to_path_buf(),
        config_path: Mutex::new(config_path.to_path_buf()),
    })
}

fn setup_config(lock_file: &Arc<TransferedFile>) -> Result<(), Error> {
    let config_path = {
        let lock = lock_file.config_path.lock().unwrap();
        lock.clone()
    };
    let mut config_file = File::create(config_path)?;

    let config = ConfigFile {
        last_changed_at: time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        file_size_chunks: lock_file.file_size_chunks,
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
                    let mut lock = transf.lock().unwrap();
                    if lock.chunks.len() > 0 {
                        (Some(lock.chunks.pop().unwrap().to_vec()), false)
                    } else if lock.should_die {
                        (None, true)
                    } else {
                        (None, false)
                    }
                };
                if let Some(c) = chunk {
                    let resp = recieve_chunk(c, &fil);
                    println!("response from receive chunk: {:?}", resp);
                    {
                        let resp = match resp {
                            Ok(id) => {
                                let response =
                                    TransferSuccess::Ok.respond((id as u64).to_be_bytes().to_vec());
                                let mut arr = [0u8; 16];
                                arr[8..].copy_from_slice(&response[1..]);
                                arr[0] = response[0];
                                println!("{:?}", response);
                                let log_len = {
                                    let mut lock = transf.lock().unwrap();
                                    lock.chunk_log.insert(id);
                                    lock.chunk_log.len()
                                };
                                if log_len % 10 == 8 {
                                    let res = update_config(&fil.config_path, &transf);
                                    println!("response_: {:?}", res);
                                } else {
                                    println!("log_len: {log_len}");
                                }

                                arr
                            }
                            Err(y) => {
                                let response = y.respond(vec![0u8]);
                                let mut arr = [0u8; 16];
                                let len = response.len().min(16);
                                arr[1..len].copy_from_slice(&response[1..len]);
                                arr[0] = response[0];
                                arr
                            }
                        };
                        let mut lock = transf.lock().unwrap();
                        lock.responses.push(resp);
                    }
                } else if should_die {
                    let mut lock = transf.lock().unwrap();
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
    println!("contents: {:?}", &contents[0..20]);
    let mut id_b = [0; 8];
    for i in 0..8 {
        id_b[i] = contents[i + 1];
    }
    let chunk_id = u64::from_be_bytes(id_b);
    let mut size_b = [0; 2];
    size_b[0] = contents[9];
    size_b[1] = contents[10];
    let chunk_size = u16::from_be_bytes(size_b);
    println!("chunk_size: {}", chunk_size);
    let mut trimed: Vec<u8> = Vec::new();
    for i in 0..chunk_size {
        trimed.push(contents[(i + 11) as usize])
    }
    println!("chunk_id: {chunk_id}");
    let location = chunk_id * (CHUNK_SIZE - OVERHEAD) as u64;
    match file.file.write_at(&trimed[..], location) {
        Ok(_) => Ok(chunk_id as usize),
        Err(y) => {
            eprintln!("{y}");
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
                    println!("size: {size}");
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
                        let mut transf = tran.lock().unwrap();
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
                    println!("3 came");
                    let mut lock = tran.lock().unwrap();

                    let chunks_number = { lock_file.file_size_chunks };
                    if lock.chunk_log.len() == chunks_number {
                        lock.should_die = true;
                        let mut buf = vec![0u8; 9];
                        buf[0] = 23u8.to_be_bytes()[0];
                        println!("buf: {:?}", buf);
                        stream.write_all(&buf).unwrap();
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
                    let _ = stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
        {
            let mut lock = tran.lock().unwrap();
            while !lock.responses.is_empty() {
                let response_to_send = lock.responses.pop().unwrap();
                let res = stream.write_all(&response_to_send);
                println!("{:?}, responses in queue: {:?}", res, response_to_send);
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
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => {
                        println!("44 header: {} not found", header_buf[0]);
                        let _ = stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                    }
                };
            }
            let mut hash_buf = [0u8; 32];
            stream.read_exact(&mut hash_buf).unwrap();
            let server_hash = hash_file(lock_file.temp_path.clone()).unwrap();
            let client_hash: Hash = hash_buf.try_into().unwrap();
            if server_hash == client_hash {
                println!("hashes match");
                let res = stream.write_all(&{
                    let mut resp = vec![0u8; 18];
                    resp[0] = 24;
                    resp
                });
                println!("result: {:?}", res);
                break;
            } else {
                println!("hashes do not match");
                let _ = stream.write_all(&ErrorTransfer::HashesDoNotMatch.respond(vec![0u8; 17]));
            }
        }
    }
    println!("file transfer complete");
}

fn update_config(path: &Mutex<PathBuf>, transf: &Arc<Mutex<Transfer>>) -> Result<(), Error> {
    let path = path.lock().unwrap();

    println!("config path: {:?}", path);

    let mut file = OpenOptions::new().read(true).write(true).open(&*path)?;

    let reader = BufReader::new(&file);
    let mut config: ConfigFile = serde_json::from_reader(reader)?;

    // Convert Vec to HashSet for comparison
    let existing: HashSet<usize> = config.transfered_chunks.iter().copied().collect();

    let lock = transf.lock().unwrap();

    if existing == lock.chunk_log {
        return Ok(());
    }

    config.last_changed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Merge new chunks in
    config.transfered_chunks = existing.union(&lock.chunk_log).copied().collect();

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
    let mut file = File::open(file).unwrap();

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}
