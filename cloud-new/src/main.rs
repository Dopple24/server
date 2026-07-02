use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::io::{BufReader, Error};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{
    thread,
    time::{self, UNIX_EPOCH},
};
use uuid::Uuid;

const CHUNK_SIZE: usize = 32768;
const OVERHEAD: usize = 11;
const MAX_STORED: usize = 20;

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    last_changed_at: u64,
    uuid: Uuid,
    file_size_chunks: usize,
    transfered_chunks: HashSet<usize>,
    owner: Vec<Uuid>,
    is_public: bool,
}

trait Code {
    fn get_code(&self) -> u8;
    fn get_message(&self) -> Vec<u8>;
    fn respond(&self, message: Vec<u8>) -> Vec<u8>;
}

enum TransferSuccess {
    Ok,
}

impl Code for TransferSuccess {
    fn get_code(&self) -> u8 {
        match self {
            TransferSuccess::Ok => 20,
        }
    }
    fn get_message(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(15);
        let msg = match self {
            TransferSuccess::Ok => "ok".to_string(),
        };
        buffer.extend_from_slice(msg.as_bytes());
        buffer
    }
    fn respond(&self, message: Vec<u8>) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(16);
        buf.push(self.get_code());
        buf.extend_from_slice(&message);
        buf
    }
}

#[derive(Debug)]
enum ErrorTransfer {
    InvalidLength,
    InvalidUuid,
    Overflow,
    NotFound,
    NotInitialized,
    AlreadyInitialized,
    ThisFileExists,
    InternalServerError,
    TooFast,
    HashesDoNotMatch,
}

#[derive(Debug)]
enum RequestType {
    //each request should have type ([0])
    //uuid of transfer ([1..16]); file size ([16..23]); file name size [23], file name (24..)
    Init,
    //index of chunk ([1..16]); chunk size ([16..18]); chunk ([18..])
    ChunkTransfer,
    //0
    Disconnect,
    Unknown,
}

impl RequestType {
    fn get_type(code: u8) -> Self {
        match code {
            0 => Self::Disconnect,
            1 => Self::Init,
            2 => Self::ChunkTransfer,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug)]
struct Request {
    request_type: RequestType,
    contents: Vec<u8>,
}

impl Request {
    fn decipher(data: [u8; CHUNK_SIZE]) -> Result<Self, ErrorTransfer> {
        if data.len() < 7 {
            println!("this too long or sth: {:?}, {}", data, data.len());
            return Err(ErrorTransfer::InvalidLength);
        }

        let req_type = match RequestType::get_type(data[0]) {
            RequestType::Unknown => return Err(ErrorTransfer::NotFound),
            x => x,
        };
        Ok(Self {
            request_type: req_type,
            contents: data[1..].to_vec(),
        })
    }
}

struct SendChunk {
    request_tape: u8,
    header: Vec<u8>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct TransferedFile {
    uuid: Uuid,
    file_size_chunks: usize,
    storage_path: PathBuf,
    temp_path: PathBuf,
    config_path: Mutex<PathBuf>,
    file: Arc<File>,
}

impl Code for ErrorTransfer {
    fn get_code(&self) -> u8 {
        match self {
            Self::InvalidLength => 40,
            Self::InvalidUuid => 40,
            Self::Overflow => 40,
            Self::NotFound => 44,
            Self::NotInitialized => 45,
            Self::AlreadyInitialized => 46,
            Self::HashesDoNotMatch => 47,
            Self::ThisFileExists => 41,
            Self::InternalServerError => 50,
            Self::TooFast => 51,
        }
    }
    fn get_message(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(15);
        let msg: String = match self {
            Self::InvalidLength => "40 invalid length".to_string(),
            Self::InvalidUuid => "40 invalid uuid".to_string(),
            Self::Overflow => "40 request too big".to_string(),
            Self::NotFound => "44 not found".to_string(),
            Self::NotInitialized => "45 not initialized".to_string(),
            Self::AlreadyInitialized => "46 already initialized".to_string(),
            Self::HashesDoNotMatch => "47 hashes do not match".to_string(),
            Self::ThisFileExists => "41 file with this name already exists".to_string(),
            Self::InternalServerError => "50 internal server error".to_string(),
            Self::TooFast => "51 too fast".to_string(),
        };
        buffer.extend_from_slice(msg.as_bytes());
        buffer
    }
    fn respond(&self, message: Vec<u8>) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(16);
        buf.push(self.get_code());
        buf.extend_from_slice(&message);
        buf
    }
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

fn init_transfer(req: Request) -> Result<TransferedFile, ErrorTransfer> {
    let mut uuid_bytes: [u8; 16] = [0; 16];
    for i in 0..=15 {
        uuid_bytes[i] = req.contents[i];
    }
    let name_len = req.contents[23] as usize;
    let file_name = String::from_utf8_lossy(&req.contents[24..24 + name_len]).to_string();

    let file_path = format!("./temp/{}", file_name);
    let storage_file_path = format!("./storage/{}", file_name);
    let config_file_path = format!("{}.config", file_path);

    let path = Path::new(&file_path);
    let storage_path = Path::new(&storage_file_path);
    let config_path = Path::new(&config_file_path);

    if path.exists() || storage_path.exists() || config_path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }
    let file = match File::create(path) {
        Ok(val) => val,
        Err(y) => {
            println!("{:?}", y);
            return Err(ErrorTransfer::InternalServerError);
        }
    };
    println!("size bytes: {:?}", &req.contents[16..=22]);
    Ok(TransferedFile {
        file_size_chunks: match decode_size(&req.contents[16..=22]) {
            Ok(val) => val.div_ceil(CHUNK_SIZE - OVERHEAD),
            Err(err) => {
                return Err(err);
            }
        },
        file: Arc::new(file),
        temp_path: path.to_path_buf(),
        storage_path: storage_path.to_path_buf(),
        config_path: Mutex::new(config_path.to_path_buf()),
        uuid: Uuid::from_bytes_le(uuid_bytes),
    })
}

fn handle_init(req: Request) -> Result<TransferedFile, ErrorTransfer> {
    match req.request_type {
        RequestType::Init => {
            let fil = init_transfer(req);
            fil
        }
        _ => Err(ErrorTransfer::NotInitialized),
    }
}

fn handle_client(mut stream: TcpStream, max_workers: usize) {
    let mut file: Option<TransferedFile> = None;
    let transfer = Arc::new(Mutex::new(Transfer::new(max_workers)));
    loop {
        let mut buffer = [0; CHUNK_SIZE];
        let _ = stream.read(&mut buffer);
        let req = match Request::decipher(buffer) {
            Ok(x) => x,
            Err(y) => {
                let mut buf = [0; 128];
                buf[0] = y.get_code();
                for (index, byte) in y.get_message().into_iter().enumerate() {
                    buf[index + 1] = byte
                }
                let _ = stream.write_all(&buf);
                return ();
            }
        };
        match handle_init(req) {
            Ok(init) => {
                file = Some(init);
                let mut buf = [0; 32];
                buf[0] = TransferSuccess::Ok.get_code();
                for (index, byte) in TransferSuccess::Ok.get_message().into_iter().enumerate() {
                    buf[index + 1] = byte
                }
                let _ = stream.write_all(&buf);
                break;
            }
            Err(y) => {
                let mut buf = [0; 128];
                buf[0] = y.get_code();
                for (index, byte) in y.get_message().into_iter().enumerate() {
                    buf[index + 1] = byte
                }
                let _ = stream.write_all(&buf);
            }
        };
    }
    let lock_file = Arc::new(file.unwrap());
    let mut handles = Vec::new();

    {
        let config_path = {
            let lock = lock_file.config_path.lock().unwrap();
            lock.clone()
        };
        let mut config_file = File::create(config_path).unwrap();

        let config = ConfigFile {
            last_changed_at: time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            uuid: lock_file.uuid,
            file_size_chunks: lock_file.file_size_chunks,
            transfered_chunks: HashSet::new(),
            is_public: false,  //is_public is todo!()
            owner: Vec::new(), //owner is todo!()
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        config_file.write_all(json.as_bytes()).unwrap();
    }

    // WORKERS
    for i in 0..max_workers {
        println!("worker #{} initialized", i);
        let transfer_clone = Arc::clone(&transfer);
        let file_clone = Arc::clone(&lock_file);
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

    //READER
    println!("starting next loop");
    let tran = Arc::clone(&transfer);
    stream.set_nonblocking(true).unwrap();
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
                    println!("locked");
                    let chunks_number = { lock_file.file_size_chunks };
                    println!(
                        "chunks_log: {:?}, chunks_number {}",
                        lock.chunk_log, chunks_number
                    );
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
                        println!("missing: {:?}", missing);
                        println!("buf: {:?}", buf);
                        match stream.write_all(&buf) {
                            Ok(_) => println!("write ok"),
                            Err(e) => println!("write error: {e}"),
                        }
                    }
                    println!("unlocked");
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

    println!("loop broken");
    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());

    let mut ready_buf = [21u8; 1];
    stream.write_all(&mut ready_buf).unwrap();

    println!("sent {:?}", ready_buf);

    println!("awaiting hash confirmation");

    {
        loop {
            let mut header_buf = [0u8; 1];
            match stream.read_exact(&mut header_buf) {
                Ok(_) => match header_buf[0] {
                    4 => {
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
                            let _ = stream
                                .write_all(&ErrorTransfer::HashesDoNotMatch.respond(vec![0u8; 17]));
                        }
                    }
                    0 => {}
                    val => {
                        println!("44 conf header: {} not found", val);
                        let _ = stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    println!("44 header: {} not found", header_buf[0]);
                    let _ = stream.write_all(&ErrorTransfer::NotFound.respond(vec![0u8; 17]));
                }
            };
        }
    }
    println!("file transfer complete");

    std::fs::copy(&lock_file.temp_path, &lock_file.storage_path).unwrap();
    std::fs::remove_file(&lock_file.temp_path).unwrap();
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

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6543")?;
    println!("Server listening on 127.0.0.1:6543");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr()?);

                std::thread::spawn(|| {
                    handle_client(stream, 5);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
