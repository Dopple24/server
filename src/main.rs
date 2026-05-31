use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use uuid::Uuid;

const CHUNK_SIZE: usize = 32768;
const OVERHEAD: usize = 11;
const MAX_STORED: usize = 10;

trait Code {
    fn get_code(&self) -> u8;
    fn get_message(&self) -> Vec<u8>;
    fn respond(&self) -> Vec<u8>;
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
    fn respond(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(16);
        buf.push(self.get_code());
        buf.extend_from_slice(&self.get_message());
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

#[derive(Debug)]
struct TransferedFile {
    uuid: Uuid,
    file_size: usize,
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
            Self::ThisFileExists => 41,
            Self::InternalServerError => 50,
            Self::TooFast => 51,
        }
    }
    fn get_message(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(1023);
        let msg: String = match self {
            Self::InvalidLength => "40 invalid length".to_string(),
            Self::InvalidUuid => "40 invalid uuid".to_string(),
            Self::Overflow => "40 request too big".to_string(),
            Self::NotFound => "44 not found".to_string(),
            Self::NotInitialized => "45 not initialized".to_string(),
            Self::AlreadyInitialized => "46 already initialized".to_string(),
            Self::ThisFileExists => "41 file with this name already exists".to_string(),
            Self::InternalServerError => "50 internal server error".to_string(),
            Self::TooFast => "51 too fast".to_string(),
        };
        buffer.extend_from_slice(msg.as_bytes());
        buffer
    }
    fn respond(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(16);
        buf.push(self.get_code());
        buf.extend_from_slice(&self.get_message());
        buf
    }
}

#[derive(Clone)]
struct Transfer {
    chunks: Vec<[u8; CHUNK_SIZE]>,
    responses: Vec<[u8; 1024]>,
    should_die: bool,
    max_workers: usize,
    dead_workers: usize,
}

impl Transfer {
    fn new(max_workers: usize) -> Self {
        Transfer {
            chunks: Vec::new(),
            responses: Vec::new(),
            should_die: false,
            max_workers,
            dead_workers: 0,
        }
    }
}

fn recieve_chunk(contents: Vec<u8>, file: &Arc<TransferedFile>) -> Result<usize, ErrorTransfer> {
    let mut id_b = [0; 8];
    for i in 0..8 {
        id_b[i] = contents[i + 1];
    }
    let chunk_id = u64::from_be_bytes(id_b);
    println!("chunk id: {chunk_id}, {:?}", id_b);
    let mut size_b = [0; 2];
    size_b[0] = contents[9];
    size_b[1] = contents[10];
    let chunk_size = u16::from_be_bytes(size_b);
    println!("chunk_size: {chunk_size}");
    let mut trimed: Vec<u8> = Vec::new();
    for i in 0..chunk_size {
        trimed.push(contents[(i + 11) as usize])
    }
    println!("trimmed: {:?}", trimed);
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
    println!("{:?}", uuid_bytes);
    let name_len = req.contents[23] as usize;
    let file_name = String::from_utf8_lossy(&req.contents[24..24 + name_len]).to_string();
    let file_path = format!("./storage/{}", file_name);
    let path = Path::new(&file_path);
    if path.exists() {
        return Err(ErrorTransfer::ThisFileExists);
    }
    let file = match File::create(path) {
        Ok(val) => val,
        Err(y) => {
            println!("{:?}", y);
            return Err(ErrorTransfer::InternalServerError);
        }
    };
    Ok(TransferedFile {
        file_size: match decode_size(&req.contents[16..=22]) {
            Ok(val) => val,
            Err(err) => {
                return Err(err);
            }
        },
        file: Arc::new(file),
        uuid: Uuid::from_bytes_le(uuid_bytes),
    })
}

fn disconnect() -> ErrorTransfer {
    todo!();
}

fn handle_request(
    data: [u8; CHUNK_SIZE],
    file: Arc<TransferedFile>,
) -> Result<TransferSuccess, ErrorTransfer> {
    let req = match Request::decipher(data) {
        Ok(x) => x,
        Err(y) => {
            eprintln!("{:?}", y);
            return Err(ErrorTransfer::NotFound);
        }
    };
    println!("request being handled{:?}", req);
    match req.request_type {
        RequestType::Disconnect => {
            panic!();
            //Err(disconnect());
        }
        RequestType::Init => Err(ErrorTransfer::AlreadyInitialized),
        RequestType::ChunkTransfer => match recieve_chunk(req.contents, &file) {
            Ok(_val) => Ok(TransferSuccess::Ok),
            Err(y) => Err(y),
        },
        RequestType::Unknown => Err(ErrorTransfer::NotFound),
    }
}

fn handle_init(req: Request) -> Result<TransferedFile, ErrorTransfer> {
    match req.request_type {
        RequestType::Init => {
            let fil = init_transfer(req);
            println!("{:?}", fil);
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
                        println!("{} took a chunk", i);
                        (Some(lock.chunks.pop().unwrap().to_vec()), false)
                    } else if lock.should_die {
                        (None, true)
                    } else {
                        (None, false)
                    }
                };
                if let Some(c) = chunk {
                    let resp = recieve_chunk(c, &fil);
                    {
                        let mut lock = transf.lock().unwrap();
                        lock.responses.push(match resp {
                            Ok(_id) => {
                                let response = TransferSuccess::Ok.respond();
                                let mut arr = [0u8; 1024];
                                let len = response.len().min(1024);
                                arr[..len].copy_from_slice(&response[..len]);
                                arr
                            }
                            Err(y) => {
                                let response = y.respond();
                                let mut arr = [0u8; 1024];
                                let len = response.len().min(1024);
                                arr[..len].copy_from_slice(&response[..len]);
                                arr
                            }
                        });
                    }
                } else if should_die {
                    let mut lock = transf.lock().unwrap();
                    lock.dead_workers += 1;
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
    loop {
        println!("loop");
        let mut buffer = [0u8; CHUNK_SIZE];
        stream.read(&mut buffer).unwrap();
        if buffer[0] == 2 {
            let transfered = {
                let mut transf = tran.lock().unwrap();
                if transf.chunks.len() < MAX_STORED {
                    transf.chunks.push(buffer);
                    true
                } else {
                    false
                }
            };
            if !transfered {
                let _ = stream.write_all(&ErrorTransfer::TooFast.respond());
            }
        } else if buffer[0] == 3 {
            let mut lock = tran.lock().unwrap();
            lock.should_die = true;
        }
        {
            let mut lock = tran.lock().unwrap();
            while !lock.responses.is_empty() {
                println!("writing");
                let _ = stream.write_all(&lock.responses.pop().unwrap());
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
}

fn decode_size(bytes: &[u8]) -> Result<usize, ErrorTransfer> {
    if bytes.len() != 7 {
        eprintln!("{:?}", bytes.len());
        return Err(ErrorTransfer::InvalidLength);
    }

    let mut value = 0usize;

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

    Ok(value)
}

fn send_feedback(feedback: Result<TransferSuccess, ErrorTransfer>) -> [u8; 128] {
    match feedback {
        Ok(val) => {
            let mut buf = [0; 128];
            buf[0] = val.get_code();
            for (index, byte) in val.get_message().into_iter().enumerate() {
                buf[index + 1] = byte
            }
            buf
        }
        Err(y) => {
            let mut buf = [0; 128];
            buf[0] = y.get_code();
            for (index, byte) in y.get_message().into_iter().enumerate() {
                buf[index + 1] = byte
            }
            buf
        }
    }
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
