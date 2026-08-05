use std::{
    eprintln,
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use uuid::Uuid;

use crate::{
    errors::is_connection_broken,
    file_transfer::{CHUNK_SIZE, OVERHEAD},
    get_file::{get_chunks_len, get_file_size},
    mapper::{MapStore, with_file_mut_unchecked},
    response::{Code, ErrorTransfer},
    share_link::LinkDatabase,
};

pub fn guest_request_file(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    public_links: &Arc<RwLock<LinkDatabase>>,
    map_store: MapStore,
) {
    let link = match {
        let links_write = public_links.write().unwrap_or_else(|e| {
            eprintln!("public_links was poisoned");
            e.into_inner()
        });
        links_write.get_link_from_token(&Uuid::from_bytes(first_message[1..17].try_into().unwrap()))
    } {
        Ok(u) => u,
        Err(_) => {
            let _ = stream.write_all(&[ErrorTransfer::InvalidRequest.get_code()]);
            return;
        }
    };
    let file_uuid = link.file_uuid;
    let file_name = link.file_name;
    let path = match get_path(&file_uuid, &map_store) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {:?}", e);
            let buf = [48u8; 1];
            let _ = stream.write_all(&buf);
            return;
        }
    };

    let file_size = match get_file_size(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to get file size: {e:?}");
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };
    let chunks_len = get_chunks_len(file_size);
    let fil = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("failed to open a file at {path:?}: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let name_bytes = file_name.as_bytes();

    let len: u16 = match u16::try_from(name_bytes.len()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("invalid length: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InvalidLength.get_code()]);
            return;
        }
    };

    let mut payload = Vec::with_capacity(2 + name_bytes.len());
    payload.extend_from_slice(&len.to_be_bytes());
    payload.extend_from_slice(name_bytes);

    match stream.write_all(&payload) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            eprintln!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let mut ready_buf = [0u8; 1];

    match stream.read_exact(&mut ready_buf) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            eprintln!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    match ready_buf[0] {
        20 => (),
        _ => {
            eprintln!("connection broken");
            return;
        }
    }

    let mut buf = [0u8; 9];
    buf[0] = 20;
    buf[1..9].copy_from_slice(&chunks_len.to_be_bytes());

    match stream.write_all(&buf) {
        Ok(_) => (),
        Err(e) if is_connection_broken(&e) => {
            eprintln!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let mut resp = [0u8; CHUNK_SIZE];

    match stream.read(&mut resp) {
        Ok(0) => {
            eprintln!("connection broken");
            return;
        }
        Ok(_) => {
            if resp[0] != 20 {
                return;
            }
        }
        Err(e) if is_connection_broken(&e) => {
            eprintln!("connection broken");
            return;
        }
        Err(e) => {
            eprintln!("unexpected read error waiting for ready signal: {e}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    }

    loop {
        let mut request_buf = [0u8; 1];
        match stream.read_exact(&mut request_buf) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("read error: {e}");
                return;
            }
        };
        match request_buf[0] {
            18 => {
                let mut chunk_id_buf = [0u8; 8];
                match stream.read_exact(&mut chunk_id_buf) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("read error: {e:?}");
                        return;
                    }
                };
                match request_chunk(&mut stream, chunk_id_buf, &fil, chunks_len as u64) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("failed to request chunk: {e:?}");
                        let _ = stream.write_all(&[e.get_code()]);
                        return;
                    }
                };
            }
            _ => {
                break;
            }
        }
    }

    match with_file_mut_unchecked(&file_uuid, &map_store, |fil| fil.unlock()) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to unlock file: {e:?}");
            return;
        }
    };
}

fn get_path(file_uuid: &Uuid, map_store: &MapStore) -> Result<PathBuf, ErrorTransfer> {
    match with_file_mut_unchecked(&file_uuid, map_store, |fil| {
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

fn request_chunk(
    stream: &mut TcpStream,
    chunk_id_bytes: [u8; 8],
    file: &File,
    file_size_chunks: u64,
) -> Result<(), ErrorTransfer> {
    let chunk_id = u64::from_be_bytes(chunk_id_bytes);

    let remaining = file_size_chunks * (CHUNK_SIZE - OVERHEAD) as u64
        - (CHUNK_SIZE - OVERHEAD) as u64 * chunk_id;
    let chunk_size = remaining.min((CHUNK_SIZE - OVERHEAD) as u64) as usize;

    let mut buf = vec![0u8; chunk_size];
    match file.read_at(&mut buf, (CHUNK_SIZE - OVERHEAD) as u64 * chunk_id) {
        Ok(_) => (),
        Err(e) => {
            eprintln!(
                "failed to read chunk {chunk_id} at offset {}: {e}",
                (CHUNK_SIZE - OVERHEAD) as u64 * chunk_id
            );
            return Err(ErrorTransfer::InternalServerError);
        }
    };

    let chunk_size: u16 = buf.len() as u16;

    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    buffer.extend_from_slice(&[2]);
    buffer.extend_from_slice(&chunk_id.to_be_bytes());
    buffer.extend_from_slice(&chunk_size.to_be_bytes());
    buffer.extend_from_slice(&buf);

    match stream.write_all(&buffer) {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("write error: {e:?}");
            Err(ErrorTransfer::Closed)
        }
    }
}
