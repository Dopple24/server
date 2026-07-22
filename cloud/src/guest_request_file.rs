use std::{
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::FileExt,
    path::PathBuf,
    println,
    sync::{Arc, RwLock},
};

use uuid::Uuid;

use crate::{
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
    let file_uuid = match {
        let mut links_write = public_links.write().unwrap();
        links_write.get_file_uuid(&Uuid::from_bytes(first_message[1..17].try_into().unwrap()))
    } {
        Some(u) => u,
        None => {
            stream.write_all(&[ErrorTransfer::InvalidRequest.get_code()]);
            return;
        }
    };
    let path = match get_path(&file_uuid, &map_store) {
        Ok(p) => p,
        Err(e) => {
            println!("error: {:?}", e);
            let buf = [48u8; 1];
            stream.write_all(&buf);
            return;
        }
    };

    let file_size = get_file_size(&path).unwrap();
    let chunks_len = get_chunks_len(file_size);
    let fil = File::open(&path).unwrap();

    println!("sending {:?}", chunks_len);
    let mut buf = [0u8; 5];
    buf[0] = 20;
    buf[1..5].copy_from_slice(&chunks_len.to_be_bytes());
    stream.write_all(&buf).unwrap();

    let mut resp = [0u8; CHUNK_SIZE];

    stream.read(&mut resp);

    if resp[0] != 20 {
        return;
    }

    loop {
        let mut request_buf = [0u8; 1];
        stream.read_exact(&mut request_buf);
        match request_buf[0] {
            18 => {
                let mut chunk_id_buf = [0u8; 8];
                stream.read_exact(&mut chunk_id_buf);
                request_chunk(&mut stream, chunk_id_buf, &fil, chunks_len as u64);
            }
            _ => {
                break;
            }
        }
    }

    println!("map_store before unlock: {:#?}", map_store);

    with_file_mut_unchecked(&file_uuid, &map_store, |fil| fil.unlock()).unwrap();
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
) {
    let chunk_id = u64::from_be_bytes(chunk_id_bytes);

    let remaining = file_size_chunks * (CHUNK_SIZE - OVERHEAD) as u64
        - (CHUNK_SIZE - OVERHEAD) as u64 * chunk_id;
    let chunk_size = remaining.min((CHUNK_SIZE - OVERHEAD) as u64) as usize;

    let mut buf = vec![0u8; chunk_size];
    file.read_at(&mut buf, (CHUNK_SIZE - OVERHEAD) as u64 * chunk_id)
        .unwrap();

    let chunk_size: u16 = buf.len() as u16;

    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    buffer.extend_from_slice(&[2]);
    buffer.extend_from_slice(&chunk_id.to_be_bytes());
    buffer.extend_from_slice(&chunk_size.to_be_bytes());
    buffer.extend_from_slice(&buf);

    stream.write_all(&buffer).unwrap();
}
