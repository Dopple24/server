use uuid::Uuid;

use crate::{
    auth::{self, login_api},
    delete_file,
    file_transfer::{CHUNK_SIZE, recieve, reinitialize},
    get_file, get_map,
    mapper::MapStore,
    request::RequestType,
};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

pub fn handle_client(mut stream: TcpStream, max_workers: usize, map_store: MapStore) {
    let mut buffer = [0u8; CHUNK_SIZE];
    println!("right there");
    let buf_len = stream.read(&mut buffer).unwrap();

    let request_type = RequestType::get_type(buffer[0]);

    if request_type == RequestType::Register {
        println!("registering");
        auth::register(stream, &buffer).expect("failed")
    } else {
        let (client_uuid, offset) = match login_api(&buffer) {
            Some(val) => val,
            None => {
                let buf = [48u8; 1];
                stream.write_all(&buf);
                return;
            }
        };
        match request_type {
            RequestType::Init => {
                recieve(stream, buffer, max_workers, map_store, &client_uuid, offset)
            }
            RequestType::Reinit => {
                reinitialize(stream, buffer, max_workers, map_store, &client_uuid, offset)
            }
            RequestType::GetFile => get_file::send_file(
                stream,
                buffer,
                max_workers,
                buf_len,
                map_store,
                &client_uuid,
                offset,
            ),
            RequestType::ReinitGetFile => get_file::reinit_send_file(
                stream,
                buffer,
                max_workers,
                buf_len,
                map_store,
                &client_uuid,
                offset,
            ),
            RequestType::GetMap => get_map::get_map(stream, map_store, &client_uuid),
            RequestType::Delete => {
                delete_file::delete_file(stream, buffer, map_store, &client_uuid, offset)
            }
            _ => {
                println!("shuting down");
                stream.shutdown(std::net::Shutdown::Both);
                return;
            }
        };
    }
}
