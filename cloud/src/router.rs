use uuid::Uuid;

use crate::{
    file_transfer::{CHUNK_SIZE, recieve, reinitialize},
    get_file, get_map,
    mapper::MapStore,
    request::RequestType,
};
use std::{io::Read, net::TcpStream};

pub fn handle_client(mut stream: TcpStream, max_workers: usize, map_store: MapStore) {
    let mut buffer = [0u8; CHUNK_SIZE];
    let client_uuid = Uuid::new_v4();
    println!("right there");
    let buf_len = stream.read(&mut buffer).unwrap();
    match RequestType::get_type(buffer[0]) {
        RequestType::Init => recieve(stream, buffer, max_workers, map_store),
        RequestType::Reinit => reinitialize(stream, buffer, max_workers, map_store),
        RequestType::GetFile => get_file::send_file(
            stream,
            buffer,
            max_workers,
            buf_len,
            map_store,
            &client_uuid,
        ),
        RequestType::ReinitGetFile => get_file::reinit_send_file(
            stream,
            buffer,
            max_workers,
            buf_len,
            map_store,
            &client_uuid,
        ),
        RequestType::GetMap => get_map::get_map(stream, map_store, &Uuid::new_v4()),
        _ => {
            println!("shuting down");
            stream.shutdown(std::net::Shutdown::Both);
            return;
        }
    };
}
