use crate::{
    file_transfer::{CHUNK_SIZE, recieve},
    request::RequestType,
};
use std::{io::Read, net::TcpStream, sync::mpsc::RecvTimeoutError};

pub fn handle_client(mut stream: TcpStream, max_workers: usize) {
    let mut buffer = [0u8; CHUNK_SIZE];
    println!("right there");
    let _ = stream.read(&mut buffer);
    match RequestType::get_type(buffer[0]) {
        RequestType::Init => {
            recieve(stream, buffer, max_workers);
        }
        _ => {
            stream.shutdown(std::net::Shutdown::Both);
            return;
        }
    };
}
