use crate::{
    file_transfer::{CHUNK_SIZE, recieve, reinitialize},
    get_file,
    request::RequestType,
};
use std::{io::Read, net::TcpStream};

pub fn handle_client(mut stream: TcpStream, max_workers: usize) {
    let mut buffer = [0u8; CHUNK_SIZE];
    println!("right there");
    let buf_len = stream.read(&mut buffer).unwrap();
    match RequestType::get_type(buffer[0]) {
        RequestType::Init => recieve(stream, buffer, max_workers),
        RequestType::Reinit => reinitialize(stream, buffer, max_workers),
        RequestType::GetFile => get_file::send_file(stream, buffer, max_workers, buf_len),
        RequestType::ReinitGetFile => {
            get_file::reinit_send_file(stream, buffer, max_workers, buf_len)
        }
        _ => {
            println!("shuting down");
            stream.shutdown(std::net::Shutdown::Both);
            return;
        }
    };
}
