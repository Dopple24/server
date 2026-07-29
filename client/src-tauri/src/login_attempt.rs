use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::get_map::first_message;

pub fn login_attempt(mut stream: TcpStream, username: &str, pass: &str) -> bool {
    match stream.write_all(&first_message(34, username, pass)) {
        Ok(_) => (),
        Err(_) => return false,
    }
    let mut buf = [0u8; 1];
    match stream.read_exact(&mut buf) {
        Ok(_) => (),
        Err(_) => return false,
    }
    buf[0] == 20
}
