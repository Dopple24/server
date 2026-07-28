use std::{
    io::{Error, Read, Write},
    net::TcpStream,
    str::FromStr,
};

use uuid::Uuid;

use crate::{auth::DatabaseError, reinit::first_message};

pub fn delete(mut stream: TcpStream, username: &str, pass: &str, uuid: &str) -> Result<(), Error> {
    let file_uuid = match Uuid::from_str(uuid) {
        Ok(uuid) => uuid,
        Err(e) => {
            eprintln!("uuid could not be parsed: {:?}", e);
            return Err(Error::last_os_error());
        }
    };

    stream.write_all(&first_message(255, &file_uuid, username, pass));
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf);
    println!("deletion ended with code: {}", buf[0]);
    Ok(())
}
