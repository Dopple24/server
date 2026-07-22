use std::{
    io::{Error, Read, Write},
    net::TcpStream,
    println,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::request_file::CHUNK_SIZE;

pub fn share_link(
    mut stream: TcpStream,
    username: &str,
    pass: &str,
    uuid: &str,
    hours_after: &str,
) -> Result<(), std::io::Error> {
    let file_uuid = match Uuid::from_str(uuid) {
        Ok(uuid) => uuid,
        Err(e) => {
            eprintln!("uuid could not be parsed: {:?}", e);
            return Err(Error::last_os_error());
        }
    };

    let hours: u64 = match u64::from_str(hours_after) {
        Ok(h) => h,
        Err(_) => 1,
    };

    let timestamp = (SystemTime::now() + Duration::from_hours(hours))
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs() as i64;

    stream.write_all(&first_message(100, &file_uuid, username, pass, timestamp));
    let mut buf = [0u8; 17];
    stream.read_exact(&mut buf);

    if buf[0] == 20 {
        let uuid = Uuid::from_bytes(buf[1..].try_into().unwrap());
        println!(
            "uuid of the share link is: {:?}\nit now can be found at: 127.0.0.1:8080/dl/{:?}",
            uuid, uuid
        )
    }
    println!("share_link ended with code: {}", buf[0]);
    Ok(())
}

pub fn first_message(
    message_code: u8,
    uuid: &Uuid,
    username: &str,
    password: &str,
    timestamp: i64,
) -> [u8; CHUNK_SIZE] {
    let username_bytes = username.as_bytes();
    let password_bytes = password.as_bytes();

    if username_bytes.len() > 255 || password_bytes.len() > 255 {
        panic!()
    }

    let username_start = 2;
    let username_end = username_start + username_bytes.len();
    let password_start = username_end + 1;
    let password_end = password_start + password_bytes.len();
    let uuid_start = password_end;
    let uuid_end = uuid_start + 16; // UUID is always exactly 16 bytes
    let timestamp_start = uuid_end;
    let timestamp_end = timestamp_start + 8;

    if timestamp_end > CHUNK_SIZE {
        panic!()
    }

    let mut buf = [0u8; CHUNK_SIZE];
    buf[0] = message_code;
    buf[1] = username_bytes.len() as u8;
    buf[username_start..username_end].copy_from_slice(username_bytes);
    buf[username_end] = password_bytes.len() as u8;
    buf[password_start..password_end].copy_from_slice(password_bytes);
    buf[uuid_start..uuid_end].copy_from_slice(uuid.as_bytes());
    buf[timestamp_start..timestamp_end].copy_from_slice(&timestamp.to_be_bytes());

    buf
}
