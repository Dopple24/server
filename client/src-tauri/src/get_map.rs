use std::{
    io::{Error, Read, Result, Write},
    net::TcpStream,
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::request_file::CHUNK_SIZE;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FolderMap {
    uuid: Uuid,
    name: String,
    last_changed_at: DateTime<Utc>,
    folders: Vec<FolderMap>,
    files: Vec<FilMap>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FilMap {
    name: String,
    last_changed_at: DateTime<Utc>,
    uuid: Uuid,
    path: PathBuf,
}

pub fn get_map(mut stream: TcpStream, username: &str, password: &str) -> Result<()> {
    stream.write_all(&first_message(9, username, password))?;
    let map_bytes = recv_framed(&mut stream)?;
    let map: FolderMap = serde_json::from_slice(&map_bytes)?;
    println!("map: {:#?}", map);
    Ok(())
}

/// Reads a length-prefixed message written by `send_framed`.
pub fn recv_framed(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn first_message(message_code: u8, username: &str, password: &str) -> [u8; CHUNK_SIZE] {
    let username_bytes = username.as_bytes();
    let password_bytes = password.as_bytes();

    if username_bytes.len() > 255 || password_bytes.len() > 255 {
        panic!()
    }

    let username_start = 2;
    let username_end = username_start + username_bytes.len();
    let password_start = username_end + 1;
    let password_end = password_start + password_bytes.len();

    let mut buf = [0u8; CHUNK_SIZE];
    buf[0] = message_code;
    buf[1] = username_bytes.len() as u8;
    buf[username_start..username_end].copy_from_slice(username_bytes);
    buf[username_end] = password_bytes.len() as u8;
    buf[password_start..password_end].copy_from_slice(password_bytes);

    buf
}
