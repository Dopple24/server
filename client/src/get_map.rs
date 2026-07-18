use std::{
    io::{Error, Read, Result, Write},
    net::TcpStream,
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

pub fn get_map(mut stream: TcpStream) -> Result<()> {
    stream.write_all(&vec![9u8])?;
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
