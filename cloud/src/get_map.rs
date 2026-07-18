use std::{io::Write, net::TcpStream, path::PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    file_transfer::CHUNK_SIZE,
    mapper::{Fil, Folder, MapStore},
};

#[derive(Serialize, Deserialize, Clone)]
pub struct FolderMap {
    uuid: Uuid,
    name: String,
    last_changed_at: DateTime<Utc>,
    folders: Vec<FolderMap>,
    files: Vec<FilMap>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FilMap {
    name: String,
    last_changed_at: DateTime<Utc>,
    uuid: Uuid,
    path: PathBuf,
}

pub fn get_map(mut stream: TcpStream, map_store: MapStore, client_uuid: Uuid) {
    let read_lock = map_store.read().unwrap();
    let new_map = to_client_map(&read_lock, client_uuid);
    let json = serde_json::to_vec(&new_map).unwrap();
    send_framed(&mut stream, &json).unwrap();
}

/// Writes `payload` as: [4-byte big-endian length][payload bytes]
pub fn send_framed(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

fn build_client_map(folder: &Folder, client_uuid: Uuid) -> FolderMap {
    let files: Vec<FilMap> = folder
        .files
        .iter()
        .filter(|f| f.access.can_view(client_uuid))
        .map(|f| FilMap {
            name: f.name.clone(),
            last_changed_at: f.last_changed_at,
            uuid: f.uuid,
            path: f.path.clone(),
        })
        .collect();

    let folders: Vec<FolderMap> = folder
        .folders
        .iter()
        .filter(|sub| sub.access.can_view(client_uuid))
        .map(|sub| build_client_map(sub, client_uuid))
        .collect();

    FolderMap {
        uuid: folder.uuid,
        name: folder.name.clone(),
        last_changed_at: folder.last_changed_at,
        folders,
        files,
    }
}

/// Public entry point. Returns None if the client can't even view the root.
pub fn to_client_map(root: &Folder, client_uuid: Uuid) -> Option<FolderMap> {
    if !root.access.can_view(client_uuid) {
        return None;
    }
    Some(build_client_map(root, client_uuid))
}
