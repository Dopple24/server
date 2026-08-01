use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Error, Read, Write},
    net::TcpStream,
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};
use tauri::{AppHandle, State};
use uuid::Uuid;

use crate::app::{
    get_chunks_len, get_file_size, request_missing_chunks, run_send_loop, CHUNK_SIZE,
    NEW_PARTS_PATH, PARTS_PATH,
};

#[derive(Deserialize, Serialize, Debug)]
pub enum PartsError {
    FailedToSave,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Parts {
    pub send: Vec<PartSend>,
    pub acc: Vec<PartAcc>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PartSend {
    pub uuid: Uuid,
    pub filename: String,
    pub path: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PartAcc {
    pub uuid: Uuid,
    pub temp_path: String,
    pub real_path: String,
    pub server_uuid: String,
}

impl Parts {
    pub fn save(&self) -> Result<(), PartsError> {
        let mut new_database_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(Path::new(NEW_PARTS_PATH))
            .map_err(|e| {
                eprintln!("error opening database file: {e:?}");
                PartsError::FailedToSave
            })?;

        let json_bytes = serde_json::to_string_pretty(self)
            .map_err(|e| {
                eprintln!("invalid json: {e:?}");
                PartsError::FailedToSave
            })?
            .into_bytes();

        new_database_file.write_all(&json_bytes).map_err(|e| {
            eprintln!("failed to write into a file: {e:?}");
            PartsError::FailedToSave
        })?;

        new_database_file.sync_all().map_err(|e| {
            eprintln!("failed to flush database file to disk: {e:?}");
            PartsError::FailedToSave
        })?;

        std::fs::rename(NEW_PARTS_PATH, PARTS_PATH).map_err(|e| {
            eprintln!("failed to replace old database with new one: {e:?}");
            PartsError::FailedToSave
        })
    }
}

/// Resumes an interrupted upload. Unlike a fresh send, the initial chunk
/// queue is seeded from a completion-check request sent right after the
/// handshake, so only chunks the server is actually missing get resent.
pub fn reinit(
    mut stream: TcpStream,
    parts: State<Arc<RwLock<Parts>>>,
    send_uuid: &str,
    username: &str,
    password: &str,
    frontend_uuid: &str,
    app: AppHandle,
) -> std::io::Result<()> {
    let send_uuid = Uuid::from_str(send_uuid).map_err(|e| {
        eprintln!("failed to get uuid: {e:?}");
        Error::last_os_error()
    })?;

    let (uuid, path) = {
        let parts_lock = parts.read().unwrap();
        let part = parts_lock
            .send
            .iter()
            .find(|s| s.uuid == send_uuid)
            .ok_or_else(Error::last_os_error)?;
        (part.uuid, part.path.clone())
    };

    let file_size = get_file_size(Path::new(&path)).map_err(|e| {
        eprintln!("failed to get file size: {e:?}");
        Error::last_os_error()
    })?;
    let chunks_len = get_chunks_len(file_size);

    let first_message = first_message(10, &uuid, username, password);
    stream.write_all(&first_message)?;

    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack)?;
    if ack[0] != 20 {
        eprintln!("reinit handshake rejected, code {}", ack[0]);
        return Err(Error::last_os_error());
    }

    let file = Arc::new(File::open(&path)?);

    // seed the queue with only what's actually missing, instead of the whole file
    let initial_chunks = request_missing_chunks(&mut stream, chunks_len)
        .map_err(|e| {
            eprintln!("completion check before reinit failed: {e:?}");
            Error::last_os_error()
        })?
        .unwrap_or_default();

    let result = run_send_loop(
        stream,
        file,
        file_size,
        chunks_len,
        initial_chunks,
        app,
        frontend_uuid.to_string(),
    );

    match result {
        Ok(()) => {
            let mut parts_write = parts.write().unwrap();
            if let Some(pos) = parts_write.send.iter().position(|item| item.uuid == uuid) {
                parts_write.send.remove(pos);
            }
            let _ = parts_write.save();
            Ok(())
        }
        Err(e) => {
            eprintln!("reinit transfer failed: {e:?}");
            // leave the parts.json entry so another reinit attempt is still possible
            Err(Error::last_os_error())
        }
    }
}

pub fn first_message(
    message_code: u8,
    uuid: &Uuid,
    username: &str,
    password: &str,
) -> [u8; CHUNK_SIZE] {
    let username_bytes = username.as_bytes();
    let password_bytes = password.as_bytes();

    assert!(
        username_bytes.len() <= 255 && password_bytes.len() <= 255,
        "username/password must each be at most 255 bytes"
    );

    let username_start = 2;
    let username_end = username_start + username_bytes.len();
    let password_start = username_end + 1;
    let password_end = password_start + password_bytes.len();
    let uuid_start = password_end;
    let uuid_end = uuid_start + 16;

    assert!(
        uuid_end <= CHUNK_SIZE,
        "first_message overflowed CHUNK_SIZE"
    );

    let mut buf = [0u8; CHUNK_SIZE];
    buf[0] = message_code;
    buf[1] = username_bytes.len() as u8;
    buf[username_start..username_end].copy_from_slice(username_bytes);
    buf[username_end] = password_bytes.len() as u8;
    buf[password_start..password_end].copy_from_slice(password_bytes);
    buf[uuid_start..uuid_end].copy_from_slice(uuid.as_bytes());

    buf
}
