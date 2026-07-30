use std::{
    io::{Error, Read, Write},
    net::TcpStream,
    str::FromStr,
};

use uuid::Uuid;

use crate::request_file::CHUNK_SIZE;

pub fn create_folder(
    mut stream: TcpStream,
    username: &str,
    password: &str,
    folder_uuid: &str,
    folder_name: &str,
) -> std::io::Result<()> {
    let folder_uuid = Uuid::from_str(folder_uuid).unwrap();
    stream.write_all(&first_message(
        50,
        username,
        password,
        &folder_uuid,
        folder_name,
    ));
    let mut buf = [0u8; 1];
    match stream.read_exact(&mut buf) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("error: {e:?}");
            return Err(Error::last_os_error());
        }
    };

    match buf[0] {
        20 => Ok(()),
        _ => Err(Error::last_os_error()),
    }
}

pub fn first_message(
    message_code: u8,
    username: &str,
    password: &str,
    parent_folder_uuid: &Uuid,
    folder_name: &str,
) -> [u8; CHUNK_SIZE] {
    let username_bytes = username.as_bytes();
    let password_bytes = password.as_bytes();
    let folder_name_bytes = folder_name.as_bytes();

    if username_bytes.len() > 255 || password_bytes.len() > 255 {
        panic!()
    }
    if folder_name_bytes.len() > u16::MAX as usize {
        panic!()
    }

    let username_start = 2;
    let username_end = username_start + username_bytes.len();
    let password_start = username_end + 1;
    let password_end = password_start + password_bytes.len();

    let offset = password_end;

    let parent_folder_uuid_beggining = offset;
    let parent_folder_uuid_end = parent_folder_uuid_beggining + 16;
    let folder_name_len_beggining = parent_folder_uuid_end;
    let folder_name_len_end = folder_name_len_beggining + 2;
    let folder_name_beggining = folder_name_len_end;
    let folder_name_end = folder_name_beggining + folder_name_bytes.len();

    if folder_name_end > CHUNK_SIZE {
        panic!()
    }

    let mut buf = [0u8; CHUNK_SIZE];
    buf[0] = message_code;
    buf[1] = username_bytes.len() as u8;
    buf[username_start..username_end].copy_from_slice(username_bytes);
    buf[username_end] = password_bytes.len() as u8;
    buf[password_start..password_end].copy_from_slice(password_bytes);

    buf[parent_folder_uuid_beggining..parent_folder_uuid_end]
        .copy_from_slice(parent_folder_uuid.as_bytes());
    buf[folder_name_len_beggining..folder_name_len_end]
        .copy_from_slice(&(folder_name_bytes.len() as u16).to_be_bytes());
    buf[folder_name_beggining..folder_name_end].copy_from_slice(folder_name_bytes);

    buf
}
