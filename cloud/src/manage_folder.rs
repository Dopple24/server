use std::{
    io::Write,
    net::TcpStream,
    sync::{Condvar, Mutex},
};

use uuid::Uuid;

use crate::{
    file_transfer::CHUNK_SIZE,
    map_tracker::notify_change,
    mapper::{AccessControl, MapStore},
    response::{Code, ErrorTransfer, TransferSuccess},
};

pub fn create_folder(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    signal: &(Mutex<u64>, Condvar),
) {
    let parent_folder_uuid_beggining = offset;
    let parent_folder_uuid_end = offset + 16;
    let folder_name_len_beggining = parent_folder_uuid_end;
    let folder_name_len_end = folder_name_len_beggining + 2;

    let folder_name_len = u16::from_be_bytes(
        first_message[folder_name_len_beggining..folder_name_len_end]
            .try_into()
            .unwrap(),
    );

    let folder_name_beggining = folder_name_len_end;
    let folder_name_end = folder_name_beggining + folder_name_len as usize;

    let parent_folder_uuid = Uuid::from_bytes(
        first_message[parent_folder_uuid_beggining..parent_folder_uuid_end]
            .try_into()
            .unwrap(),
    );

    let folder_name =
        match String::from_utf8(first_message[folder_name_beggining..folder_name_end].to_vec()) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("invalid folder name: {e:?}");
                let _ = stream.write_all(&[ErrorTransfer::InvalidFileName.get_code()]);
                return;
            }
        };

    let access = AccessControl {
        owner: *client_uuid,
        is_public_for_viewing: true,
        is_public_for_changing: true,
        is_visible_for: Vec::new(),
        is_editable_for: Vec::new(),
    };

    match map_store.create_folder(
        Some(parent_folder_uuid),
        &folder_name,
        &*client_uuid,
        access,
    ) {
        Ok(_) => {
            notify_change(signal);
            let _ = stream.write_all(&[TransferSuccess::Ok.get_code()]);
        }
        Err(e) => {
            let _ = stream.write_all(&[ErrorTransfer::from(e).get_code()]);
        }
    };
}

pub fn delete_folder(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    signal: &(Mutex<u64>, Condvar),
) {
    println!("delete folder called by client: {client_uuid:?}");
    let folder_uuid_beggining = offset;
    let folder_uuid_end = offset + 16;
    let folder_uuid = Uuid::from_bytes(
        first_message[folder_uuid_beggining..folder_uuid_end]
            .try_into()
            .unwrap(),
    );
    match map_store.delete_folder(folder_uuid, client_uuid) {
        Ok(_) => {
            signal.1.notify_all();
            let _ = stream.write_all(&[TransferSuccess::Ok.get_code()]);
        }
        Err(e) => {
            let _ = stream.write_all(&[ErrorTransfer::from(e).get_code()]);
        }
    };
}
