use std::{
    eprintln,
    fs::remove_file,
    io::Write,
    net::TcpStream,
    sync::{Condvar, Mutex},
};

use uuid::Uuid;

use crate::{
    file_transfer::CHUNK_SIZE,
    map_tracker::notify_change,
    mapper::{MapStore, with_file_mut},
    response::{Code, ErrorTransfer, TransferSuccess},
};

pub fn delete_file(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    signal: &(Mutex<u64>, Condvar),
) {
    let uuid = Uuid::from_bytes(first_message[offset..16 + offset].try_into().unwrap());
    println!("uuid: {:?}", uuid);

    println!("client_uuid: {:?}", client_uuid);
    match with_file_mut(&uuid, &map_store, client_uuid, |fil| fil.lock()) {
        Ok(locked) => {
            if !locked {
                let buf = [ErrorTransfer::Locked.get_code(); 1];
                let _ = stream.write_all(&buf);
                return;
            }
        }
        Err(e) => {
            let buf = [e.get_code(); 1];
            let _ = stream.write_all(&buf);
            return;
        }
    };

    match with_file_mut(&uuid, &map_store, client_uuid, |fil| {
        if !fil.access.can_edit(&client_uuid) {
            let _ = stream.write_all(&[ErrorTransfer::Forbidden.get_code(); 1]);
            return;
        } else {
            match remove_file(fil.path.clone()) {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("delete file failed at removing the file: {:?}", e);
                    let _ = stream.write_all(&[50]);
                    return;
                }
            };
        }
    }) {
        Ok(_) => (),
        Err(e) => {
            let _ = stream.write_all(&[e.get_code()]);
            return;
        }
    };

    match map_store.remove_file(&uuid, client_uuid) {
        Ok(_) => {
            notify_change(signal);
            let _ = stream.write_all(&[TransferSuccess::Ok.get_code()]);
        }
        Err(e) => {
            eprintln!("error removing file from map: {:?}", e);
            let _ = stream.write_all(&[e.get_code()]);
        }
    }
}
