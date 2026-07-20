use std::{fs::remove_file, io::Write, net::TcpStream};

use uuid::Uuid;

use crate::{
    file_transfer::CHUNK_SIZE,
    mapper::{Fil, MapStore, with_file_mut},
    request::RequestType::CompletionCheck,
    response::{Code, ErrorTransfer, TransferSuccess},
};

pub fn delete_file(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
) {
    let uuid = Uuid::from_bytes(first_message[offset..16 + offset].try_into().unwrap());
    println!("uuid: {:?}", uuid);

    println!("client_uuid: {:?}", client_uuid);
    let fil = {
        match with_file_mut(&uuid, &map_store, client_uuid, |fil| fil.lock()) {
            Ok(locked) => {
                if !locked {
                    let buf = [ErrorTransfer::Locked.get_code(); 1];
                    stream.write_all(&buf);
                    return;
                }
            }
            Err(e) => {
                let buf = [e.get_code(); 1];
                stream.write_all(&buf);
                return;
            }
        };

        let map_read = map_store.read().unwrap();
    };

    with_file_mut(&uuid, &map_store, client_uuid, |fil| {
        if !fil.access.can_edit(&client_uuid) {
            stream.write_all(&[ErrorTransfer::Forbidden.get_code(); 1]);
            return;
        } else {
            remove_file(fil.path.clone());
        }
    });

    match map_store.remove_file(&uuid, client_uuid) {
        Ok(_) => {
            stream.write_all(&[TransferSuccess::Ok.get_code()]);
        }
        Err(e) => {
            println!("error removing file: {:?}", e);
            stream.write_all(&[e.get_code()]);
        }
    }
}
