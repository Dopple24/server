use std::{fs::remove_file, io::Write, net::TcpStream};

use uuid::Uuid;

use crate::{
    file_transfer::CHUNK_SIZE,
    mapper::{Fil, MapStore},
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
    let fil = match Fil::find_mut(&uuid, &map_store, client_uuid) {
        Ok(mut fil) => {
            if !fil.lock() {
                let buf = [ErrorTransfer::Locked.get_code(); 1];
                stream.write_all(&buf);
                return;
            } else {
                fil
            }
        }
        Err(e) => {
            let buf = [e.get_code(); 1];
            stream.write_all(&buf);
            return;
        }
    };

    if !fil.access.can_edit(&client_uuid) {
        println!("accessControl: {:?}", fil.access);
        println!("client uuid: {:?}", client_uuid);
        stream.write_all(&[ErrorTransfer::Forbidden.get_code(); 1]);
        return;
    }

    remove_file(fil.path);

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
