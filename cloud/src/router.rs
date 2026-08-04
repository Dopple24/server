use crate::{
    auth::{self, login_api},
    delete_file,
    file_transfer::{CHUNK_SIZE, recieve, reinitialize},
    get_file, get_map, guest_request_file, manage_folder,
    map_tracker::{self, track},
    mapper::{MapStore, with_file_mut},
    request::RequestType,
    share_link::{self, LinkDatabase},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    println,
    sync::{Arc, Condvar, Mutex, RwLock},
};

pub fn handle_client(
    mut stream: TcpStream,
    max_workers: usize,
    map_store: MapStore,
    signal: &(Mutex<u64>, Condvar),
    public_links: &Arc<RwLock<LinkDatabase>>,
) {
    let mut buffer = [0u8; CHUNK_SIZE];
    let buf_len = stream.read(&mut buffer).unwrap();

    let request_type = RequestType::get_type(buffer[0]);

    if request_type == RequestType::Register {
        println!("registering");
        auth::register(stream, &buffer).expect("failed")
    } else if request_type == RequestType::GuestRequestFile {
        println!("request_file by guest");
        guest_request_file::guest_request_file(stream, buffer, public_links, map_store);
    } else {
        let (client_uuid, offset) = match login_api(&buffer) {
            Some(val) => val,
            None => {
                let buf = [48u8; 1];
                let _ = stream.write_all(&buf);
                return;
            }
        };
        match request_type {
            RequestType::Init => recieve(
                stream,
                buffer,
                max_workers,
                map_store,
                &client_uuid,
                offset,
                signal,
            ),
            RequestType::Reinit => reinitialize(
                stream,
                buffer,
                max_workers,
                map_store,
                &client_uuid,
                offset,
                signal,
            ),
            RequestType::GetFile => {
                let file_uuid = get_file::send_file(
                    stream,
                    buffer,
                    max_workers,
                    buf_len,
                    &map_store,
                    &client_uuid,
                    offset,
                    false,
                );
                if let Some(file_uuid) = file_uuid {
                    match with_file_mut(&file_uuid, &map_store, &client_uuid, |fil| fil.unlock()) {
                        Ok(fil) => fil,
                        Err(e) => {
                            eprintln!("failed to unlock: {e:?}");
                            return;
                        }
                    };
                }
            }
            RequestType::ReinitGetFile => {
                let file_uuid = get_file::send_file(
                    stream,
                    buffer,
                    max_workers,
                    buf_len,
                    &map_store,
                    &client_uuid,
                    offset,
                    true,
                );
                if let Some(file_uuid) = file_uuid {
                    match with_file_mut(&file_uuid, &map_store, &client_uuid, |fil| fil.unlock()) {
                        Ok(fil) => fil,
                        Err(e) => {
                            eprintln!("failed to unlock: {e:?}");
                            return;
                        }
                    };
                }
            }
            RequestType::GetMap => get_map::get_map(&mut stream, &map_store, &client_uuid),
            RequestType::Delete => {
                delete_file::delete_file(stream, buffer, map_store, &client_uuid, offset, signal)
            }
            RequestType::DeleteFolder => {
                manage_folder::delete_folder(
                    stream,
                    buffer,
                    map_store,
                    &client_uuid,
                    offset,
                    signal,
                );
            }
            RequestType::ShareLink => {
                share_link::share_link(
                    stream,
                    buffer,
                    map_store,
                    &client_uuid,
                    offset,
                    public_links,
                );
            }
            RequestType::LoginAttempt => {
                auth::attempt_login(stream);
            }
            RequestType::CreateFolder => {
                manage_folder::create_folder(
                    stream,
                    buffer,
                    map_store,
                    &client_uuid,
                    offset,
                    signal,
                );
            }
            RequestType::MapTracker => {
                map_tracker::track(stream, map_store, &client_uuid, signal);
            }
            _ => {
                println!("shuting down");
                let _ = stream.shutdown(std::net::Shutdown::Both);
                return;
            }
        };
    }
}
