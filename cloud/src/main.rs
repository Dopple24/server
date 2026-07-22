use router::handle_client;
use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
};

use crate::mapper::MapStore;

mod auth;
mod delete_file;
mod file_transfer;
mod get_file;
mod get_map;
mod guest_request_file;
mod mapper;
mod request;
mod response;
mod router;
mod share_link;

fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    let mut map_store = MapStore::load().unwrap();
    map_store.unlock_all().unwrap();

    let public_links = Arc::new(RwLock::new(share_link::LinkDatabase::load()));

    let listener = TcpListener::bind("127.0.0.1:6543")?;
    println!("Server listening on 127.0.0.1:6543");

    for stream in listener.incoming() {
        let store_clone = map_store.clone();
        let links_clone = public_links.clone();
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr()?);

                std::thread::spawn(move || {
                    println!("was there");
                    handle_client(stream, 5, store_clone, &links_clone);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
