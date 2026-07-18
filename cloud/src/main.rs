use router::handle_client;
use std::{net::TcpListener, path::Path};

use crate::mapper::MapStore;

mod file_transfer;
mod get_file;
mod get_map;
mod mapper;
mod request;
mod response;
mod router;

fn main() -> std::io::Result<()> {
    let map_store = MapStore::load().unwrap();

    let listener = TcpListener::bind("127.0.0.1:6543")?;
    println!("Server listening on 127.0.0.1:6543");

    for stream in listener.incoming() {
        let store_clone = map_store.clone();
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr()?);

                std::thread::spawn(|| {
                    println!("was there");
                    handle_client(stream, 5, store_clone);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
