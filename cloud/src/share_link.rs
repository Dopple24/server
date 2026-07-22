use std::{
    io::Write,
    net::TcpStream,
    path::Path,
    println,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PUBLIC_LINKS_PATH: &str = "./public_links.json";
const TEMP_PUBLIC_LINKS_PATH: &str = "./public_links.json.temp";

use crate::{
    file_transfer::CHUNK_SIZE,
    mapper::MapStore,
    response::{Code, TransferSuccess},
};

#[derive(Deserialize, Serialize, Debug)]
pub struct PublicLink {
    file_uuid: Uuid,
    valid_until: i64,
    token: Uuid,
}

impl PublicLink {
    pub fn new(file_uuid: Uuid, valid_until: i64, token: Uuid) -> PublicLink {
        PublicLink {
            file_uuid,
            valid_until,
            token,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LinkDatabase {
    links: Vec<PublicLink>,
}

impl LinkDatabase {
    pub fn load() -> Self {
        let path = Path::new(PUBLIC_LINKS_PATH);

        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(db) => db,
                Err(e) => {
                    eprintln!("Failed to parse {}: {e}", PUBLIC_LINKS_PATH);
                    LinkDatabase { links: Vec::new() }
                }
            },
            Err(e) => {
                eprintln!("Failed to read {}: {e}", PUBLIC_LINKS_PATH);
                LinkDatabase { links: Vec::new() }
            }
        }
    }
    pub fn save(&self) -> Result<(), std::io::Error> {
        let contents = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        std::fs::write(TEMP_PUBLIC_LINKS_PATH, contents)?;
        std::fs::rename(TEMP_PUBLIC_LINKS_PATH, PUBLIC_LINKS_PATH)
    }
    pub fn add(&mut self, link: PublicLink) -> Result<(), std::io::Error> {
        self.links.push(link);
        self.save()
    }
    pub fn cleanup(&mut self) -> Result<(), std::io::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.links.retain(|link| now < link.valid_until);
        self.save()
    }
    pub fn get_file_uuid(&mut self, token: &Uuid) -> Option<Uuid> {
        self.cleanup();
        match self.links.iter().find(|link| &link.token == token) {
            Some(link) => Some(link.file_uuid),
            None => None,
        }
    }
}

pub fn share_link(
    mut stream: TcpStream,
    first_message: [u8; CHUNK_SIZE],
    map_store: MapStore,
    client_uuid: &Uuid,
    offset: usize,
    public_links: &Arc<RwLock<LinkDatabase>>,
) {
    println!("share_link called");
    println!("public links: {:?}", public_links);
    let file_uuid = Uuid::from_bytes(first_message[offset..offset + 16].try_into().unwrap());
    let map_read = map_store.read().unwrap();
    match map_read.find_file_clone(&file_uuid, client_uuid) {
        Ok(f) => f,
        Err(e) => {
            println!("share link failed: {:?}", e);
            stream.write_all(&[e.get_code()]);
            return;
        }
    };

    let valid_until = i64::from_be_bytes(
        first_message[offset + 16..offset + 16 + 8]
            .try_into()
            .unwrap(),
    );

    let token = Uuid::new_v4();

    let mut response_buf = [0u8; 17];
    response_buf[0] = TransferSuccess::Ok.get_code();
    response_buf[1..].copy_from_slice(token.as_bytes());

    let mut links_write = public_links.write().unwrap();
    println!(
        "{:?}",
        links_write.add(PublicLink::new(file_uuid, valid_until, token))
    );
    stream.write_all(&response_buf);
}
