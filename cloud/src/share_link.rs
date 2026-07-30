use std::{
    eprintln,
    io::Write,
    net::TcpStream,
    path::Path,
    println,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PUBLIC_LINKS_PATH: &str = "./public_links.json";
const TEMP_PUBLIC_LINKS_PATH: &str = "./public_links.json.temp";

use crate::{
    file_transfer::CHUNK_SIZE,
    mapper::MapStore,
    response::{Code, ErrorTransfer, TransferSuccess},
};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PublicLink {
    pub file_name: String,
    pub file_uuid: Uuid,
    pub valid_until: i64,
    pub token: Uuid,
}

impl PublicLink {
    pub fn new(file_uuid: Uuid, valid_until: i64, token: Uuid, file_name: String) -> PublicLink {
        PublicLink {
            file_name,
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
    pub fn get_filename(&self, uuid: &Uuid) -> Result<String, String> {
        match self.links.iter().find(|l| &l.token == uuid) {
            Some(l) => Ok(l.file_name.clone()),
            None => Err("no token".to_string()),
        }
    }
    #[allow(dead_code)]
    pub fn get_link_from_token(&self, token: &Uuid) -> Result<PublicLink, String> {
        self.links
            .iter()
            .find(|l| &l.token == token)
            .cloned()
            .ok_or_else(|| "no token".to_string())
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
        match self.cleanup() {
            Ok(_) => (),
            Err(e) => {
                eprintln!("failed to cleanup: {e:?}. Carrying on");
            }
        };
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
    let file_uuid = Uuid::from_bytes(first_message[offset..offset + 16].try_into().unwrap());
    let map_read = match map_store.read() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to get read on map_store: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };
    let fil = match map_read.find_file_clone(&file_uuid, client_uuid) {
        Ok(f) => f,
        Err(e) => {
            println!("share link failed: {:?}", e);
            let _ = stream.write_all(&[e.get_code()]);
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

    let mut links_write = public_links.write().unwrap_or_else(|pl| {
        eprintln!("public_links was poisoned");
        pl.into_inner()
    });

    match links_write.add(PublicLink::new(file_uuid, valid_until, token, fil.name)) {
        Ok(_) => (),
        Err(e) => {
            eprintln!("failed to add link to links: {e:?}");
            let _ = stream.write_all(&[ErrorTransfer::InternalServerError.get_code()]);
            return;
        }
    };

    let _ = stream.write_all(&response_buf);
}
