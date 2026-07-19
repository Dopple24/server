use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
};

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::file_transfer::CHUNK_SIZE;

const DATABASE_LOCATION: &str = "./database.json";
const NEW_DATABASE_LOCATION: &str = "./new_database.json";

#[derive(Debug)]
pub enum DatabaseError {
    UsernameOccupied,
    HashingFailed,
    FailedToSave,
    FailedToLoad,
    MalformedMessage,
    Forbidden,
}

#[derive(Serialize, Deserialize, Debug)]
struct Database {
    users: Vec<User>,
}

#[derive(Serialize, Deserialize, Debug)]
struct User {
    username: String,
    password_hash: String,
    uuid: Uuid,
}

impl Database {
    pub fn new_user(&mut self, username: &str, pass: &str) -> Result<Uuid, DatabaseError> {
        if self.find_by_username(username).is_some() {
            return Err(DatabaseError::UsernameOccupied);
        }
        let new_user = User::new(username, pass)?;
        let uuid = new_user.uuid;
        self.users.push(new_user);
        Ok(uuid)
    }
    pub fn find_by_username(&self, username: &str) -> Option<&User> {
        self.users.iter().find(|u| u.username == username)
    }
    pub fn login(&self, username: &str, password: &str) -> Option<Uuid> {
        let user = self.find_by_username(username)?;
        let parsed_hash = PasswordHash::new(&user.password_hash).ok()?;
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .ok()?;
        Some(user.uuid)
    }
    pub fn save(&self) -> Result<(), DatabaseError> {
        let mut new_database_file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(Path::new(NEW_DATABASE_LOCATION))
        {
            Ok(fil) => fil,
            Err(e) => {
                eprint!("error opening database file: {:?}", e);
                return Err(DatabaseError::FailedToSave);
            }
        };
        let json_bytes = match serde_json::to_string_pretty(self) {
            Ok(json) => json.into_bytes(),
            Err(e) => {
                eprintln!("invalid json: {:?}", e);
                return Err(DatabaseError::FailedToSave);
            }
        };
        match new_database_file.write_all(&json_bytes) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("failed to write into a file: {:?}", e);
                return Err(DatabaseError::FailedToSave);
            }
        };

        if let Err(e) = new_database_file.sync_all() {
            eprintln!("failed to flush database file to disk: {:?}", e);
            return Err(DatabaseError::FailedToSave);
        }

        match std::fs::rename(NEW_DATABASE_LOCATION, DATABASE_LOCATION) {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("failed to replace old database with new one: {:?}", e);
                Err(DatabaseError::FailedToSave)
            }
        }
    }

    pub fn load() -> Result<Self, DatabaseError> {
        let mut database_file = match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(Path::new(DATABASE_LOCATION))
        {
            Ok(fil) => fil,
            Err(e) => {
                eprint!("error opening database file: {:?}", e);
                return Err(DatabaseError::FailedToLoad);
            }
        };
        let mut database_string = String::new();
        match database_file.read_to_string(&mut database_string) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("failed to stringify: {:?}", e);
                return Err(DatabaseError::FailedToLoad);
            }
        };
        if database_string.trim().is_empty() {
            return Ok(Database { users: Vec::new() });
        }
        match serde_json::from_str(&database_string) {
            Ok(val) => Ok(val),
            Err(e) => {
                eprintln!("failed to parse database string into struct: {:?}", e);
                Err(DatabaseError::FailedToLoad)
            }
        }
    }
}

impl User {
    pub fn new(username: &str, pass: &str) -> Result<Self, DatabaseError> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = match Argon2::default().hash_password(pass.as_bytes(), &salt) {
            Ok(val) => val.to_string(),
            Err(_) => return Err(DatabaseError::HashingFailed),
        };
        Ok(User {
            username: username.to_string(),
            password_hash: hash,
            uuid: Uuid::new_v4(),
        })
    }
}

pub fn register(
    mut stream: TcpStream,
    first_message: &[u8; CHUNK_SIZE],
) -> Result<(), DatabaseError> {
    let (username, pass, admin_pass) = match parse_register_message(first_message) {
        Ok(val) => val,
        Err(e) => {
            let buf = [40u8; 1];
            stream.write_all(&buf);
            return Err(e);
        }
    };
    if !has_admin_privileges(&admin_pass) {
        let buf = [44u8; 1];
        stream.write_all(&buf);
        return Err(DatabaseError::Forbidden);
    }
    let mut database = match Database::load() {
        Ok(val) => val,
        Err(e) => {
            let buf = [50u8; 1];
            stream.write_all(&buf);
            return Err(e);
        }
    };
    match database.new_user(&username, &pass) {
        Ok(_) => {
            let buf = [20u8; 1];
            stream.write_all(&buf);
            database.save();
            Ok(())
        }
        Err(e) => {
            let buf = [41u8; 1];
            stream.write_all(&buf);
            Err(e)
        }
    }
}

fn parse_register_message(
    buf: &[u8; CHUNK_SIZE],
) -> Result<(String, String, String), DatabaseError> {
    let username_len = buf[1] as usize;
    let username_start = 2;
    let username_end = username_start + username_len;

    if username_end >= CHUNK_SIZE {
        return Err(DatabaseError::MalformedMessage); // bounds check before anything else
    }

    let username = String::from_utf8(buf[username_start..username_end].to_vec())
        .map_err(|_| DatabaseError::MalformedMessage)?;

    let password_len = buf[username_end] as usize;
    let password_start = username_end + 1;
    let password_end = password_start + password_len;

    if password_end > CHUNK_SIZE {
        return Err(DatabaseError::MalformedMessage);
    }

    let password = String::from_utf8(buf[password_start..password_end].to_vec())
        .map_err(|_| DatabaseError::MalformedMessage)?;

    let admin_password_len = buf[password_end] as usize;
    let admin_password_start = password_end + 1;
    let admin_password_end = admin_password_start + admin_password_len;

    if admin_password_end > CHUNK_SIZE {
        return Err(DatabaseError::MalformedMessage);
    }

    let admin_password = String::from_utf8(buf[admin_password_start..admin_password_end].to_vec())
        .map_err(|_| DatabaseError::MalformedMessage)?;

    Ok((username, password, admin_password))
}

fn has_admin_privileges(pass: &str) -> bool {
    let admin_hash = match std::env::var("ADMIN_PASSWORD_HASH") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("ADMIN_PASSWORD_HASH not set");
            return false;
        }
    };
    let parsed_hash = match PasswordHash::new(&admin_hash).ok() {
        Some(val) => val,
        None => {
            eprintln!("failed to parse admin hash");
            return false;
        }
    };
    Argon2::default()
        .verify_password(pass.as_bytes(), &parsed_hash)
        .is_ok()
}

pub fn login_api(buf: &[u8; CHUNK_SIZE]) -> Option<(Uuid, usize)> {
    let username_len = buf[1] as usize;
    let username_start = 2;
    let username_end = username_start + username_len;

    if username_end >= CHUNK_SIZE {
        return None;
    }

    let username = std::str::from_utf8(&buf[username_start..username_end]).ok()?;

    let password_len = buf[username_end] as usize;
    let password_start = username_end + 1;
    let password_end = password_start + password_len;

    if password_end > CHUNK_SIZE {
        return None;
    }

    let password = std::str::from_utf8(&buf[password_start..password_end]).ok()?;

    let uuid = match Database::load() {
        Ok(val) => val,
        Err(e) => {
            eprintln!("failed to load database: {:?}", e);
            return None;
        }
    }
    .login(username, password);
    match uuid {
        Some(u) => Some((u, password_end)),
        None => None,
    }
}
