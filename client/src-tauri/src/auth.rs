use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::request_file::CHUNK_SIZE;

#[derive(Debug)]
pub enum DatabaseError {
    UsernameOccupied,
    HashingFailed,
    FailedToSave,
    FailedToLoad,
    MalformedMessage,
    Forbidden,
    Unknown,
}

pub fn register(
    mut stream: TcpStream,
    username: &str,
    pass: &str,
    admin_pass: &str,
) -> Result<(), DatabaseError> {
    let username_bytes = username.as_bytes();
    let pass_bytes = pass.as_bytes();
    let admin_pass_bytes = admin_pass.as_bytes();

    if username_bytes.len() > 255 || pass_bytes.len() > 255 {
        return Err(DatabaseError::MalformedMessage);
    }

    let username_start = 2;
    let username_end = username_start + username_bytes.len();
    let password_start = username_end + 1;
    let password_end = password_start + pass_bytes.len();
    let admin_password_start = password_end + 1;
    let admin_password_end = admin_password_start + admin_pass_bytes.len();

    if admin_password_end > CHUNK_SIZE {
        return Err(DatabaseError::MalformedMessage);
    }

    let mut buf = [0u8; CHUNK_SIZE];
    buf[0] = 8;
    buf[1] = username_bytes.len() as u8;
    buf[username_start..username_end].copy_from_slice(username_bytes);
    buf[username_end] = pass_bytes.len() as u8;
    buf[password_start..password_end].copy_from_slice(pass_bytes);
    buf[password_end] = admin_pass_bytes.len() as u8;
    buf[admin_password_start..admin_password_end].copy_from_slice(admin_pass_bytes);

    stream.write_all(&buf).map_err(|e| {
        eprintln!("failed to send register message: {:?}", e);
        return DatabaseError::FailedToSave; // or a dedicated network-error variant, see below
    });

    let mut response_buf = [0u8; 1];
    stream.read(&mut response_buf);
    match response_buf[0] {
        20 => {
            println!("success");
            Ok(())
        }
        40 => {
            println!(
                "sometihing wrong with username, password or admin password - either too long or in bad format"
            );
            Err(DatabaseError::MalformedMessage)
        }
        41 => {
            println!("choose a different username");
            Err(DatabaseError::UsernameOccupied)
        }
        44 => {
            println!("incorrect admin password");
            Err(DatabaseError::Forbidden)
        }
        50 => {
            println!("internal server error");
            Err(DatabaseError::FailedToLoad)
        }
        _ => {
            println!("unknown response code");
            Err(DatabaseError::Unknown)
        }
    }
}
