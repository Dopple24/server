use std::{
    io::{Read, Result, Write},
    net::TcpStream,
    thread,
};

use uuid::Uuid;

use crate::{SOCKET, request_file::CHUNK_SIZE};

use tiny_http::{Header, Request, Response, StatusCode};

fn request_to_writer<W: Write>(mut stream: TcpStream, uuid: &Uuid, mut out: W) -> Result<()> {
    let mut first_msg = [0u8; CHUNK_SIZE];
    first_msg[0] = 200;
    first_msg[1..17].copy_from_slice(uuid.as_bytes());
    stream.write_all(&first_msg)?;

    let mut meta = [0u8; 5];
    stream
        .read_exact(&mut meta)
        .expect("failed to read metadata from storage server");
    assert_eq!(
        meta[0], 20,
        "unexpected opcode in metadata response, expected 20"
    );
    let chunks_len = u32::from_be_bytes(
        meta[1..5]
            .try_into()
            .expect("failed to parse chunks_len from metadata"),
    ) as u64;

    stream.write_all(&[20])?;

    for chunk_id in 0..chunks_len {
        let mut req = [0u8; 9];
        req[0] = 18;
        req[1..9].copy_from_slice(&chunk_id.to_be_bytes());
        stream.write_all(&req)?;

        let mut header = [0u8; 11];
        stream.read_exact(&mut header)?;
        assert_eq!(
            header[0], 2,
            "unexpected opcode in chunk response for chunk {chunk_id}, expected 2"
        );
        let size = u16::from_be_bytes(
            header[9..11]
                .try_into()
                .expect("failed to parse chunk size from header"),
        ) as usize;

        let mut chunk = vec![0u8; size];
        stream.read_exact(&mut chunk)?;
        out.write_all(&chunk)?;
    }

    stream.write_all(&[0])?;
    Ok(())
}

pub fn handle_download(req: Request, uuid: &Uuid) {
    let (reader, writer) = std::io::pipe().expect("failed to create pipe");
    let filename = format!("attachment; filename=\"{}\"", uuid);
    let uuid = uuid.clone();

    thread::spawn(move || {
        request_to_writer(
            TcpStream::connect(SOCKET).expect("failed to connect to storage server"),
            &uuid,
            writer,
        )
        .expect("error during file transfer");
    });

    let headers = vec![
        Header::from_bytes(&b"Content-Type"[..], &b"application/octet-stream"[..])
            .expect("failed to build Content-Type header"),
        Header::from_bytes(&b"Content-Disposition"[..], filename.as_bytes())
            .expect("failed to build Content-Disposition header"),
    ];

    let response = Response::new(StatusCode(200), headers, reader, None, None);
    req.respond(response)
        .expect("failed to send response to client");
}
