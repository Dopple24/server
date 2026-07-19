use crate::file_transfer::CHUNK_SIZE;
use crate::response::ErrorTransfer;

#[derive(Debug, PartialEq)]
pub enum RequestType {
    //each request should have type ([0])
    //uuid of transfer ([1..16]); file size ([16..23]); file name size [23], file name (24..)
    Init,
    //index of chunk ([1..16]); chunk size ([16..18]); chunk ([18..])
    Reinit,
    //uuid of reinit transfer ([1..16])
    ChunkTransfer,
    GetFile,
    ReinitGetFile,
    //0
    Disconnect,
    CompletionCheck,
    Verification,
    GetMap,
    Register,
    Unknown,
}

impl RequestType {
    pub fn get_type(code: u8) -> Self {
        match code {
            0 => Self::Disconnect,
            1 => Self::Init,
            10 => Self::Reinit,
            2 => Self::ChunkTransfer,
            3 => Self::CompletionCheck,
            4 => Self::Verification,
            5 => Self::GetFile,
            6 => Self::ReinitGetFile,
            8 => Self::Register,
            9 => Self::GetMap,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug)]
pub struct Request {
    pub request_type: RequestType,
    pub contents: Vec<u8>,
}

impl Request {
    pub fn decipher(data: [u8; CHUNK_SIZE]) -> Result<Self, ErrorTransfer> {
        if data.len() < 7 {
            println!("this too long or sth: {:?}, {}", data, data.len());
            return Err(ErrorTransfer::InvalidLength);
        }

        let req_type = match RequestType::get_type(data[0]) {
            RequestType::Unknown => return Err(ErrorTransfer::NotFound),
            x => x,
        };
        Ok(Self {
            request_type: req_type,
            contents: data[1..].to_vec(),
        })
    }
}
