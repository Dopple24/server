pub trait Code {
    fn get_code(&self) -> u8;
    fn get_message(&self) -> Vec<u8>;
    fn respond(&self, message: Vec<u8>) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(16);
        buf.push(self.get_code());
        buf.extend_from_slice(&message);
        buf
    }
}

pub enum TransferSuccess {
    Ok,
}

impl Code for TransferSuccess {
    fn get_code(&self) -> u8 {
        match self {
            TransferSuccess::Ok => 20,
        }
    }
    fn get_message(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(15);
        let msg = match self {
            TransferSuccess::Ok => "ok".to_string(),
        };
        buffer.extend_from_slice(msg.as_bytes());
        buffer
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ErrorTransfer {
    InvalidLength,
    InvalidHash,
    Overflow,
    NotFound,
    NotInitialized,
    AlreadyInitialized,
    ThisFileExists,
    InternalServerError,
    TooFast,
    HashesDoNotMatch,
    Forbidden,
    Locked,
    InvalidRequest,
    Closed,
}

impl Code for ErrorTransfer {
    fn get_code(&self) -> u8 {
        match self {
            Self::InvalidLength => 40,
            Self::Overflow => 40,
            Self::ThisFileExists => 41,
            Self::InvalidRequest => 42,
            Self::NotFound => 44,
            Self::NotInitialized => 45,
            Self::AlreadyInitialized => 46,
            Self::HashesDoNotMatch => 47,
            Self::Forbidden => 48,
            Self::InvalidHash => 49,
            Self::InternalServerError => 50,
            Self::TooFast => 51,
            Self::Locked => 53,
            Self::Closed => 10,
        }
    }
    fn get_message(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(15);
        let msg: String = match self {
            Self::InvalidLength => "40 invalid length".to_string(),
            Self::InvalidHash => "49 invalid hash".to_string(),
            Self::Overflow => "40 request too big".to_string(),
            Self::InvalidRequest => "42 invalid request".to_string(),
            Self::NotFound => "44 not found".to_string(),
            Self::NotInitialized => "45 not initialized".to_string(),
            Self::AlreadyInitialized => "46 already initialized".to_string(),
            Self::HashesDoNotMatch => "47 hashes do not match".to_string(),
            Self::ThisFileExists => "41 file with this name already exists".to_string(),
            Self::Forbidden => "48 forbiden".to_string(),
            Self::InternalServerError => "50 internal server error".to_string(),
            Self::TooFast => "51 too fast".to_string(),
            Self::Locked => "53 requested file is locked".to_string(),
            Self::Closed => "10 closed".to_string(),
        };
        buffer.extend_from_slice(msg.as_bytes());
        buffer
    }
}
