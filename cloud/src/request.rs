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
    Delete,
    GuestRequestFile,
    ShareLink,
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
            100 => Self::ShareLink,
            200 => Self::GuestRequestFile,
            255 => Self::Delete,
            _ => Self::Unknown,
        }
    }
}
