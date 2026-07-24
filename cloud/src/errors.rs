use std::io::ErrorKind;

pub fn is_connection_broken(e: &std::io::Error) -> bool {
    match e.kind() {
        ErrorKind::BrokenPipe
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::WriteZero
        | ErrorKind::UnexpectedEof => true,
        _ => false,
    }
}
