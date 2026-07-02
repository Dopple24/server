use router::handle_client;
use std::net::TcpListener;

mod file_transfer;
mod request;
mod response;
mod router;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6543")?;
    println!("Server listening on 127.0.0.1:6543");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr()?);

                std::thread::spawn(|| {
                    println!("was there");
                    handle_client(stream, 5);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
