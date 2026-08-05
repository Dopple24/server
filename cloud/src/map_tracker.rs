use std::{
    net::TcpStream,
    sync::{Condvar, Mutex},
};

use uuid::Uuid;

use crate::{get_map::get_map, mapper::MapStore};

pub fn track(
    mut stream: TcpStream,
    map_store: MapStore,
    client_uuid: &Uuid,
    signal: &(Mutex<u64>, Condvar),
) {
    println!("called track by client: {client_uuid:?}");
    let mut last_seen = 0;
    loop {
        let (lock, cvar) = &signal;
        let mut version = lock.lock().unwrap();
        while *version == last_seen {
            version = cvar.wait(version).unwrap();
        }

        last_seen = *version;
        drop(version);
        get_map(&mut stream, &map_store, client_uuid);
    }
}

pub fn notify_change(signal: &(Mutex<u64>, Condvar)) {
    let (lock, cvar) = signal;
    {
        let mut version = lock.lock().unwrap();
        *version += 1;
    } // lock released here
    cvar.notify_all();
    println!("notified");
}
