// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    net::TcpStream,
    sync::{Arc, RwLock},
};
use tauri::State;

use std::sync::mpsc;
use tauri_plugin_dialog::DialogExt;

use crate::{
    app::{get_parts_rw_lock, sending, SOCKET},
    login_attempt::login_attempt,
    reinit::Parts,
};

mod app;
mod auth;
mod delete_file;
mod get_map;
mod guest_request_file;
mod login_attempt;
mod reinit;
mod request_file;
mod response;
mod share_link;

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

#[tauri::command]
fn request_map(username: &str, password: &str) -> Result<get_map::FolderMap, String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    get_map::get_map(stream, username, password).map_err(|e| e.to_string())
}

#[tauri::command]
fn try_login(username: &str, password: &str) -> Result<bool, String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    Ok(login_attempt(stream, username, password))
}

#[tauri::command]
async fn test_dialog(app: tauri::AppHandle) {
    println!("test_dialog called");
    let (tx, rx) = mpsc::channel();
    app.dialog().file().pick_file(move |file_path| {
        println!("picked: {:?}", file_path);
        let _ = tx.send(());
    });
    println!("pick_file() call returned");
    let _ = rx.recv(); // <-- add this
    println!("recv returned");
}

#[tauri::command]
async fn upload(
    username: String,
    password: String,
    folder_uuid: String,
    parts: State<'_, Arc<RwLock<Parts>>>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    app.dialog().file().pick_file(move |file_path| {
        let _ = tx.send(file_path.map(|p| p.to_string()));
    });

    // Move the blocking recv() onto a real blocking-pool thread,
    // so it doesn't block the async task driving the command.
    let path = tauri::async_runtime::spawn_blocking(move || rx.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
        .ok_or("failed to select a file".to_string())?;

    println!("upload called");
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");

    match sending(stream, &path, parts, &username, &password, &folder_uuid) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

fn main() {
    let parts = get_parts_rw_lock();
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(parts)
        .invoke_handler(tauri::generate_handler![
            greet,
            request_map,
            try_login,
            upload,
            test_dialog,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
