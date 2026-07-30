// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    net::TcpStream,
    path::Path,
    sync::{Arc, RwLock},
};
use tauri::State;

use std::sync::mpsc;
use tauri_plugin_dialog::DialogExt;

use crate::{
    app::{get_parts_rw_lock, sending, SOCKET},
    login_attempt::login_attempt,
    reinit::{reinit, Parts},
};

mod app;
mod auth;
mod create_folder;
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
fn request_parts(parts: State<'_, Arc<RwLock<Parts>>>) -> Result<Parts, String> {
    parts
        .read()
        .map(|guard| guard.clone())
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn try_login(username: &str, password: &str) -> Result<bool, String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    Ok(login_attempt(stream, username, password))
}

#[tauri::command]
fn delete_file(username: &str, password: &str, uuid: &str) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    match delete_file::delete(stream, username, password, uuid) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
fn create_folder(
    username: &str,
    password: &str,
    folder_uuid: &str,
    folder_name: &str,
) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    match create_folder::create_folder(stream, username, password, folder_uuid, folder_name) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
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
    let _ = rx.recv();
    println!("recv returned");
}

#[tauri::command]
async fn upload(
    username: String,
    password: String,
    folder_uuid: String,
    parts: State<'_, Arc<RwLock<Parts>>>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let (tx, rx) = mpsc::channel();
    app.dialog().file().pick_file(move |file_path| {
        let _ = tx.send(file_path.map(|p| p.to_string()));
    });

    let path = tauri::async_runtime::spawn_blocking(move || rx.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
        .ok_or("failed to select a file".to_string())?;

    println!("upload called");
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");

    let file_name = Path::new(&path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            eprintln!("invalid filename");
            "invalid filename".to_string()
        })?;

    match sending(stream, &path, parts, &username, &password, &folder_uuid) {
        //<-- this panics
        Ok(_) => Ok(file_name.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
async fn upload_reinit(
    username: String,
    password: String,
    send_uuid: String,
    parts: State<'_, Arc<RwLock<Parts>>>,
) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");
    match reinit(stream, parts, &send_uuid, &username, &password) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
async fn download(
    username: String,
    password: String,
    file_uuid: String,
    parts: State<'_, Arc<RwLock<Parts>>>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    app.dialog().file().save_file(move |file_path| {
        let _ = tx.send(file_path.map(|p| p.to_string()));
    });
    let path = tauri::async_runtime::spawn_blocking(move || rx.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
        .ok_or("no destination selected".to_string())?;
    println!("download called");
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");
    match request_file::request(stream, 10, parts, &username, &password, &file_uuid, &path) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
async fn download_reinit(
    username: String,
    password: String,
    acc_uuid: String,
    parts: State<'_, Arc<RwLock<Parts>>>,
) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");
    match request_file::reinitialize(stream, parts, 10, &acc_uuid, &username, &password) {
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
            upload_reinit,
            test_dialog,
            delete_file,
            download,
            download_reinit,
            request_parts,
            create_folder
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
