// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    env::args,
    io::Write,
    net::TcpStream,
    path::{Path, PathBuf},
    sync::{
        mpsc::{self, Receiver},
        Arc, Mutex, RwLock,
    },
    thread,
};
use tauri::{AppHandle, Emitter, State}; // async_runtime::Mutex removed
use tauri_plugin_dialog::DialogExt;
use uuid::Uuid;

use crate::{
    app::{get_parts_rw_lock, sending, serve_public, SOCKET},
    get_map::first_message,
    login_attempt::login_attempt,
    reinit::{reinit, Parts},
};

#[derive(serde::Serialize, Clone)]
struct FileName {
    transfer_id: String,
    filename: String,
}

#[derive(serde::Serialize, Clone)]
struct UploadItem {
    folder_uuid: String,
    path: String,
    username: String,
    password: String,
    frontend_uuid: String,
    is_reinit: bool,
}

struct UploadQueue {
    tx: Mutex<mpsc::Sender<UploadItem>>,
}

#[derive(serde::Serialize, Clone)]
struct TransferStatus {
    transfer_id: String,
    success: bool,
    error: Option<String>,
}

#[derive(serde::Serialize, Clone)]
struct DownloadItem {
    username: String,
    password: String,
    file_uuid: String,
    frontend_uuid: String,
    path: Option<String>,
    is_reinit: bool,
}

struct DownloadQueue {
    tx: Mutex<mpsc::Sender<DownloadItem>>,
}

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
fn remove_part(uuid: Uuid, parts: State<'_, Arc<RwLock<Parts>>>) -> Result<(), String> {
    let mut parts_write = parts.write().unwrap();
    if let Some(pos) = parts_write.send.iter().position(|item| item.uuid == uuid) {
        parts_write.send.remove(pos);
        let _ = parts_write.save();
        Ok(())
    } else if let Some(pos) = parts_write.acc.iter().position(|item| item.uuid == uuid) {
        parts_write.send.remove(pos);
        let _ = parts_write.save();
        Ok(())
    } else {
        Err("uuid not found".to_string())
    }
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
fn share_file(
    username: &str,
    password: &str,
    uuid: &str,
    hours_after: u64,
) -> Result<String, String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    match share_link::share_link(stream, username, password, uuid, hours_after) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
fn delete_folder(username: &str, password: &str, uuid: &str) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    match delete_file::delete_folder(stream, username, password, uuid) {
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
async fn upload_batch(
    username: String,
    password: String,
    folder_uuid: String,
    upload_q: State<'_, UploadQueue>,
    paths: Vec<(String, String)>,
    handle: AppHandle,
) -> Result<(), String> {
    app::upload_batch(username, password, folder_uuid, upload_q, paths, handle);
    Ok(())
}

#[tauri::command]
async fn upload(
    username: String,
    password: String,
    folder_uuid: String,
    frontend_uuid: String,
    upload_q: State<'_, UploadQueue>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    app.dialog().file().pick_file(move |file_path| {
        let _ = tx.send(file_path.map(|p| p.to_string()));
    });

    let path = tauri::async_runtime::spawn_blocking(move || rx.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
        .ok_or("failed to select a file".to_string())?;

    let file_name = Path::new(&path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            eprintln!("invalid filename");
            "invalid filename".to_string()
        })?;

    let _ = app.emit(
        "file_name",
        FileName {
            transfer_id: frontend_uuid.to_string(),
            filename: file_name.to_string(),
        },
    );

    let item = UploadItem {
        folder_uuid,
        path,
        username,
        password,
        frontend_uuid: frontend_uuid.clone(),
        is_reinit: false,
    };

    {
        let guard = upload_q.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = guard.send(item) {
            let status = TransferStatus {
                transfer_id: frontend_uuid.clone(),
                success: false,
                error: Some(e.to_string()),
            };
            let _ = app.emit("transfer-complete", status);
        };
    }

    Ok(())
}

#[tauri::command]
async fn upload_reinit(
    username: String,
    password: String,
    send_uuid: String,
    frontend_uuid: String,
    upload_q: State<'_, UploadQueue>,
    handle: AppHandle,
) -> Result<(), String> {
    let item = UploadItem {
        folder_uuid: send_uuid,
        path: String::new(),
        username,
        password,
        frontend_uuid: frontend_uuid.clone(),
        is_reinit: true,
    };

    {
        let guard = upload_q.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = guard.send(item) {
            let status = TransferStatus {
                transfer_id: frontend_uuid.clone(),
                success: false,
                error: Some(e.to_string()),
            };
            let _ = handle.emit("transfer-complete", status);
        };
    }

    Ok(())
}

#[tauri::command]
async fn download(
    username: String,
    password: String,
    file_uuid: String,
    frontend_uuid: String,
    file_name: String,
    download_q: State<'_, DownloadQueue>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    app.dialog()
        .file()
        .set_file_name(file_name)
        .save_file(move |file_path| {
            let _ = tx.send(file_path.map(|p| p.to_string()));
        });
    let path = tauri::async_runtime::spawn_blocking(move || rx.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
        .ok_or("no destination selected".to_string())?;

    let item = DownloadItem {
        username,
        password,
        file_uuid,
        frontend_uuid: frontend_uuid.clone(),
        path: Some(path),
        is_reinit: false,
    };

    {
        let guard = download_q.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = guard.send(item) {
            let status = TransferStatus {
                transfer_id: frontend_uuid.clone(),
                success: false,
                error: Some(e.to_string()),
            };
            let _ = app.emit("transfer-complete", status);
        };
    }

    Ok(())
}

#[tauri::command]
async fn download_reinit(
    username: String,
    password: String,
    acc_uuid: String,
    download_q: State<'_, DownloadQueue>,
    frontend_uuid: String,
    handle: AppHandle,
) -> Result<(), String> {
    let item = DownloadItem {
        username,
        password,
        file_uuid: acc_uuid,
        frontend_uuid: frontend_uuid.clone(),
        path: None,
        is_reinit: true,
    };

    {
        let guard = download_q.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = guard.send(item) {
            let status = TransferStatus {
                transfer_id: frontend_uuid.clone(),
                success: false,
                error: Some(e.to_string()),
            };
            let _ = handle.emit("transfer-complete", status);
        };
    }

    Ok(())
}

#[tauri::command]
async fn start_tracker(username: String, password: String, app: AppHandle) -> Result<(), String> {
    let mut stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    let _ = stream.write_all(&first_message(53, &username, &password));
    loop {
        println!("was in start_tracker");
        let map_bytes = match get_map::recv_framed(&mut stream) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to recv_frfamed: {e:?}");
                return Err(e.to_string());
            }
        };
        let map: get_map::FolderMap = match serde_json::from_slice(&map_bytes) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("invalid map: {e:?}");
                return Err(e.to_string());
            }
        };
        println!("emitted map");
        app.emit("folder-map-updated", &map)
            .map_err(|e| e.to_string())?;
    }
}

#[tauri::command]
async fn register(username: String, password: String, admin_pass: String) -> Result<(), String> {
    let stream = TcpStream::connect(SOCKET).map_err(|e| e.to_string())?;
    println!("connected");
    match auth::register(stream, &username, &password, &admin_pass) {
        Ok(a) => Ok(a),
        Err(e) => Err(e.to_string()),
    }
}

fn setup_upload_q(parts: Arc<RwLock<Parts>>, handle: AppHandle, rx: Receiver<UploadItem>) {
    thread::spawn(move || {
        for transfer in rx {
            let stream = match TcpStream::connect(SOCKET) {
                Ok(s) => s,
                Err(e) => {
                    let _ = handle.emit(
                        "transfer-complete",
                        TransferStatus {
                            transfer_id: transfer.frontend_uuid.clone(),
                            success: false,
                            error: Some(e.to_string()),
                        },
                    );
                    continue;
                }
            };

            let mut handle_ref = handle.clone();
            let result = if transfer.is_reinit {
                reinit(
                    stream,
                    parts.clone(),
                    &transfer.folder_uuid,
                    &transfer.username,
                    &transfer.password,
                    &transfer.frontend_uuid,
                    &mut handle_ref,
                )
            } else {
                sending(
                    stream,
                    &transfer.path,
                    parts.clone(),
                    &transfer.username,
                    &transfer.password,
                    &transfer.folder_uuid,
                    &transfer.frontend_uuid,
                    &mut handle_ref,
                )
            };

            let status = match result {
                Ok(_) => TransferStatus {
                    transfer_id: transfer.frontend_uuid.clone(),
                    success: true,
                    error: None,
                },
                Err(e) => TransferStatus {
                    transfer_id: transfer.frontend_uuid.clone(),
                    success: false,
                    error: Some(e.to_string()),
                },
            };

            let _ = handle.emit("transfer-complete", status);
        }
    });
}

fn setup_download_q(parts: Arc<RwLock<Parts>>, mut handle: AppHandle, rx: Receiver<DownloadItem>) {
    thread::spawn(move || {
        for transfer in rx {
            println!("download called");
            let stream = TcpStream::connect(SOCKET)
                .map_err(|e| e.to_string())
                .unwrap();
            println!("connected");

            let res = if transfer.is_reinit {
                match request_file::reinitialize(
                    stream,
                    parts.clone(),
                    10,
                    &transfer.file_uuid,
                    &transfer.username,
                    &transfer.password,
                    &transfer.frontend_uuid,
                    &mut handle,
                ) {
                    Ok(a) => Ok(a),
                    Err(e) => Err(e.to_string()),
                }
            } else {
                if transfer.path.is_none() {
                    continue;
                }
                match request_file::request(
                    stream,
                    10,
                    parts.clone(),
                    &transfer.username,
                    &transfer.password,
                    &transfer.file_uuid,
                    &transfer.path.unwrap(),
                    &transfer.frontend_uuid,
                    &mut handle,
                ) {
                    Ok(a) => Ok(a),
                    Err(e) => Err(e.to_string()),
                }
            };

            let status = match res {
                Ok(_) => TransferStatus {
                    transfer_id: transfer.frontend_uuid.clone(),
                    success: true,
                    error: None,
                },
                Err(e) => TransferStatus {
                    transfer_id: transfer.frontend_uuid.clone(),
                    success: false,
                    error: Some(e.to_string()),
                },
            };

            let _ = handle.emit("transfer-complete", status);
        }
    });
}

fn main() {
    let args: Vec<String> = args().collect();
    if args.get(1).map(|a| a == "--serve_public").unwrap_or(false) {
        serve_public();
    }

    let parts = get_parts_rw_lock();
    let parts_for_state = parts.clone();
    let (tx_upl, rx_upl) = mpsc::channel::<UploadItem>();
    let (tx_dwn, rx_dwn) = mpsc::channel::<DownloadItem>();

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(parts_for_state)
        .manage(UploadQueue {
            tx: Mutex::new(tx_upl),
        })
        .manage(DownloadQueue {
            tx: Mutex::new(tx_dwn),
        })
        .setup(move |app| {
            let handle = app.handle().clone();
            let parts = parts.clone();
            setup_upload_q(parts.clone(), handle.clone(), rx_upl);
            setup_download_q(parts, handle, rx_dwn);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            request_map,
            try_login,
            register,
            upload,
            upload_reinit,
            test_dialog,
            delete_file,
            delete_folder,
            download,
            download_reinit,
            request_parts,
            create_folder,
            share_file,
            remove_part,
            start_tracker,
            upload_batch,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
