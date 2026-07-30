use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Serialize};
use std::{
    eprintln,
    fs::{self, create_dir, remove_dir_all},
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard},
};
use uuid::Uuid;

use crate::response::ErrorTransfer;

const MAP_PATH: &str = "./map.json";
const MAP_TMP_PATH: &str = "./map.json.tmp";

/// Shared ownership/permission fields, used by both Folder and Fil.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AccessControl {
    pub owner: Uuid,
    pub is_public_for_viewing: bool,
    pub is_public_for_changing: bool,
    pub is_visible_for: Vec<Uuid>,
    pub is_editable_for: Vec<Uuid>,
}

impl AccessControl {
    pub fn can_view(&self, user: &Uuid) -> bool {
        self.is_public_for_viewing
            || &self.owner == user
            || self.is_visible_for.contains(&user)
            || self.can_edit(user)
    }

    pub fn can_edit(&self, user: &Uuid) -> bool {
        self.is_public_for_changing || &self.owner == user || self.is_editable_for.contains(&user)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Folder {
    pub uuid: Uuid,
    pub name: String,
    pub last_changed_at: DateTime<Utc>,
    pub folders: Vec<Folder>,
    pub files: Vec<Fil>,
    pub path: PathBuf,
    pub is_locked: bool,
    #[serde(flatten)]
    pub access: AccessControl,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Fil {
    pub name: String,
    pub last_changed_at: DateTime<Utc>,
    pub uuid: Uuid,
    pub path: PathBuf,
    pub is_locked: bool,
    #[serde(flatten)]
    pub access: AccessControl,
}

impl Fil {
    pub fn new(
        filename: String,
        path: PathBuf,
        owner: Uuid,
        is_public_for_viewing: bool,
        is_public_for_changing: bool,
        is_visible_for: Vec<Uuid>,
        is_editable_for: Vec<Uuid>,
    ) -> Self {
        Fil {
            name: filename,
            last_changed_at: Local::now().to_utc(),
            uuid: Uuid::new_v4(),
            path,
            is_locked: false,
            access: AccessControl {
                owner,
                is_public_for_viewing,
                is_public_for_changing,
                is_visible_for,
                is_editable_for,
            },
        }
    }

    pub fn lock(&mut self) -> bool {
        match self.is_locked {
            true => false,
            false => {
                self.is_locked = true;
                true
            }
        }
    }

    pub fn unlock(&mut self) {
        self.is_locked = false;
    }
}

impl Folder {
    #[allow(dead_code)]
    fn scan(path: &Path, owner: Uuid) -> io::Result<Folder> {
        let meta = fs::metadata(path)?;
        let last_changed_at: DateTime<Utc> = meta.modified()?.into();

        let mut folders = Vec::new();
        let mut files = Vec::new();

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                folders.push(Folder::scan(&entry_path, owner)?);
            } else if file_type.is_file() {
                let file_meta = entry.metadata()?;
                let file_changed: DateTime<Utc> = file_meta.modified()?.into();

                files.push(Fil {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    last_changed_at: file_changed,
                    uuid: Uuid::new_v4(),
                    path: entry_path,
                    is_locked: false,
                    access: AccessControl {
                        owner,
                        is_public_for_viewing: true,
                        is_public_for_changing: true,
                        is_visible_for: Vec::new(),
                        is_editable_for: Vec::new(),
                    },
                });
            }
        }

        Ok(Folder {
            uuid: Uuid::new_v4(),
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            last_changed_at,
            folders,
            files,
            path: path.to_path_buf(),
            is_locked: false,
            access: AccessControl {
                owner,
                is_public_for_viewing: false,
                is_public_for_changing: false,
                is_visible_for: Vec::new(),
                is_editable_for: Vec::new(),
            },
        })
    }

    pub fn find_mut(&mut self, target: Uuid) -> Option<&mut Folder> {
        if self.uuid == target {
            return Some(self);
        }
        for sub in &mut self.folders {
            if let Some(found) = sub.find_mut(target) {
                return Some(found);
            }
        }
        None
    }

    fn list_files(&self) -> Vec<Fil> {
        let mut files = self.files.clone();
        files.extend(self.folders.iter().flat_map(|f| f.list_files()));
        files
    }

    pub fn find_file_parent(
        &mut self,
        target: &Uuid,
        client_uuid: &Uuid,
    ) -> Result<&mut Self, ErrorTransfer> {
        if let Some(f) = self.files.iter().find(|f| &f.uuid == target) {
            return if f.access.can_view(client_uuid) {
                Ok(self)
            } else {
                Err(ErrorTransfer::Forbidden)
            };
        }

        for folder in &mut self.folders {
            if let Ok(parent) = folder.find_file_parent(target, client_uuid) {
                return Ok(parent);
            }
        }

        Err(ErrorTransfer::NotFound)
    }

    pub fn find_folder_parent(
        &mut self,
        target: &Uuid,
        client_uuid: &Uuid,
    ) -> Result<&mut Self, MapError> {
        if let Some(f) = self.folders.iter().find(|f| &f.uuid == target) {
            return if f.access.can_view(client_uuid) {
                Ok(self)
            } else {
                Err(MapError::InvalidFolderLocation)
            };
        }
        for folder in &mut self.folders {
            if let Ok(parent) = folder.find_folder_parent(target, client_uuid) {
                return Ok(parent);
            }
        }
        Err(MapError::InvalidFolderLocation)
    }

    pub fn find_file_clone(&self, target: &Uuid, client_uuid: &Uuid) -> Result<Fil, ErrorTransfer> {
        if let Some(f) = self.files.iter().find(|f| &f.uuid == target) {
            return if f.access.can_view(client_uuid) {
                Ok(f.clone())
            } else {
                Err(ErrorTransfer::Forbidden)
            };
        }
        for folder in &self.folders {
            if let Ok(file) = folder.find_file_clone(target, client_uuid) {
                return Ok(file);
            }
        }
        Err(ErrorTransfer::NotFound)
    }

    pub fn new(name: &str, parent_path: &PathBuf, access: AccessControl) -> Self {
        Folder {
            uuid: Uuid::new_v4(),
            name: name.to_string(),
            last_changed_at: Local::now().to_utc(),
            folders: Vec::new(),
            files: Vec::new(),
            path: parent_path.join(name),
            is_locked: false,
            access,
        }
    }
}

#[derive(Debug)]
pub enum MapError {
    #[allow(dead_code)]
    Io(io::Error),
    #[allow(dead_code)]
    Json(serde_json::Error),
    #[allow(dead_code)]
    FolderNotFound(Uuid),
    /// Another thread poisoned the lock by panicking while holding it.
    Poisoned,
    FolderAlreadyPresent,
    InvalidFolderLocation,
}

impl From<io::Error> for MapError {
    fn from(e: io::Error) -> Self {
        MapError::Io(e)
    }
}

impl From<serde_json::Error> for MapError {
    fn from(e: serde_json::Error) -> Self {
        MapError::Json(e)
    }
}

/// Writes `root` to disk atomically: write to a temp file, then rename
/// over the real path. Readers of map.json (in this process or any other
/// tool) never observe a partially-written file.
fn persist(root: &Folder) -> Result<(), MapError> {
    let json = serde_json::to_string_pretty(root)?;
    fs::write(MAP_TMP_PATH, json)?;
    fs::rename(MAP_TMP_PATH, MAP_PATH)?;
    Ok(())
}

/// Shared, thread-safe handle to the in-memory map. Clone this (cheap,
/// just bumps an Arc refcount) to share across threads.
#[derive(Clone, Debug)]
pub struct MapStore {
    pub inner: Arc<RwLock<Folder>>,
}

impl MapStore {
    /// Loads the map from disk into memory.
    pub fn load() -> Result<Self, MapError> {
        let contents = fs::read_to_string(MAP_PATH)?;
        let root: Folder = serde_json::from_str(&contents)?;
        Ok(MapStore {
            inner: Arc::new(RwLock::new(root)),
        })
    }

    pub fn unlock_all(&mut self) -> Result<(), MapError> {
        let folder = self.inner.write().unwrap();
        folder.list_files().iter_mut().for_each(|fil| fil.unlock());
        persist(&folder)
    }

    /// Rebuilds the map from `path` on disk, replacing the in-memory map
    /// and persisting it. Takes the write lock for the whole operation,
    /// so no reads or other writes can interleave.
    #[allow(dead_code)]
    pub fn map_new(&self, path: &PathBuf) -> Result<(), MapError> {
        let owner = Uuid::new_v4();
        let new_root = Folder::scan(path, owner)?;

        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;
        persist(&new_root)?;
        *guard = new_root;
        Ok(())
    }

    /// Inserts `file` into the folder identified by `folder_uuid` (or the
    /// root if `None`), persists to disk, and updates the in-memory map.
    /// Blocks until any in-progress reads finish; blocks other writers
    /// until this completes.
    pub fn add_file(&self, folder_uuid: Option<Uuid>, file: Fil) -> Result<(), MapError> {
        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;

        match folder_uuid {
            None => guard.files.push(file),
            Some(target) => {
                let folder = guard
                    .find_mut(target)
                    .ok_or(MapError::FolderNotFound(target))?;
                folder.files.push(file);
            }
        }

        persist(&guard)?;
        Ok(())
    }

    pub fn delete_folder(&self, folder_uuid: Uuid, client_uuid: &Uuid) -> Result<(), MapError> {
        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;
        let folder = guard
            .find_mut(folder_uuid)
            .ok_or(MapError::FolderNotFound(folder_uuid))?;

        if !folder.access.can_edit(client_uuid) {
            return Err(MapError::InvalidFolderLocation);
        }

        match remove_dir_all(folder.path.clone()) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("failed to delete dir: {e:?}");
                return Err(MapError::InvalidFolderLocation);
            }
        };

        let folder_uuid = folder.uuid;

        println!("folder.folders: {:?}", folder.folders);
        match guard.find_folder_parent(&folder_uuid, client_uuid) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("failed to find folder parent: {e:?}");
                return Err(e);
            }
        }
        .folders
        .retain(|f| {
            let should_retain = f.uuid != folder_uuid;
            if should_retain {
                println!("f.uuid: {:?}, folder_uuid: {:?}", f.uuid, folder_uuid);
            }
            should_retain
        });
        persist(&guard)
    }

    pub fn create_folder(
        &self,
        folder_uuid: Option<Uuid>,
        folder_name: &str,
        client_uuid: &Uuid,
        access: AccessControl,
    ) -> Result<(), MapError> {
        let folder_name = match sanitized_folder_name(folder_name) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("invalid file name: {e:?}");
                return Err(MapError::InvalidFolderLocation);
            }
        };
        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;
        let guard_path = guard.path.clone();
        match folder_uuid {
            None => guard
                .folders
                .push(Folder::new(&folder_name, &guard_path, access)),
            Some(target) => {
                let folder = guard
                    .find_mut(target)
                    .ok_or(MapError::FolderNotFound(target))?;
                if !folder.access.can_edit(client_uuid) {
                    return Err(MapError::InvalidFolderLocation);
                }
                if folder
                    .folders
                    .iter()
                    .find(|f| &f.name == &folder_name)
                    .is_some()
                {
                    return Err(MapError::FolderAlreadyPresent);
                };
                match create_dir(folder.path.clone().join(&folder_name)) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("failed to create dir at : {e:?}");
                        return Err(MapError::InvalidFolderLocation);
                    }
                };
                folder
                    .folders
                    .push(Folder::new(&folder_name, &folder.path, access));
            }
        }

        persist(&guard)?;
        Ok(())
    }

    pub fn get_path(&self, folder_uuid: Uuid) -> Result<PathBuf, MapError> {
        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;
        let folder = guard
            .find_mut(folder_uuid)
            .ok_or(MapError::FolderNotFound(folder_uuid))?;
        Ok(folder.path.clone())
    }

    pub fn remove_file(&self, file_uuid: &Uuid, client_uuid: &Uuid) -> Result<(), ErrorTransfer> {
        let mut map_write = self.inner.write().unwrap();
        let folder = match map_write.find_file_parent(file_uuid, client_uuid) {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        if let Some(pos) = folder.files.iter().position(|file| &file.uuid == file_uuid) {
            folder.files.remove(pos);
            match persist(&mut map_write) {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("failed to persist: {e:?}");
                    return Err(ErrorTransfer::InternalServerError);
                }
            };
        };

        Ok(())
    }

    /// Read-only access to the map. Any number of readers can hold this
    /// concurrently; they only block while a write is in progress.
    pub fn read(&self) -> Result<RwLockReadGuard<'_, Folder>, MapError> {
        self.inner.read().map_err(|_| MapError::Poisoned)
    }
}

fn sanitized_folder_name(folder_name: &str) -> Result<String, String> {
    let name = Path::new(folder_name)
        .file_name()
        .ok_or_else(|| "invalid folder name".to_string())?
        .to_str()
        .ok_or_else(|| "folder name is not valid UTF-8".to_string())?;

    Ok(name.to_string())
}

pub fn with_file_mut<T>(
    target: &Uuid,
    map: &MapStore,
    client_uuid: &Uuid,
    f: impl FnOnce(&mut Fil) -> T,
) -> Result<T, ErrorTransfer> {
    let mut guard = map.inner.write().unwrap(); // needs write lock now
    let fil = find_file_mut(&mut guard, target).ok_or(ErrorTransfer::NotFound)?;

    if !fil.access.can_view(client_uuid) {
        return Err(ErrorTransfer::Forbidden);
    }

    Ok(f(fil))
}

///doesn't check if the current client has access to this file.
pub fn with_file_mut_unchecked<T>(
    target: &Uuid,
    map: &MapStore,
    f: impl FnOnce(&mut Fil) -> T,
) -> Result<T, ErrorTransfer> {
    let mut guard = map.inner.write().unwrap(); // needs write lock now
    let fil = find_file_mut(&mut guard, target).ok_or(ErrorTransfer::NotFound)?;

    Ok(f(fil))
}

/// Recursively searches the folder tree for a file with the given uuid.
fn find_file_mut<'a>(folder: &'a mut Folder, target: &Uuid) -> Option<&'a mut Fil> {
    if let Some(pos) = folder.files.iter().position(|fil| &fil.uuid == target) {
        return Some(&mut folder.files[pos]);
    }
    for sub in folder.folders.iter_mut() {
        if let Some(fil) = find_file_mut(sub, target) {
            return Some(fil);
        }
    }
    None
}
