use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
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
    pub fn new(owner: Uuid) -> Self {
        AccessControl {
            owner,
            is_public_for_viewing: false,
            is_public_for_changing: false,
            is_visible_for: Vec::new(),
            is_editable_for: Vec::new(),
        }
    }

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
    pub fn find_mut(
        target: &Uuid,
        map: &MapStore,
        client_uuid: &Uuid,
    ) -> Result<Fil, ErrorTransfer> {
        let guard = map.inner.read().unwrap();
        let files = guard.list_files();
        let fil = files.iter().find(|fil| &fil.uuid == target);
        match fil {
            None => return Err(ErrorTransfer::NotFound),
            Some(f) => {
                if f.access.can_view(client_uuid) {
                    Ok(f.clone())
                } else {
                    Err(ErrorTransfer::Forbidden)
                }
            }
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
    pub fn lock_unchecked(&mut self) {
        self.is_locked = true;
    }
    pub fn unlock(&mut self) {
        self.is_locked = false;
    }
}

impl Folder {
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
}

#[derive(Debug)]
pub enum MapError {
    Io(io::Error),
    Json(serde_json::Error),
    FolderNotFound(Uuid),
    /// Another thread poisoned the lock by panicking while holding it.
    Poisoned,
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
        Ok(())
    }

    /// Rebuilds the map from `path` on disk, replacing the in-memory map
    /// and persisting it. Takes the write lock for the whole operation,
    /// so no reads or other writes can interleave.
    pub fn map_new(&self, path: &PathBuf) -> Result<(), MapError> {
        let owner = Uuid::new_v4();
        let new_root = Folder::scan(path, owner)?;

        let mut guard = self.inner.write().map_err(|_| MapError::Poisoned)?;
        persist(&new_root)?;
        *guard = new_root;
        Ok(())
        // write guard dropped here -> readers/writers unblocked
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
        // write guard dropped here
    }

    pub fn remove_file(&self, file_uuid: &Uuid, client_uuid: &Uuid) -> Result<(), ErrorTransfer> {
        let mut map_write = self.inner.write().unwrap();
        let mut folder = match map_write.find_file_parent(file_uuid, client_uuid) {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        if let Some(pos) = folder.files.iter().position(|file| &file.uuid == file_uuid) {
            folder.files.remove(pos);
            println!("map_write: {:#?}", map_write);
            persist(&mut map_write);
        };

        Ok(())
    }

    /// Read-only access to the map. Any number of readers can hold this
    /// concurrently; they only block while a write is in progress.
    pub fn read(&self) -> Result<RwLockReadGuard<'_, Folder>, MapError> {
        self.inner.read().map_err(|_| MapError::Poisoned)
    }
}
