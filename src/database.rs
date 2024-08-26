use crate::fetch::{Change, Item};
use crate::model::{ChangedFile, ChangedFolder, ChangedPath, Drive, File, Folder};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePool, SqlitePoolOptions};
use std::collections::{HashMap, HashSet};
use tracing::{error, trace, warn};

pub(crate) type Connection = SqliteConnection;

pub(crate) type Pool = SqlitePool;

pub async fn establish_connection(database_path: &str) -> sqlx::Result<Pool> {
    let options = SqliteConnectOptions::default()
        .create_if_missing(true)
        .foreign_keys(true)
        .filename(database_path);

    let pool = SqlitePoolOptions::new().connect_with(options).await?;

    sqlx::migrate!().run(&pool).await?;

    Ok(pool)
}

pub async fn clear_changelog(drive_id: &str, pool: &Pool) -> sqlx::Result<()> {
    ChangedFolder::clear(drive_id, pool).await?;
    ChangedFile::clear(drive_id, pool).await?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip(changes, pool))]
pub async fn merge_changes<I>(
    drive_id: &str,
    changes: I,
    page_token: &str,
    pool: &Pool,
) -> sqlx::Result<()>
where
    I: IntoIterator<Item = Change>,
{
    let mut tx = pool.begin().await?;

    // Update the page token
    Drive::update_page_token(drive_id, page_token, &mut tx).await?;

    // Collect all changes
    let mut folder_changes: HashMap<String, FolderChange> = HashMap::new();
    let mut file_changes: HashMap<String, FileChange> = HashMap::new();

    for change in changes {
        match change {
            Change::DriveChanged(drive) => {
                Folder::update_name(drive_id, drive_id, &drive.name, &mut tx).await?;
            }
            Change::DriveRemoved(_) => (), // Ignore, as we're processing this drive
            Change::ItemChanged(item) => {
                // Integrate item_to_change logic
                if item.drive_id() == drive_id {
                    match item {
                        Item::Folder(folder) => {
                            folder_changes.insert(folder.id.clone(), FolderChange::Update(folder));
                        }
                        Item::File(file) => {
                            file_changes.insert(file.id.clone(), FileChange::Update(file));
                        }
                    }
                } else {
                    trace!("moved to another shared drive, marked as removed");
                    let id = item.into_id();
                    if !file_changes.contains_key(&id) {
                        folder_changes.insert(id.clone(), FolderChange::Remove);
                    } else {
                        file_changes.insert(id, FileChange::Remove);
                    }
                }
            }
            Change::ItemRemoved(id) => {
                if !file_changes.contains_key(&id) {
                    folder_changes.insert(id.clone(), FolderChange::Remove);
                } else {
                    file_changes.insert(id, FileChange::Remove);
                }
            }
        }
    }

    // Process folder changes
    for (folder_id, change) in folder_changes {
        match change {
            FolderChange::Update(folder) => {
                folder.upsert(&mut tx).await?;
            }
            FolderChange::Remove => {
                // Cascade delete will handle child items
                Folder::delete(&folder_id, drive_id, &mut tx).await?;
            }
        }
    }

    // Process file changes
    for (file_id, change) in file_changes {
        match change {
            FileChange::Update(file) => {
                file.upsert(&mut tx).await?;
            }
            FileChange::Remove => {
                File::delete(&file_id, drive_id, &mut tx).await?;
            }
        }
    }

    tx.commit().await
}

enum FolderChange {
    Update(Folder),
    Remove, // (id, drive_id)
}

enum FileChange {
    Update(File),
    Remove, // (id, drive_id)
}

#[tracing::instrument(level = "debug", skip(name, items, pool))]
pub async fn add_drive(
    drive_id: &str,
    name: &str,
    page_token: &str,
    items: impl IntoIterator<Item = Item>,
    pool: &Pool,
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;

    // Create the drive
    Drive::create(drive_id, page_token, &mut tx).await?;

    // Create the root folder
    let root_folder = Folder {
        id: drive_id.to_owned(),
        drive_id: drive_id.to_owned(),
        name: name.to_owned(),
        parent: None,
        trashed: false,
    };
    root_folder.create(&mut tx).await?;

    // Separate folders and files, and create maps
    let mut folders: HashMap<String, Folder> = HashMap::new();
    let mut files: Vec<File> = Vec::new();

    for item in items {
        match item {
            Item::Folder(folder) => {
                folders.insert(folder.id.clone(), folder);
            }
            Item::File(file) => {
                files.push(file);
            }
        }
    }

    // Process folders
    let mut processed_folders: HashSet<String> = HashSet::new();
    processed_folders.insert(drive_id.to_string());

    let mut orphaned_folders = Vec::new();

    while !folders.is_empty() {
        let mut progress = false;
        let mut folders_to_remove = Vec::new();

        for (id, folder) in &folders {
            if processed_folders.contains(folder.parent.as_ref().unwrap_or(&drive_id.to_string())) {
                folder.create(&mut tx).await?;
                processed_folders.insert(id.clone());
                folders_to_remove.push(id.clone());
                progress = true;
            }
        }

        // Remove processed folders
        for id in &folders_to_remove {
            folders.remove(id);
        }

        // If no progress, remaining folders are orphaned
        if !progress {
            orphaned_folders.extend(folders.keys().cloned());
            break;
        }
    }

    if !orphaned_folders.is_empty() {
        error!("Orphaned folders detected: {:?}", orphaned_folders);
        warn!("Continuing processing despite orphaned folders");
    }

    // Process files
    for file in files {
        if processed_folders.contains(&file.parent) {
            file.create(&mut tx).await?;
        } else {
            warn!(
                "Parent folder {} not found for file {}, skipping",
                file.parent, file.id
            );
        }
    }

    // Commit the transaction
    tx.commit().await
}

pub async fn get_drive(drive_id: &str, pool: &Pool) -> sqlx::Result<Option<Drive>> {
    Drive::get_by_id(drive_id, pool).await
}

pub async fn get_changed_files(drive_id: &str, pool: &Pool) -> sqlx::Result<Vec<ChangedFile>> {
    ChangedFile::get_all(drive_id, pool).await
}

pub async fn get_changed_folders(drive_id: &str, pool: &Pool) -> sqlx::Result<Vec<ChangedFolder>> {
    ChangedFolder::get_all(drive_id, pool).await
}

pub async fn get_changed_paths(drive_id: &str, pool: &Pool) -> sqlx::Result<Vec<ChangedPath>> {
    ChangedPath::get_all(drive_id, pool).await
}
