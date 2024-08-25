use std::collections::{HashMap, HashSet, VecDeque};
use sqlx::{Sqlite, Transaction};
use crate::fetch::{Change, Item};
use crate::model::{ChangedFile, ChangedFolder, ChangedPath, Drive, File, Folder};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePool, SqlitePoolOptions};
use tracing::{debug, error, info, trace, warn};

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

fn item_to_change(drive_id: &str, item: Item) -> Change {
    match item.drive_id() == drive_id {
        true => Change::ItemChanged(item),
        false => {
            trace!("moved to another shared drive, marked as removed");
            Change::ItemRemoved(item.into_id())
        }
    }
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

    // 更新 page_token
    Drive::update_page_token(drive_id, page_token, &mut tx).await?;

    // 收集所有变更
    let mut folder_changes: HashMap<String, FolderChange> = HashMap::new();
    let mut file_changes: HashMap<String, FileChange> = HashMap::new();

    for change in changes {
        match change {
            Change::DriveChanged(drive) => {
                Folder::update_name(drive_id, drive_id, &drive.name, &mut tx).await?;
            }
            Change::DriveRemoved(_) => (), // 忽略，因为我们正在处理这个驱动器
            Change::ItemChanged(Item::Folder(folder)) => {
                folder_changes.insert(folder.id.clone(), FolderChange::Update(folder));
            }
            Change::ItemChanged(Item::File(file)) => {
                file_changes.insert(file.id.clone(), FileChange::Update(file));
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

    // 处理文件夹变更
    for (folder_id, change) in folder_changes {
        match change {
            FolderChange::Update(folder) => {
                folder.upsert(&mut tx).await?;
            }
            FolderChange::Remove => {
                // 级联删除会处理子项目
                Folder::delete(&folder_id, drive_id, &mut tx).await?;
            }
        }
    }

    // 处理文件变更
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
) -> sqlx::Result<()>
{
    let mut tx = pool.begin().await?;

    // Create the drive
    Drive::create(drive_id, page_token, &mut tx).await?;

    // Separate folders and files
    let (folders, files): (Vec<_>, Vec<_>) = items.into_iter().partition(|item| matches!(item, Item::Folder(_)));

    // Create the root folder
    let root_folder = Folder {
        id: drive_id.to_owned(),
        drive_id: drive_id.to_owned(),
        name: name.to_owned(),
        parent: None,
        trashed: false,
    };
    root_folder.create(&mut tx).await?;

    // Create a map of parent_id to vec of child folders
    let mut folder_map: HashMap<Option<String>, Vec<Folder>> = HashMap::new();
    for item in folders {
        if let Item::Folder(folder) = item {
            folder_map.entry(folder.parent.clone()).or_default().push(folder);
        }
    }
    // 打印 folder_map 的值
    debug!("folder_map: {:?}", folder_map);

    // Process folders in level order
    if let Err(e) = process_folders(&mut tx, drive_id, &folder_map, root_folder.id.clone()).await {
        tx.rollback().await?;
        return Err(e);
    }

    // Insert all files
    if let Err(e) = insert_files(&mut tx, &files, &folder_map).await {
        tx.rollback().await?;
        return Err(e);
    }

    // Commit the transaction
    tx.commit().await
}

async fn process_folders(
    tx: &mut Transaction<'_, Sqlite>,
    drive_id: &str,
    folder_map: &HashMap<Option<String>, Vec<Folder>>,
    // 将 root_folder.id 作为参数传递
    root_folder_id: String,
) -> sqlx::Result<()>
{
    let mut queue = VecDeque::new();
    queue.push_back(Some(root_folder_id));
    let mut folder_ids = HashSet::new();
    folder_ids.insert(drive_id.to_string());

    while let Some(parent) = queue.pop_front() {
        if let Some(children) = folder_map.get(&parent) {
            for folder in children {
                if parent.is_some() && !folder_ids.contains(parent.as_ref().unwrap()) {
                    warn!(
                        "Parent folder ID {} not found for new folder ID {}, skipping insertion",
                        parent.as_ref().unwrap(),
                        folder.id
                    );
                    continue;
                }
                match folder.create(tx).await {
                    Ok(_) => {
                        trace!("Created folder: {}", folder.id);
                        folder_ids.insert(folder.id.clone());
                        queue.push_back(Some(folder.id.clone()));
                    }
                    Err(e) => {
                        error!("Failed to create folder {}: {}", folder.id, e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn insert_files(
    tx: &mut Transaction<'_, Sqlite>,
    files: &[Item],
    folder_map: &HashMap<Option<String>, Vec<Folder>>,
) -> sqlx::Result<()>
{
    for item in files {
        if let Item::File(file) = item {
            if !folder_map.values().flatten().any(|folder| &folder.id == &file.parent) {
                warn!(
                    "Parent folder ID {} not found for new file ID {}, skipping insertion",
                    file.parent,
                    file.id
                );
                continue;
            }
            match file.create(tx).await {
                Ok(_) => {
                    trace!("Created file: {}", file.id);
                }
                Err(e) => {
                    error!("Failed to create file {}: {}", file.id, e);
                }
            }
        }
    }
    Ok(())
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