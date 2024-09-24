use crate::database::{Connection, Pool};
use futures::prelude::*;
use sqlx::Result;
use tracing::trace;

#[derive(Debug)]
pub struct Folder {
    pub id: String,
    pub drive_id: String,
    pub name: String,
    pub trashed: bool,
    pub parent: Option<String>,
}

impl Folder {
    pub(crate) async fn create(&self, conn: &mut Connection) -> Result<()> {
        match sqlx::query!(
            "
            INSERT INTO folders
                (id, drive_id, name, trashed, parent)
            VALUES
                ($1, $2, $3, $4, $5)
            ",
            self.id,
            self.drive_id,
            self.name,
            self.trashed,
            self.parent,
        )
        .execute(conn)
        .await
        {
            Ok(_) => {
                trace!(id = %self.id, "created folder");
                Ok(())
            },
            Err(e) => {
                tracing::warn!("创建文件夹失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn upsert(&self, conn: &mut Connection) -> Result<()> {
        match sqlx::query!(
            "
            INSERT INTO folders
                (id, drive_id, name, trashed, parent)
            VALUES
                ($1, $2, $3, $4, $5)
            ON CONFLICT (id, drive_id) DO UPDATE SET
                name = EXCLUDED.name,
                trashed = EXCLUDED.trashed,
                parent = EXCLUDED.parent
            ",
            self.id,
            self.drive_id,
            self.name,
            self.trashed,
            self.parent,
        )
        .execute(conn)
        .await
        {
            Ok(_) => {
                trace!(id = %self.id, "upserted folder");
                Ok(())
            },
            Err(e) => {
                tracing::warn!("更新文件夹失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn delete(id: &str, drive_id: &str, conn: &mut Connection) -> Result<()> {
        match sqlx::query!(
            "DELETE FROM folders WHERE id = $1 AND drive_id = $2",
            id,
            drive_id
        )
        .execute(conn)
        .await
        {
            Ok(_) => {
                trace!(id = %id, "deleted folder");
                Ok(())
            },
            Err(e) => {
                tracing::warn!("删除文件夹失败: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_children(
        parent_id: &str,
        drive_id: &str,
        conn: &mut Connection,
    ) -> Result<Option<Vec<String>>> {
        match sqlx::query!(
            r#"
            SELECT id
            FROM folders
            WHERE parent = $1 AND drive_id = $2
            "#,
            parent_id,
            drive_id
        )
        .fetch_all(conn)
        .await
        {
            Ok(rows) => {
                if rows.is_empty() {
                    trace!(parent_id = %parent_id, drive_id = %drive_id, "no children found for folder");
                    Ok(None)
                } else {
                    let children: Vec<String> = rows.into_iter().map(|row| row.id).collect();
                    trace!(parent_id = %parent_id, drive_id = %drive_id, count = %children.len(), "fetched children for folder");
                    Ok(Some(children))
                }
            },
            Err(e) => {
                tracing::warn!("获取文件夹子项失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn update_name(
        id: &str,
        drive_id: &str,
        name: &str,
        conn: &mut Connection,
    ) -> Result<()> {
        match sqlx::query!(
            "UPDATE folders SET name = $3 WHERE id = $1 AND drive_id = $2",
            id,
            drive_id,
            name
        )
        .execute(conn)
        .await
        {
            Ok(_) => {
                trace!(id = %id, "updated folder name to {}", name);
                Ok(())
            },
            Err(e) => {
                tracing::warn!("更新文件夹名称失败: {}", e);
                Err(e)
            }
        }
    }
}

#[derive(Debug)]
pub enum ChangedFolder {
    Created(Folder),
    Deleted(Folder),
}

impl From<ChangedFolder> for Folder {
    fn from(folder: ChangedFolder) -> Self {
        match folder {
            ChangedFolder::Created(folder) => folder,
            ChangedFolder::Deleted(folder) => folder,
        }
    }
}

struct FolderChangelog {
    pub id: String,
    pub drive_id: String,
    pub name: String,
    pub trashed: bool,
    pub parent: Option<String>,
    pub deleted: bool,
}

impl From<FolderChangelog> for ChangedFolder {
    fn from(f: FolderChangelog) -> Self {
        let folder = Folder {
            id: f.id,
            drive_id: f.drive_id,
            name: f.name,
            parent: f.parent,
            trashed: f.trashed,
        };

        match f.deleted {
            true => Self::Created(folder),
            false => Self::Deleted(folder),
        }
    }
}

impl ChangedFolder {
    pub(crate) async fn get_all(drive_id: &str, pool: &Pool) -> Result<Vec<Self>> {
        match sqlx::query_as!(
            FolderChangelog,
            "SELECT * FROM folder_changelog WHERE drive_id = $1",
            drive_id
        )
        .fetch(pool)
        // Turn the FolderChangelog into a ChangedFolder
        .map_ok(|f| f.into())
        .try_collect()
        .await
        {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::warn!("获取文件夹变更日志失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn clear(drive_id: &str, pool: &Pool) -> Result<()> {
        match sqlx::query!("DELETE FROM folder_changelog WHERE drive_id = $1", drive_id)
            .execute(pool)
            .await
        {
            Ok(_) => {
                trace!("cleared folder changelog");
                Ok(())
            },
            Err(e) => {
                tracing::warn!("清除文件夹变更日志失败: {}", e);
                Err(e)
            }
        }
    }
}
