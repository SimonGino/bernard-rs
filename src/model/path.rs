use std::collections::HashMap;
use std::path::PathBuf;

use crate::database::Pool;
use futures::prelude::*;

#[derive(Debug)]
pub enum Path {
    File(InnerPath),
    Folder(InnerPath),
}

impl Path {
    pub fn trashed(&self) -> bool {
        match self {
            Self::File(inner) => inner.trashed,
            Self::Folder(inner) => inner.trashed,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct InnerPath {
    pub id: String,
    pub drive_id: String,
    pub path: PathBuf,
    pub trashed: bool,
}

#[derive(Debug)]
pub enum ChangedPath {
    Created(Path),
    Deleted(Path),
}

impl From<ChangedPath> for Path {
    fn from(path: ChangedPath) -> Self {
        match path {
            ChangedPath::Created(path) => path,
            ChangedPath::Deleted(path) => path,
        }
    }
}

impl From<ChangedPath> for InnerPath {
    fn from(path: ChangedPath) -> Self {
        match path {
            ChangedPath::Created(path) => path.into(),
            ChangedPath::Deleted(path) => path.into(),
        }
    }
}

impl From<Path> for InnerPath {
    fn from(path: Path) -> Self {
        match path {
            Path::File(inner) => inner,
            Path::Folder(inner) => inner,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PathChangelog {
    pub id: String,
    pub drive_id: String,
    pub path: String,
    pub folder: bool,
    pub deleted: bool,
    pub trashed: bool,
}

impl From<PathChangelog> for Path {
    fn from(p: PathChangelog) -> Self {
        let inner_path = InnerPath {
            id: p.id,
            drive_id: p.drive_id,
            path: p.path.into(),
            trashed: p.trashed,
        };

        match p.folder {
            true => Path::Folder(inner_path),
            false => Path::File(inner_path),
        }
    }
}

impl From<PathChangelog> for ChangedPath {
    fn from(path: PathChangelog) -> Self {
        match path.deleted {
            true => Self::Deleted(path.into()),
            false => Self::Created(path.into()),
        }
    }
}

impl ChangedPath {
    pub(crate) async fn get_all(drive_id: &str, pool: &Pool) -> sqlx::Result<Vec<Self>> {
        let path_changelogs: Vec<PathChangelog> = match sqlx::query_as::<_, PathChangelog>(
            "SELECT * FROM path_changelog WHERE drive_id = $1"
        )
            .bind(drive_id)
            .fetch_all(pool)
            .await
        {
            Ok(changelogs) => changelogs,
            Err(e) => {
                tracing::warn!("获取路径变更日志失败: {}", e);
                return Err(e);
            }
        };

        // 使用 HashMap 来去除重复项，保留最新的变更
        let mut unique_changes: HashMap<String, PathChangelog> = HashMap::new();
        for changelog in path_changelogs {
            unique_changes.insert(changelog.path.clone(), changelog);
        }

        // 转换为 ChangedPath 并收集结果
        Ok(unique_changes.into_values()
            .map(|p| p.into())
            .collect())
    }
}
