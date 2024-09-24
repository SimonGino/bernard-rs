use crate::database::{Connection, Pool};

#[derive(Debug)]
pub struct Drive {
    pub id: String,
    pub page_token: String,
}

impl Drive {
    pub(crate) async fn create(
        id: &str,
        page_token: &str,
        conn: &mut Connection,
    ) -> sqlx::Result<()> {
        match sqlx::query!(
            "INSERT INTO drives (id, page_token) VALUES ($1, $2)",
            id,
            page_token
        )
        .execute(conn)
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!("创建驱动器失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn get_by_id(id: &str, pool: &Pool) -> sqlx::Result<Option<Self>> {
        match sqlx::query_as!(Self, "SELECT * FROM drives WHERE id = $1", id)
            .fetch_optional(pool)
            .await
        {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::warn!("获取驱动器失败: {}", e);
                Err(e)
            }
        }
    }

    pub(crate) async fn update_page_token(
        id: &str,
        page_token: &str,
        conn: &mut Connection,
    ) -> sqlx::Result<()> {
        match sqlx::query!(
            "UPDATE drives SET page_token = $2 WHERE id = $1",
            id,
            page_token,
        )
        .execute(conn)
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!("更新驱动器页面令牌失败: {}", e);
                Err(e)
            }
        }
    }
}
