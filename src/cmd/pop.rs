use bytes::Bytes;
use tracing::{debug, instrument};
use crate::connect::Connection;
use crate::entity::{Frame, Db, Parse};

/// 获取key的值。
/// 如果键不存在，则返回特殊值nil。
/// 如果key中存储的值不是字符串，则返回一个错误，因为GET只处理字符串值。
#[derive(Debug)]
pub struct Pop {
    /// 要获取的 key
    key: String,
    right: bool,
}

impl Pop {
    // 利用 key 创建一个新的`Pop`命令
    pub fn new(key: impl ToString,right:bool) -> Pop {
        Pop {
            key: key.to_string(),
            right,
        }
    }

    // 获取 key
    pub fn key(&self) -> &str {
        &self.key
    }


    // 将 parse 转为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pop> {
        // 获取 get 后面那个帧（即key）
        let key = parse.next_string()?;
        let u = parse.next_u64()?;
        let right = if u == 0 { false } else { true };
        Ok(Pop { key, right })
    }
    // 将命令用于 db 数据中
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.pop(&self.key, self.right) {
            // 找到命令，返回Bulk
            Frame::Bulk(value)
        } else {
            // 没有找到命令
            Frame::Null
        };
        debug!(?response);
        // 将找到的值返回
        dst.write_frame(&response).await?;
        Ok(())
    }

    // 将输入的命令封装为Frame
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("pop".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        let u = if self.right { 1 } else { 0 };
        frame.push_u64(u);
        frame
    }
}
