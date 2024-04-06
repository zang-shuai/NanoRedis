use bytes::Bytes;
use tracing::{debug, instrument};
use crate::connect::Connection;
use crate::entity::{Frame, Db, Parse};

/// 获取key的值。
/// 如果键不存在，则返回特殊值nil。
/// 如果key中存储的值不是字符串，则返回一个错误，因为GET只处理字符串值。
#[derive(Debug)]
pub struct Lrange {
    /// 要获取的 key
    key: String,
    start: u64,
    end: u64,
}

impl Lrange {
    // 利用 key 创建一个新的`Lrange`命令
    pub fn new(key: impl ToString, start: u64, end: u64) -> Lrange {
        Lrange {
            key: key.to_string(),
            start,
            end,
        }
    }

    // 获取 key
    pub fn key(&self) -> &str {
        &self.key
    }


    // 将 parse 转为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lrange> {
        let key = parse.next_string()?;
        let start = parse.next_u64()?;
        let end = parse.next_u64()?;

        Ok(Lrange { key, start, end })
    }
    // 将命令用于 db 数据中
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.lrange(&self.key,self.start,self.end) {
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
        frame.push_bulk(Bytes::from("lrange".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_u64(self.start);
        frame.push_u64(self.end);
        frame
    }
}
