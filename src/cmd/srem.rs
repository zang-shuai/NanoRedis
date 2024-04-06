use crate::entity::{Db, Frame, Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

// 移除set中的指定元素
#[derive(Debug)]
pub struct Srem {
    key: String,
    datas: Vec<String>,
}

impl Srem {
    pub fn new(key: impl ToString, datas: Vec<String>) -> Srem {
        Srem {
            key: key.to_string(),
            datas,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    // 将frame转为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srem> {
        // 获取 key
        let key = parse.next_string()?;

        // 获取 value
        let len = parse.next_u64()?;

        let mut datas = vec![];
        for _ in 0..len {
            datas.push(parse.next_string()?);
        }
        Ok(Srem { key, datas })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.srem(&self.key, self.datas) {
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

    // 命令封装成帧
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("srem".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        let len = self.datas.len() as u64;
        frame.push_u64(len);
        for v in self.datas {
            frame.push_bulk(Bytes::from(v));
        }
        frame
    }
}
