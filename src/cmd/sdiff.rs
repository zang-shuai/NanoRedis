use crate::entity::{Db, Frame, Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

#[derive(Debug)]
pub struct Sdiff {
    keys: Vec<String>,
}

impl Sdiff {
    pub fn new(keys: Vec<String>) -> Sdiff {
        Sdiff {
            keys,
        }
    }

    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    // 将命令后面的参数转换为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sdiff> {
        // 获取 key
        let len = parse.next_u64()?;
        let mut keys = Vec::new();
        for _ in 0..len {
            keys.push(parse.next_string()?);
        }
        Ok(Sdiff { keys })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.sdiff(self.keys) {
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
        frame.push_bulk(Bytes::from("sdiff".as_bytes()));
        frame.push_u64(self.keys.len() as u64);
        for key in self.keys {
            frame.push_bulk(Bytes::from(key));
        }
        frame
    }
}
