use crate::entity::{Db, Frame, Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

#[derive(Debug)]
pub struct Sismember {
    key: String,
    value: String,
}

impl Sismember {
    pub fn new(key: impl ToString, value: String) -> Sismember {
        Sismember {
            key: key.to_string(),
            value,
        }
    }

    // 将命令后面的参数转换为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sismember> {
        // 获取 key
        let key = parse.next_string()?;
        // 获取 value
        let value = parse.next_string()?;
        Ok(Sismember { key, value })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.sismember(self.key.clone(),self.value.clone()) {
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
        frame.push_bulk(Bytes::from("sismember".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.value.into_bytes()));
        frame
    }
}
