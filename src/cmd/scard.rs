use crate::entity::{Db, Frame, Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

//  返回set中元素的个数
#[derive(Debug)]
pub struct Scard {
    key: String,
}

impl Scard {
    pub fn new(key: String) -> Scard {
        Scard {
            key
        }
    }

    // 将命令后面的参数转换为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scard> {
        // 获取 key
        let key = parse.next_string()?;
        Ok(Scard { key })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 获取值
        let response = if let Some(value) = db.scard(self.key) {
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
        frame.push_bulk(Bytes::from("scard".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
