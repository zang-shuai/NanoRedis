use crate::entity::{Db, Frame, Parse};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

#[derive(Debug)]
pub struct Sadd {
    key: String,
    datas: Vec<String>,
}

impl Sadd {
    pub fn new(key: impl ToString, datas: Vec<String>) -> Sadd {
        Sadd {
            key: key.to_string(),
            datas,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    // 将frame转为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sadd> {
        // 获取 key
        let key = parse.next_string()?;

        // 获取 value
        let len = parse.next_u64()?;

        let mut datas = vec![];
        for _ in 0..len {
            datas.push(parse.next_string()?);
        }
        Ok(Sadd { key, datas })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.sadd(self.key, self.datas);
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // 命令封装成帧
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("sadd".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        let len = self.datas.len() as u64;
        frame.push_u64(len);
        for v in self.datas {
            frame.push_bulk(Bytes::from(v));
        }
        frame
    }
}
