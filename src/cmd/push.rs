use crate::entity::{Db, Frame, Parse};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;

#[derive(Debug)]
pub struct Push {
    key: String,
    value: Vec<String>,
    right: bool,
}

impl Push {
    pub fn new(key: impl ToString, value: Vec<String>, right: bool) -> Push {
        Push {
            key: key.to_string(),
            value,
            right,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Vec<String> {
        &self.value
    }

    pub fn expire(&self) -> Option<Duration> {
        None
    }

    // 将frame转为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Push> {
        // 获取 key
        let key = parse.next_string()?;

        // 获取 value
        let lens = parse.next_i64()?;
        let (len, right) = if lens > 0 {
            (lens as u64, true)
        } else {
            ((-lens) as u64, false)
        };
        let mut value = vec![];
        for _ in 0..len {
            value.push(parse.next_string()?);
        }
        Ok(Push { key, value, right })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.push(self.key, self.value, self.right);
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // 命令封装成帧
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("push".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        let mut len: i64 = self.value.len() as i64;
        if !self.right {
            len = -len;
        }
        frame.push_i64(len);
        for v in self.value {
            frame.push_bulk(Bytes::from(v));
        }
        frame
    }
}
