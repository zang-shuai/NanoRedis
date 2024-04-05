use crate::entity::{Db, Frame, Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::connect::Connection;
use crate::utils::serialization::{bytes_to_i64, i64_to_bytes};

#[derive(Debug)]
pub struct Incrby {
    key: String,
    value: i64,
}

impl Incrby {
    pub fn new(key: impl ToString, value: i64) -> Incrby {
        Incrby {
            key: key.to_string(),
            value,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
    // 将命令后面的参数转换为命令对象
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Incrby> {
        // 获取 key
        let key = parse.next_string()?;

        // 获取 value
        let value = parse.next_i64()?;

        // println!("{:?}", &i as &[u8]);


        // let value = bytes_to_i64(i).unwrap();

        Ok(Incrby { key, value })
    }

    // 应用相关命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.incrby(self.key, self.value);
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // 命令封装成帧
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("incrby".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_i64(self.value);
        frame
    }
}
