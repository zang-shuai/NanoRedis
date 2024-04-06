//! mini 客户端

use crate::cmd::{Get, Incrby, Ping, Pop, Push, Set};
use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
// use tokio::time::error::Error;
use tracing::{debug, instrument};
use crate::connect::{Connection};
use crate::entity::Frame;
use crate::entity::Frame::Error as FrameError;

// 与Redis服务器建立连接。
// 由单个"TcpStream"支持，"Client"提供了基本的网络客户端功能（无池化、重试等）。
// 使用[`connect`]（fn @ connect）函数建立连接。
///
/// 请求是使用"Client"的各种方法发出的。
pub struct Client {
    connection: Connection,
}

// 订阅者收到的消息
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    ///类似于新建
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        // 将 ping 的内容传进来，创建对象，再封装成帧
        let frame = Ping::new(msg).into_frame();
        // 输出日志
        debug!(request = ?frame);
        // 连接，将帧写入tcpstream 中
        self.connection.write_frame(&frame).await?;
        // 读取相应
        match self.read_response().await? {
            // 解开相应的帧，返回回去
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 将 key 封装成对象，再封装成帧
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        // 将帧写入 tcpstream
        self.connection.write_frame(&frame).await?;

        // 等待响应，将响应帧解开返回
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }
    #[instrument(skip(self))]
    pub async fn mget(&mut self, keys: &Vec<String>) -> crate::Result<Option<Bytes>> {
        let mut res = BytesMut::new();
        for key in keys {
            let frame = Get::new(key).into_frame();
            debug!(request = ?frame);
            self.connection.write_frame(&frame).await?;
            match self.read_response().await? {
                Frame::Simple(value) => {
                    res.extend_from_slice(&Bytes::from(value));
                    res.extend(b",");
                }
                Frame::Bulk(value) => {
                    res.extend(value);
                    res.extend(b",");
                }
                Frame::Null => {
                    res.extend(b"None,");
                }
                frame => { return Err(frame.to_error()); }
            };
        }
        Ok(Some(Bytes::from(res)))
    }

    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes, expiration: Option<Duration>) -> crate::Result<()> {
        let cmd = Set::new(key, value, expiration);
        let frame = cmd.into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }
    #[instrument(skip(self))]
    pub async fn mset(&mut self, datas: &Vec<String>) -> crate::Result<()> {
        let l = datas.len();
        if l & 1 == 1 {
            return Err(FrameError("Wrong number of parameters".to_string()).to_error());
        }
        let mut i = 0;
        while i < l {
            self.set(datas[i].as_str(), Bytes::from(datas[i + 1].clone()), None).await?;
            i += 2;
        }
        Ok(())
    }
    #[instrument(skip(self))]
    pub async fn incrby(&mut self, key: &str, value: i64) -> crate::Result<()> {
        let cmd = Incrby::new(key, value);
        let frame = cmd.into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }
    #[instrument(skip(self))]
    pub async fn push(&mut self, key: &str, value: Vec<String>,right:bool) -> crate::Result<()> {
        let cmd = Push::new(key, value,right);
        let frame = cmd.into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }
    #[instrument(skip(self))]
    pub async fn pop(&mut self, key: &str, right:bool) -> crate::Result<Option<Bytes>> {
        let cmd = Pop::new(key, right);
        let frame = cmd.into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }
    /// 读取响应帧
    async fn read_response(&mut self) -> crate::Result<Frame> {
        // 获取服务端的相应
        let response = self.connection.read_frame().await?;
        debug!(?response);
        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}