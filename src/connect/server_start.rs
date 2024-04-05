use crate::entity::{Db, DbDropGuard};
use crate::connect::{Connection, Shutdown};
use crate::cmd::{Command};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// 服务器侦听器状态。在“run”调用中创建。它包括一个"run"方法
#[derive(Debug)]
pub struct Listener {
    // 数据库
    db_holder: DbDropGuard,

    // tcp 监听器
    listener: TcpListener,

    // 限制最大连接数（信号量机制）
    limit_connections: Arc<Semaphore>,

    // 向所有活动连接广播关闭信号。
    notify_shutdown: broadcast::Sender<()>,
    /// 用作正常关闭进程的一部分，以等待客户端连接完成处理。
    /// 一旦所有的“关闭”handle超出范围，Tokio通道将关闭。
    /// 当一个信道关闭时，接收器接收到“无”。
    /// 这用于检测所有连接处理程序的完成。
    /// 当一个连接处理程序被初始化时，它被分配一个`shown_complete_tx`的克隆
    /// 。当侦听器关闭时，它会删除这个`shown_complete_tx`字段所持有的发送器。
    /// 一旦所有的处理程序任务完成，所有的`UNC '克隆也将被删除。
    /// 这会导致`shoot_complete_config.recv（）`以`None`完成。此时，退出服务器进程是安全的。
    shutdown_complete_tx: mpsc::Sender<()>,
}

// 每个连接处理程序。读取来自"connection"的请求并将命令应用到"db"。
#[derive(Debug)]
pub struct Handler {
    // 数据库
    db: Db,

    // 连接
    connection: Connection,

    // 关闭
    shutdown: Shutdown,

    // 不直接使用
    _shutdown_complete: mpsc::Sender<()>,
}

// 最大连接数
const MAX_CONNECTIONS: usize = 250;

// 运行
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 广播一个关闭信息
    let (notify_shutdown, _) = broadcast::channel(1);
    // 多生产，单接收（客户端回复可以关闭）
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // 初始化监听器
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 检测能否连接，拷贝连接指针，获得所有权，
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // 获取 tcpstream
            let socket = self.accept().await?;

            // 为每个连接创建一个 handler
            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // 一旦所有克隆被丢弃，通知接收器一半
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 生成一个新任务来处理连接
            tokio::spawn(async move {
                // 处理连接诶
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                // 删除锁
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // 一直循环获取
        loop {
            // 如果获取到，则返回 stream
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }
            // 暂停backoff秒，暂停时间随着循环翻倍
            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 只要没有收到关闭信号，则循环
        while !self.shutdown.is_shutdown() {
            // 读取请求帧和关闭信号，返回读取到的东西
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            // 获取帧
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // 将帧封装成命令
            let cmd = Command::from_frame(frame)?;
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```记录日志
            debug!(?cmd);

            // 执行应用命令所需的工作。这可能会导致数据库状态发生变化。
            // 连接被传递到apply函数，允许命令将响应帧直接写入连接。
            // 在pub/sub的情况下，可以将多个帧发送回对等体。
            // 服务端执行命令
            cmd.apply(&self.db, &mut self.connection).await?;
        }

        Ok(())
    }
}
