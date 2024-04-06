use tokio::sync::{Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, LinkedList};
use std::io::Read;
// use std::str::Bytes;
use std::sync::{Arc, Mutex};
use tracing::debug;
use crate::utils::serialization::{bytes_to_i64, i64_to_bytes};

// `Db`的包装类。为了允许有序地清理"Db"，当这个结构被丢弃时，通过信号通知后台清除任务关闭系统
#[derive(Debug)]
pub struct DbDropGuard {
    /// 删除此"DbHolder"结构时将关闭的"Db"实例。
    db: Db,
}

#[derive(Debug, Clone)]
pub struct Db {
    // 数据库中有多个共享指针
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    // 标准 std 互斥锁包裹 state，不用 tokio 下的锁（原因略）
    state: Mutex<State>,

    // 通知后台任务处理条目过期。后台任务等待通知，然后检查过期值或关机信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    // 存储数据
    entries: HashMap<String, Entry>,

    // pub 与 sub 的存储，可以不断进行订阅，广播
    // pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 跟踪键的TTL（网络生存时间）。
    /// 这就允许后台任务对这个映射进行迭代，以找到下一个到期的值。
    /// 同一瞬间创建多个条目是可能的，因此，“Instant”对于key来说是不够的。一个唯一的键（`String`）用于打破这些束缚。
    expirations: BTreeSet<(Instant, String)>,

    // db关闭时为True。当所有的"Db"值都被 drop 时。将其设置为"true"，则向后台任务发出退出的信号。
    shutdown: bool,
}

// 数据条目
#[derive(Debug)]
struct Entry {
    // byte 数据
    data: DbData,

    // 条目过期时，应该从数据库中删除。
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone)]
enum DbData {
    String(Bytes),
    List(LinkedList<Bytes>),
    Set(BTreeSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
}

// 新建和获取数据库指针
impl DbDropGuard {
    // 新建
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    // 返回一个数据库的指针
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

// 实现 drop 实例
impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 向'Db'实例发出信号以关闭
        self.db.shutdown_purge_task();
    }
}

impl Db {
    // 创建一个新的`Db`实例
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 启动后台任务
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    // 获取 key 的值
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 数据浅拷贝出去
        let state = self.shared.state.lock().unwrap();
        let option = state.entries.get(key).map(|entry| entry.data.clone()).unwrap();
        match option {
            DbData::String(v) => {
                Some(v)
            }
            DbData::List(_) => { None }
            DbData::Set(_) => { None }
            DbData::Hash(_) => { None }
        }
    }

    // 设置键值，以及可选的过期持续时间。如果存在该键，则会先删除在插入。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        // 如果这个`set`成为下一个过期的密钥**，则需要通知后台任务，以便它可以更新其状态。是否需要通知任务是在"set"例程期间计算的。
        let mut notify = false;
        // 获取到期时间
        let expires_at = expire.map(|duration| {
            // 设置到期时间
            let when = Instant::now() + duration;
            // 只有当新插入的expiration是下一个要删除的键时，才通知辅助任务。在这种情况下，需要唤醒worker以更新其状态。
            // 查看树中的第一个结点（最小结点），是否大于当前节点的存在时间
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // 将值插入哈希表中
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: DbData::String(value),
                expires_at,
            },
        );
        // 如果键已经存在。则删除
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // 删除 expiration
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // 插入键值对到树中
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }
        // 释放互斥锁
        drop(state);

        if notify {
            // 激活 notified(需要删除节点)
            self.shared.background_task.notify_one();
        }
    }


    pub(crate) fn incrby(&self, key: String, value: i64) -> Option<Bytes> {
        let mut state = self.shared.state.lock().unwrap();
        match state.entries.get_mut(&key) {
            None => {}
            Some(v) => {
                match &mut v.data {
                    DbData::String(serde_derive) => {
                        let int = bytes_to_i64(serde_derive.clone()).unwrap();

                        *serde_derive = Bytes::from((int + value).to_string());
                    }
                    DbData::List(_) => {}
                    DbData::Set(_) => {}
                    DbData::Hash(_) => {}
                }
            }
        }
        let option = match state.entries.get_mut(&key).map(|entry| entry.data.clone()) {
            None => {
                Some(Bytes::from("error"))
            }
            Some(ref mut data) => {
                *data = match data {
                    DbData::String(serde_derive) => {
                        let int = bytes_to_i64(serde_derive.clone()).unwrap();
                        // *serde_derive = bytes.clone();
                        // data.data = DbData::String(bytes.clone());
                        println!("{}", int + value);
                        println!("{:?}", DbData::String(Bytes::from((int + value).to_string())));
                        DbData::String(Bytes::from((int + value).to_string()))
                        // Some(bytes.clone())
                    }
                    _ => {
                        DbData::String(Bytes::from("error".to_string()))
                    }
                };
                println!("{:?}", *data);
                Some(Bytes::from("OK"))
            }
        };
        drop(state);
        return option;
    }
    pub(crate) fn push(&self, key: String, value: Vec<String>, right: bool) {
        let mut state = self.shared.state.lock().unwrap();

        let option = match state.entries.get_mut(&key) {
            None => {
                // 将值插入哈希表中
                let expire = None;
                // 如果这个`set`成为下一个过期的密钥**，则需要通知后台任务，以便它可以更新其状态。是否需要通知任务是在"set"例程期间计算的。
                let mut notify = false;
                // 获取到期时间
                let expires_at = expire.map(|duration| {
                    // 设置到期时间
                    let when = Instant::now() + duration;
                    // 只有当新插入的expiration是下一个要删除的键时，才通知辅助任务。在这种情况下，需要唤醒worker以更新其状态。
                    // 查看树中的第一个结点（最小结点），是否大于当前节点的存在时间
                    notify = state
                        .next_expiration()
                        .map(|expiration| expiration > when)
                        .unwrap_or(true);

                    when
                });
                let linked_list: LinkedList<Bytes> = if right {
                    value.into_iter().map(|str| Bytes::from(str)).collect()
                } else {
                    value.into_iter().map(|str| Bytes::from(str)).rev().collect()
                };

                let prev = state.entries.insert(
                    key.clone(),
                    Entry {
                        data: DbData::List(linked_list),
                        expires_at,
                    },
                );
                // // 如果键已经存在。则删除
                if let Some(prev) = prev {
                    if let Some(when) = prev.expires_at {
                        // 删除 expiration
                        state.expirations.remove(&(when, key.clone()));
                    }
                }
                // 插入键值对到树中
                if let Some(when) = expires_at {
                    state.expirations.insert((when, key));
                }
                // 释放互斥锁
                if notify {
                    // 激活 notified(需要删除节点)
                    self.shared.background_task.notify_one();
                }
                Some(Bytes::from("error"));
            }
            Some(data) => {
                let dbdata = &mut data.data;
                if right {
                    if let DbData::List(ref mut l) = dbdata {
                        for v in value {
                            l.push_back(Bytes::from(v));
                        }
                    }
                } else {
                    if let DbData::List(ref mut l) = dbdata {
                        for v in value {
                            l.push_front(Bytes::from(v));
                        }
                    }
                }
                // let int = bytes_to_i64(bytes1.clone()).unwrap();
                // let bytes = Bytes::from((int + value).to_string()).clone();
                // data.data = DbData::String(bytes.clone());
                // Some(bytes.clone())
            }
        };
        drop(state);
    }

    // 关闭信号
    fn shutdown_purge_task(&self) {
        // 删除state，通知删除树，shotdown
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    // 取消所有过期的密钥，并返回下一个密钥将过期的"Instant"。后台任务将休眠，直到此时。返回 None 表示数据库为空
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // 数据库正在关闭。共享指针都已经删除。后台任务退出。
            return None;
        }
        // `lock（）`返回一个`MutexGuard`而不是`& mut State`。
        // 借用检查器无法"穿透"互斥保护，
        // 所以我们在循环外得到一个对`State`的"真正"可变引用。
        let state = &mut *state;

        // 查找此前计划过期的所有密钥。

        // 获取当前时间
        let now = Instant::now();
        // 遍历这个二叉树，当when>now时，返回，否则删除
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // 由于二叉树有序，因此返回
                return Some(when);
            }
            // 删除数据库中的值，同时删除树中的值
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    // 返回是否关闭
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// 后台任务执行的过程
///
/// 等待通知。收到通知后，从共享状态句柄中清除所有过期的密钥。如果设置了"shoot"，则终止任务。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果设置了关闭标志，则任务应退出。
    while !shared.is_shutdown() {
        // 删除所有过期的密钥。该函数返回下一个密钥到期的时刻
        if let Some(when) = shared.purge_expired_keys() {
            // 等待直到下一个密钥过期或直到后台任务收到通知。
            // 如果任务收到通知，则它必须重新加载其状态，因为新密钥已设置为提前过期。
            // 这是通过循环来完成的。
            tokio::select! {
                // 睡眠到此
                _ = time::sleep_until(when) => {}
                // 等通知
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 未来没有到期的钥匙。等待任务通知。
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}
