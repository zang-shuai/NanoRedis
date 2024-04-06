use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::convert::Infallible;
use std::num::ParseIntError;
use std::str;
use std::time::Duration;
use nano_redis::connect::Client;
use nano_redis::{DEFAULT_PORT};

#[derive(Parser, Debug)]
#[clap(name = "nano-redis-cli", version, author, about = "Issue Redis commands")]
struct Cli {
    #[clap(subcommand)]
    command: CommandParser,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum CommandParser {
    Ping {
        /// 发给 ping 的信息
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    /// 获取key的值。
    Get {
        key: String,
    },
    /// 设置 key 以保存字符串值。
    Set {
        key: String,
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,
        #[clap(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    Mset {
        datas: Vec<String>
    },
    Mget {
        datas: Vec<String>
    },
    Incr {
        key: String
    },
    Incrby {
        key: String,
        #[clap(default_value_t = 1, value_parser = i64_from_str)]
        value: i64,
    },
    Lpush {
        key: String,
        datas: Vec<String>,
    },
    Rpush {
        key: String,
        datas: Vec<String>,
    },
    Lpop {
        key: String,
    },
    Rpop {
        key: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> nano_redis::Result<()> {
    // 开启日志
    tracing_subscriber::fmt::try_init()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取要连接的远程地址
    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = Client::connect(&addr).await?;

    match cli.command {
        CommandParser::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        CommandParser::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("{}", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        CommandParser::Set { key, value, expires } => {
            client.set(&key, value, expires).await?;
            println!("OK");
        }
        CommandParser::Mset { datas } => {
            client.mset(&datas).await?;
            println!("OK");
        }
        CommandParser::Mget { datas } => {
            if let Some(value) = client.mget(&datas).await? {
                let data_str = str::from_utf8(&value).unwrap().trim_end_matches(',');
                for (i, item) in data_str.split(',').enumerate() {
                    println!("{:?}:\"{}\"\t", datas[i], item);
                }
            } else {
                println!("(nil)");
            }
        }
        CommandParser::Incrby { key, value } => {
            client.incrby(&key, value).await?;
            println!("OK");
        }
        CommandParser::Incr { key } => {
            client.incrby(&key, 1).await?;
            println!("OK");
        }
        CommandParser::Lpush { key, datas } => {
            client.push(&key, datas, false).await?;
            println!("OK");
        }
        CommandParser::Rpush { key, datas } => {
            client.push(&key, datas, true).await?;
            println!("OK");
        }
        CommandParser::Lpop { key } => {
            if let Some(value) = client.pop(&key, false).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        CommandParser::Rpop { key } => {
            if let Some(value) = client.pop(&key, true).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}

fn i32_from_str(src: &str) -> Result<i32, ParseIntError> {
    // Ok(Bytes::from(src.to_string()));
    src.parse::<i32>()
}

fn i64_from_str(src: &str) -> Result<i64, ParseIntError> {
    // Ok(Bytes::from(src.to_string()));
    src.parse::<i64>()
}