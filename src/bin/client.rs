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
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        CommandParser::Set { key, value, expires } => {
            client.set(&key, value,expires).await?;
            println!("OK");
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
