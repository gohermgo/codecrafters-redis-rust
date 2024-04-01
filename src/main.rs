#![allow(clippy::pedantic)]
use std::{
    collections::HashMap,
    env, fmt,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    num::ParseIntError,
    str::FromStr,
    sync::{
        // mpsc::{self, Receiver, Sender},
        Arc,
        // Mutex,
        RwLock,
    },
    time::{Duration, Instant},
    vec::IntoIter,
};

#[derive(Debug)]
pub enum DataType<'a> {
    SimpleString(&'a str),
    BulkString(Option<&'a str>),
    Array(Vec<DataType<'a>>),
}

impl fmt::Display for DataType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DataType::*;
        match self {
            SimpleString(payload) => f.write_fmt(format_args!("+{}\r\n", payload)),
            BulkString(Some(elt)) => {
                f.write_fmt(format_args!("${}\r\n{}\r\n", elt.as_bytes().len(), elt))
            }
            BulkString(None) => f.write_str("$-1\r\n"),
            Array(elts) => f.write_str(
                elts.iter()
                    .fold(format!("*{}\r\n", elts.len()), |acc, elt| {
                        format!("{}{}", acc, elt)
                    })
                    .as_str(),
            ),
        }
    }
}

impl<'a> TryFrom<&'a str> for DataType<'a> {
    type Error = io::Error;
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        use io::ErrorKind::InvalidData;
        use DataType::*;
        let organize_split = |(hd, tl): (&'a str, &'a str)| {
            let (prefix, hd) = hd.split_at(1);
            (prefix, hd, tl)
        };
        match value.split_once("\r\n").map(organize_split) {
            Some(("*", count, mut tl)) => {
                let count: usize = count
                    .parse()
                    .map_err(|_| io::Error::new(InvalidData, "Failed to parse array-count"))?;
                let mut buf = vec![];
                for _ in 0..count {
                    let (segment, remainder) = DataType::chainparse(tl)?;
                    tl = remainder.unwrap_or_default();
                    buf.push(segment);
                }
                Ok(Array(buf))
            }

            Some(("$", len, tl)) => {
                let into_io_error = |e: ParseIntError| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to parse bulk-string length {len} ({:?})", e.kind()),
                    )
                };
                let length_error = |data_type: &str| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid length {len} for {data_type} {tl}"),
                    )
                };
                let try_into_bulk_string = |len: usize| match tl.get(0..len) {
                    Some(content) => Ok(BulkString(Some(content))),
                    None => Err(length_error("bulk-string")),
                };
                let try_into_null_bulk_string = |len: isize| match len {
                    -1 => Ok(BulkString(None)),
                    _ => Err(length_error("presumed null bulk-string")),
                };
                len.parse()
                    .map_err(into_io_error)
                    .and_then(try_into_bulk_string)
                    .or(len
                        .parse()
                        .map_err(into_io_error)
                        .and_then(try_into_null_bulk_string))
                // let len: usize = len.parse().map_err(|_| {
                //     io::Error::new(
                //         io::ErrorKind::InvalidData,
                //         "Failed to parse bulk-string length",
                //     )
                // })?;
                // Ok(Self::BulkString(tl.get(0..len).unwrap_or_default()))
                // }
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing delimiter",
            )),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown")),
        }
    }
}

// impl<'a> TryFrom<&'a [u8]> for RESPData<'a> {
//     type Error = io::Error;
//     fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
//         RESPData::try_from(
//             &*value
//                 .into_iter()
//                 .map(|byte| *byte as char)
//                 .collect::<String>(),
//         )
//     }
// }

impl<'a> DataType<'a> {
    fn chainparse(s: &'a str) -> io::Result<(Self, Option<&str>)> {
        let segment = Self::try_from(s)?;
        match s.split_once(segment.to_string().as_str()) {
            Some((_, tl)) => Ok((segment, Some(tl))),
            None => Ok((segment, None)),
        }
    }
    #[allow(dead_code)]
    fn try_extract(&self) -> Option<&'a str> {
        match self {
            Self::SimpleString(s) => Some(s),
            Self::BulkString(s) => *s,
            _ => None,
        }
    }
    fn try_take(self) -> Option<&'a str> {
        match self {
            Self::SimpleString(s) => Some(s),
            Self::BulkString(s) => s,
            _ => None,
        }
    }
}

pub enum Command<'a> {
    Ping(Option<&'a str>),
    Echo(&'a str),
    Set,
    Get(Option<String>),
}

impl<'a> FromStr for Command<'a> {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("RESPCommand FromStr {s}");
        if s.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Payload empty"));
        };

        match s.split_once(' ') {
            Some((hd, tl)) => Command::match_command_with_payload(hd, tl),
            None => Command::match_command(s),
        }
    }
}

impl<'a> TryFrom<&[u8]> for Command<'a> {
    type Error = io::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Command::from_str(&value.iter().map(|byte| *byte as char).collect::<String>())
    }
}

impl<'a> TryFrom<Vec<u8>> for Command<'a> {
    type Error = io::Error;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Command::try_from(value.as_slice())
    }
}

impl fmt::Display for Command<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Command::*;
        let s = match self {
            Ping(Some(_payload)) => todo!(),
            Ping(None) => DataType::SimpleString("PONG"),
            Echo(s) => DataType::BulkString(Some(s)),
            Set => DataType::SimpleString("OK"),
            // Set(start, timeout_opt) => match timeout_opt {
            //     None => DataType::SimpleString("OK"),
            //     Some(timeout) if start.elapsed() < *timeout => DataType::SimpleString("OK"),
            //     _ => DataType::BulkString(None),
            // },
            Get(Some(s)) => DataType::BulkString(Some(s.as_str())),
            Get(None) => DataType::BulkString(None),
        };
        f.write_fmt(format_args!("{}", s))
    }
}
pub trait Spawner<'a, T> {
    fn spawn(&'a self) -> io::Result<T>;
}
// impl<'a> Spawner<'a, RedisListener<'a>> for TcpStream {
//     fn spawn(&'a self) -> io::Result<RedisListener<'a>> {
//         let mut buf = [0; 1024];
//         let read = self.read(&mut buf)?;
//         todo!()
//     }
// }
pub trait MutSpawner<'a, T> {
    fn spawn(&'a mut self) -> io::Result<T>;
}
// impl<'a> MutSpawner<'a, RedisListener<'a>> for TcpStream {
//     fn spawn(&'a mut self) -> io::Result<RedisListener<'a>> {
//         let mut buf = [0; 1024];
//         let read = self.read(&mut buf)?;
//         std::str::from_utf8(&buf[0..read])
//             .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{e:?}")))
//             .map(|query| RedisListener {
//                 query,
//                 stream: self,
//             })
//     }
// }
// impl<'a> MutSpawner<'a, RedisServer<'a>> for TcpStream {
//     fn spawn(&'a mut self) -> io::Result<RedisServer<'a>> {
//         let tcp_arc = Arc::new(RwLock::new(*self));
//         let tcp_clone = Arc::clone(&tcp_arc);

//         let (query_tx, query_rx) = mpsc::channel();
//         let (data_tx, data_rx) = mpsc::channel();
//         let listener_thread = std::thread::spawn(|| loop {
//             let query = query_rx
//                 .recv()
//                 .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, format!("{e:?}")))?;
//         });
//         let listener = RedisListener { listener_thread };
//         let tcp_clone = Arc::clone(&tcp_arc);
//         Ok(RedisServer {
//             listener,
//             responder,
//             query_tx,
//             stream,
//         })
//     }
// }
// pub struct RedisServer<'a> {
//     listener: RedisListener,
//     responder: RedisResponder,
//     query_tx: Sender<&'a str>,
//     stream: &'a mut TcpStream,
// }
// pub struct RedisListener {
//     listener_thread: JoinHandle<io::Result<()>>,
// }
// pub struct RedisResponder {
//     thread: JoinHandle<io::Result<()>>,
// }

// pub struct RedisQuery<'a> {
//     data: DataType<'a>,
// }

impl<'a> Command<'a> {
    fn match_command_with_payload<'b>(
        _command: &'b str,
        _payload: &'b str,
    ) -> Result<Self, io::Error> {
        todo!()
    }
    fn match_command(command: &str) -> Result<Command<'a>, io::Error> {
        match command {
            "PING" | "ping" => Ok(Command::Ping(None)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, command)),
        }
    }
}
pub struct MapValueTimer {
    start: Instant,
    timeout: Duration,
}
impl MapValueTimer {
    pub fn new(timeout: Duration) -> Self {
        Self {
            start: Instant::now(),
            timeout,
        }
    }
    fn is_expired(&self) -> bool {
        self.start.elapsed() >= self.timeout
    }
}
pub struct MapValue {
    data: String,
    timer: Option<MapValueTimer>,
}
impl MapValue {
    fn is_expired(&self) -> bool {
        if let Some(timer) = &self.timer {
            timer.is_expired()
        } else {
            false
        }
    }
}
pub struct MapEntry {
    key: String,
    value: MapValue,
}
// Handling of SET logic
impl<'a> TryFrom<&mut IntoIter<DataType<'a>>> for MapEntry {
    type Error = io::Error;
    fn try_from(value: &mut IntoIter<DataType<'a>>) -> Result<Self, Self::Error> {
        let key_val_opt = value.next().and_then(DataType::try_take).and_then(|key| {
            value
                .next()
                .and_then(DataType::try_take)
                .map(|val| (key.to_string(), val.to_string()))
        });

        match key_val_opt {
            Some((key, data)) => {
                let timer = value
                    .next()
                    .and_then(DataType::try_take)
                    .and_then(|contained| {
                        if contained == "px" {
                            value
                                .next()
                                .and_then(DataType::try_take)
                                .and_then(|timeout_str| timeout_str.parse().ok())
                                .map(Duration::from_millis)
                                .map(MapValueTimer::new)
                        } else {
                            None
                        }
                    });

                Ok(MapEntry {
                    key,
                    value: MapValue { data, timer },
                })
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to parse key-value pair",
            )),
        }
    }
}
// type DataMapValue = (String, OptionalTimer);
type DataMap = HashMap<String, MapValue>;
type ThreadSafeDataMap = Arc<RwLock<DataMap>>;
fn handle_incoming(mut stream: TcpStream, db_arc: ThreadSafeDataMap) -> io::Result<()> {
    loop {
        println!("accepted new connection");
        let mut buf = [0; 1024];
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        println!("read {bytes_read} bytes");
        let data = std::str::from_utf8(&buf[0..bytes_read])
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Non-utf8 str received {e:?}"),
                )
            })
            .and_then(DataType::try_from)?;
        println!("Parsed: {data:?}");
        use Command::*;
        use DataType::*;
        let commands: Vec<Command> = match data {
            BulkString(None) => vec![],
            BulkString(Some(s)) | SimpleString(s) => vec![Command::from_str(s)]
                .into_iter()
                .filter_map(|r| r.ok())
                .collect(),
            Array(elts) => {
                println!("Parsing array");
                let mut commands = vec![];
                let mut elt_iter = elts.into_iter();
                while let Some(elt) = elt_iter.next() {
                    let command_opt = match elt {
                        SimpleString(s) | BulkString(Some(s)) => match s {
                            "ECHO" | "echo" => elt_iter.next().and_then(|payload| match payload {
                                SimpleString(to_echo) | BulkString(Some(to_echo)) => {
                                    Some(Echo(to_echo))
                                }
                                _ => None,
                            }),
                            "PING" | "ping" => {
                                Some(Ping(elt_iter.next().and_then(|elt| match elt {
                                    SimpleString(to_ping) => Some(to_ping),
                                    BulkString(to_ping) => to_ping,
                                    _ => None,
                                })))
                            }
                            "SET" | "set" => {
                                let map_entry = MapEntry::try_from(&mut elt_iter)?;
                                {
                                    let mut write_guard = db_arc.write().unwrap();
                                    let k = map_entry.key;
                                    let v = map_entry.value;
                                    write_guard.insert(k, v)
                                };
                                Some(Set)
                            }
                            "GET" | "get" => {
                                elt_iter.next().and_then(DataType::try_take).map(|k| {
                                    let guard = db_arc.read().unwrap();
                                    Get(guard
                                        .get(k)
                                        .and_then(
                                            |v| {
                                                if v.is_expired() {
                                                    None
                                                } else {
                                                    Some(&v.data)
                                                }
                                            },
                                        )
                                        .cloned())
                                })
                            }
                            _ => None,
                        },
                        _ => todo!(),
                    };
                    if let Some(command) = command_opt {
                        commands.push(command);
                    };
                }
                commands
            }
        };
        for command in commands {
            stream.write_all(command.to_string().as_bytes())?;
        }
    }
    Ok(())
}

fn parse_port_argument(mut args: env::Args) -> Option<String> {
    while let Some(arg) = args.next() {
        if arg == *"--port" {
            return args.next();
        }
    }
    None
}

fn main() -> io::Result<()> {
    let arg_iter = env::args();
    let port = parse_port_argument(arg_iter).unwrap_or("6379".into());
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind(format!("{}:{}", "127.0.0.1", port))?;

    let db = HashMap::new();
    let safe_db = RwLock::new(db);
    let thsafe_db = Arc::new(safe_db);

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let db_arc = thsafe_db.clone();
                std::thread::spawn(|| handle_incoming(_stream, db_arc));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
