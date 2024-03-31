use std::{
    collections::HashMap,
    fmt,
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
    // thread::JoinHandle,
};

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
    fn try_extract(&self) -> Option<&'a str> {
        match self {
            Self::SimpleString(s) => Some(s),
            Self::BulkString(s) => *s,
            _ => None,
        }
    }
}

pub enum RESPCommand<'a> {
    Ping(Option<&'a str>),
    Echo(&'a str),
    Set,
    Get(Option<String>),
}

impl<'a> FromStr for RESPCommand<'a> {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("RESPCommand FromStr {s}");
        if s.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Payload empty"));
        };

        match s.split_once(' ') {
            Some((hd, tl)) => RESPCommand::match_command_with_payload(hd, tl),
            None => RESPCommand::match_command(s),
        }
    }
}

impl<'a> TryFrom<&[u8]> for RESPCommand<'a> {
    type Error = io::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        RESPCommand::from_str(&value.iter().map(|byte| *byte as char).collect::<String>())
    }
}

impl<'a> TryFrom<Vec<u8>> for RESPCommand<'a> {
    type Error = io::Error;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        RESPCommand::try_from(value.as_slice())
    }
}

impl fmt::Display for RESPCommand<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RESPCommand::*;
        let s = match self {
            Ping(Some(_payload)) => todo!(),
            Ping(None) => DataType::SimpleString("PONG"),
            Echo(s) => DataType::BulkString(Some(s)),
            Set => DataType::SimpleString("OK"),
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

impl<'a> RESPCommand<'a> {
    fn match_command_with_payload<'b>(
        _command: &'b str,
        _payload: &'b str,
    ) -> Result<Self, io::Error> {
        todo!()
    }
    fn match_command(command: &str) -> Result<RESPCommand<'a>, io::Error> {
        match command {
            "PING" | "ping" => Ok(RESPCommand::Ping(None)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, command)),
        }
    }
}

fn handle_incoming(
    mut stream: TcpStream,
    db_arc: Arc<RwLock<HashMap<String, String>>>,
) -> io::Result<()> {
    loop {
        println!("accepted new connection");
        let mut buf = [0; 1024];
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        println!("read {bytes_read} bytes");
        let s: String = buf[0..bytes_read]
            .iter()
            .map(|byte| *byte as char)
            .collect();
        // let match_opt = |data: &RESPData<'_>| match data {
        //     RESPData::BulkStr(s) | RESPData::Str(s) => RESPCommand::from_str(s).ok(),
        //     _ => None,
        // };
        let s_s = s.as_str();
        let data = DataType::try_from(s_s)?;
        println!("Parsed: {data:?}");
        use DataType::*;
        let commands: Vec<RESPCommand> = match data {
            BulkString(None) => vec![],
            BulkString(Some(s)) | DataType::SimpleString(s) => vec![RESPCommand::from_str(s)]
                .into_iter()
                .filter_map(|r| r.ok())
                .collect(),
            DataType::Array(elts) => {
                println!("Parsing array");
                let mut commands = vec![];
                let mut elt_iter = elts.iter();
                while let Some(elt) = elt_iter.next() {
                    let command_opt = match elt {
                        DataType::SimpleString(s) | DataType::BulkString(Some(s)) => match s {
                            &"ECHO" | &"echo" => {
                                elt_iter.next().and_then(|payload| match payload {
                                    DataType::SimpleString(to_echo)
                                    | DataType::BulkString(Some(to_echo)) => {
                                        Some(RESPCommand::Echo(to_echo))
                                    }
                                    _ => None,
                                })
                            }
                            &"PING" | &"ping" => Some(RESPCommand::Ping(elt_iter.next().and_then(
                                |elt| match elt {
                                    DataType::SimpleString(to_ping) => Some(*to_ping),
                                    DataType::BulkString(to_ping) => *to_ping,
                                    _ => None,
                                },
                            ))),
                            &"SET" | &"set" => {
                                elt_iter.next().and_then(|k| {
                                    match (
                                        k.try_extract(),
                                        elt_iter.next().and_then(DataType::try_extract),
                                    ) {
                                        (Some(k), Some(v)) => {
                                            let mut rw_guard = db_arc.write().unwrap();
                                            rw_guard.insert(k.into(), v.into());
                                            Some(RESPCommand::Set)
                                        }
                                        _ => None,
                                    }
                                })
                                // if let Some(x) = elt_iter.next().map(|k| (match k {
                                //     RESPData::Str(s) | RESPData::BulkStr(s) => Some(*s),
                                //     _ => None
                                //     }, elt_iter.next().map(|v| match v {
                                //     RESPData::Str(s) | RESPData::BulkStr(s) => Some(*s),
                                //     _ => None
                                // })) {
                                //     todo!()
                                // };
                            }
                            &"GET" | &"get" => {
                                elt_iter.next().and_then(DataType::try_extract).map(|k| {
                                    let guard = db_arc.read().unwrap();
                                    RESPCommand::Get(guard.get(k).cloned())
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

fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

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
