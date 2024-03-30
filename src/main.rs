use std::{
    fmt,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    str::FromStr,
};

pub enum RESPData<'a> {
    Str(&'a str),
    BulkStr(&'a str),
    Arr(Vec<RESPData<'a>>),
}

impl fmt::Display for RESPData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RESPData::Str(payload) => f.write_fmt(format_args!("+{}\r\n", payload)),
            RESPData::BulkStr(elt) => {
                f.write_fmt(format_args!("${}\r\n{}\r\n", elt.as_bytes().len(), elt))
            }
            RESPData::Arr(_elts) => {
                let mut args = format!("*{}\r\n", _elts.len());
                for elt in _elts {
                    args = format!("{}{}", args, elt);
                }
                f.write_str(args.as_str())
            }
        }
    }
}

impl<'a> TryFrom<&'a str> for RESPData<'a> {
    type Error = io::Error;
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value.split_once("\r\n") {
            Some((hd, mut tl)) => match hd.split_at(1) {
                ("*", count) => {
                    let count: usize = count.parse().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "Failed to parse array-count")
                    })?;
                    let mut buf = vec![];
                    for _ in 0..count {
                        let (segment, remainder) = RESPData::chainparse(tl)?;
                        tl = remainder.unwrap_or_default();
                        buf.push(segment);
                    }
                    Ok(Self::Arr(buf))
                }
                ("$", len) => {
                    let len: usize = len.parse().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Failed to parse bulk-string length",
                        )
                    })?;
                    Ok(Self::BulkStr(tl.get(0..len).unwrap_or_default()))
                }
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown")),
            },
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing delimiter",
            )),
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

impl<'a> RESPData<'a> {
    fn chainparse(s: &'a str) -> io::Result<(Self, Option<&str>)> {
        let segment = Self::try_from(s)?;
        match s.split_once(segment.to_string().as_str()) {
            Some((_, tl)) => Ok((segment, Some(tl))),
            None => Ok((segment, None)),
        }
    }
}

pub enum RESPCommand<'a> {
    Ping(Option<&'a str>),
    Echo(&'a str),
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
        RESPCommand::from_str(&*value.iter().map(|byte| *byte as char).collect::<String>())
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
        match self {
            RESPCommand::Ping(Some(_payload)) => todo!(),
            RESPCommand::Ping(None) => f.write_str(RESPData::Str("PONG").to_string().as_str()),
            RESPCommand::Echo(s) => f.write_str(RESPData::BulkStr(s).to_string().as_str()),
        }
    }
}

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

fn handle_incoming(mut stream: TcpStream) -> io::Result<()> {
    loop {
        println!("accepted new connection");
        let mut buf = [0; 1024];
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        println!("read {bytes_read} bytes");
        let s: String = buf[0..bytes_read]
            .into_iter()
            .map(|byte| *byte as char)
            .collect();
        // let match_opt = |data: &RESPData<'_>| match data {
        //     RESPData::BulkStr(s) | RESPData::Str(s) => RESPCommand::from_str(s).ok(),
        //     _ => None,
        // };
        let data = RESPData::try_from(s.as_str())?;
        println!("Parsed: {data}");
        let commands: Vec<RESPCommand> = match data {
            RESPData::BulkStr(s) | RESPData::Str(s) => vec![RESPCommand::from_str(s)]
                .into_iter()
                .filter_map(|r| r.ok())
                .collect(),
            RESPData::Arr(elts) => {
                let mut commands = vec![];
                let mut elt_iter = elts.iter();
                while let Some(elt) = elt_iter.next() {
                    let command_opt = match elt {
                        RESPData::Str(s) | RESPData::BulkStr(s) => match RESPCommand::from_str(s) {
                            Ok(RESPCommand::Echo(mut to_echo)) => {
                                println!("Replacing {to_echo}");
                                to_echo = match elt_iter.next() {
                                    Some(RESPData::Str(s) | RESPData::BulkStr(s)) => *s,
                                    _ => to_echo,
                                };
                                println!("with {to_echo}");
                                Some(RESPCommand::Echo(to_echo))
                            }
                            Ok(command) => Some(command),
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

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                std::thread::spawn(|| handle_incoming(_stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
