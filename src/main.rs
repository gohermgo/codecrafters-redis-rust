use std::{
    fmt,
    io::{self, Read, Write},
    net::TcpListener,
    str::FromStr,
};

pub enum RESPData<'a> {
    Str(&'a str),
}

impl fmt::Display for RESPData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RESPData::Str(payload) => f.write_fmt(format_args!("+{}\r\n", payload)),
        }
    }
}

pub enum RESPCommand<'a> {
    Ping(Option<&'a str>),
}

impl<'a> FromStr for RESPCommand<'a> {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("Trying RESPCommand from {s}");
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
            RESPCommand::Ping(None) => f.write_str("PONG"),
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
            "PING" => Ok(RESPCommand::Ping(None)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, command)),
        }
    }
}

fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let mut command_buffer = vec![];
                _stream.read(&mut command_buffer)?;
                let command = RESPCommand::try_from(command_buffer)?;
                _stream.write(command.to_string().as_bytes())?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
