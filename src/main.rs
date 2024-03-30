use std::{fmt, net::TcpListener};

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

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
