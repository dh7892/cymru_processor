use flate2::read::GzDecoder;
use quick_xml::de::from_str;
use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlReader;
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool, Pool};

const DB_URL: &str = "sqlite://sqlite.db";
const DB_URL2: &str = "postgres://postgres:postgres@localhost:5432/postgres";

extern crate prog_rs;

use prog_rs::prelude::*;

async fn process_reputation(reputation: Reputation, db: &SqlitePool) {
    // Do something with record
    // println!("{:?}", reputation);
    // Insert the data into our database
    let result = sqlx::query("INSERT INTO reputation (stamp, addr, notes, cc, reputation_key, proto, family, asn, category, reputation_score, port) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(reputation.stamp)
        .bind(reputation.addr)
        .bind(reputation.notes)
        .bind(reputation.cc)
        .bind(reputation.reputation_key)
        .bind(reputation.proto)
        .bind(reputation.family)
        .bind(reputation.asn)
        .bind(reputation.category)
        .bind(reputation.reputation_score)
        .bind(reputation.port)
        .execute( db).await.unwrap();
}

async fn parse_xml(reader: BufReader<GzDecoder<prog_rs::FileProgress>>, db: Pool<Sqlite>) -> quick_xml::Result<()> {
    let mut reader = XmlReader::from_reader(reader);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut txt = Vec::new();
    let mut counter = 0;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == quick_xml::name::QName(b"reputation") => {
                txt.clear();
                txt.extend(b"<reputation>");
                counter += 1;
                let mut event_buf = Vec::new();
                loop {
                    match reader.read_event_into(&mut event_buf) {
                        Ok(Event::Text(e)) => txt.extend(e.escape_ascii()),
                        Ok(Event::End(ref e))
                            if e.name() == quick_xml::name::QName(b"reputation") =>
                        {
                            txt.extend(b"</reputation>");
                            break;
                        }
                        Ok(Event::Start(e)) => {
                            txt.push(b'<');
                            txt.extend(e.name().into_inner());
                            txt.push(b'>');
                        }
                        Ok(Event::End(e)) => {
                            txt.push(b'<');
                            txt.push(b'/');
                            txt.extend(e.name().into_inner());
                            txt.push(b'>');
                        }
                        Ok(Event::Eof) => panic!("Error: EOF reached while parsing XML."),
                        Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                        _ => (),
                    }
                    event_buf.clear();
                }
                let text_string = std::str::from_utf8(&txt).unwrap();
                match from_str(text_string) {
                    Ok(reputation) => process_reputation(reputation, &db).await,
                    Err(e) => {
                        println!(
                            "Text in current element: {}",
                            std::str::from_utf8(&txt).unwrap()
                        );
                        panic!("Error at position {}: {:?}", reader.buffer_position(), e)
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                println!(
                    "Text in current element: {}",
                    std::str::from_utf8(&txt).unwrap()
                );
                panic!("Error at position {}: {:?}", reader.buffer_position(), e)
            }
            _ => (),
        }
        buf.clear();
    }
    Ok(())
}

async fn read_gz_file<P: AsRef<Path>>(path: P, db: Pool<Sqlite> ) -> std::io::Result<()> {
    let file = File::open(path)
        .unwrap()
        .progress()
        .with_prefix("Reading file")
        .with_output_stream(prog_rs::OutputStream::StdErr)
        .with_bar_position(prog_rs::BarPosition::Right);
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    match parse_xml(reader, db).await {
        Ok(_) => println!("Finished processing the file."),
        Err(e) => eprintln!("Failed to process file: {}", e),
    }

    Ok(())
}


#[tokio::main]
async fn main() {
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        println!("Creating database {}", DB_URL);
        match Sqlite::create_database(DB_URL).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        println!("Database already exists");
    }
    
    let db = SqlitePool::connect(DB_URL).await.unwrap();
    let result = sqlx::query("CREATE TABLE IF NOT EXISTS reputation (id INTEGER PRIMARY KEY NOT NULL, 
    stamp VARCHAR(250) NOT NULL,
    addr VARCHAR(250) NOT NULL,
    notes VARCHAR(250) NOT NULL,
    cc VARCHAR(250) NOT NULL,
    reputation_key VARCHAR(250) NOT NULL,
    proto VARCHAR(250),
    family VARCHAR(250),
    asn VARCHAR(250) NOT NULL,
    category VARCHAR(250) NOT NULL,
    reputation_score VARCHAR(250) NOT NULL,
    port VARCHAR(250));").execute(&db).await.unwrap();
    println!("Create user table result: {:?}", result);
//     // read_gz_file("cut.xml.gz").unwrap();
    read_gz_file("cut.xml.gz", db).await.unwrap();

}
#[derive(Deserialize, Debug)]
struct Reputation {
    stamp: String,
    addr: String,
    notes: Option<String>,
    cc: String,
    reputation_key: String,
    proto: Option<String>,
    family: Option<String>,
    asn: String,
    category: String,
    reputation_score: String,
    port: Option<String>,
}

#[test]
fn test_deserialize_xml() {
    let data = "<repfeed version=\"2\" generated=\"2023-07-31 14:00:00\">
        <reputations>
            <reputation>
                <stamp>2023-07-30 14:00:00</stamp>
                <addr>173.231.184.122</addr>
                <notes>hostname: pywolwnvd.biz;</notes>
                <cc>US</cc>
                <reputation_key>A29B0C10000D8E0F0G0H0I10000J0K2</reputation_key>
                <family>http_post</family>
                <asn>32475</asn>
                <category>controller</category>
                <reputation_score>25</reputation_score>
                <port>80</port>
            </reputation>
        </reputations>
    </repfeed>";
}

#[test]
fn test_deserialize_reputation() {
    let data = "<reputation>
        <stamp>2023-07-30 14:00:00</stamp>
        <addr>173.231.184.122</addr>
        <notes>hostname: pywolwnvd.biz;</notes>
        <cc>US</cc>
        <reputation_key>A29B0C10000D8E0F0G0H0I10000J0K2</reputation_key>
        <family>http_post</family>
        <asn>32475</asn>
        <category>controller</category>
        <reputation_score>25</reputation_score>
        <port>80</port>
    </reputation>";

    let res: Result<Reputation, _> = quick_xml::de::from_str(data);
    match res {
        Ok(rep) => assert_eq!(rep.cc, "US"),
        Err(_) => assert!(false),
    }
}
