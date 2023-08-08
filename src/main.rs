use chrono::{DateTime, NaiveDateTime, ParseError};
use flate2::read::GzDecoder;
use quick_xml::de::from_str;
use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlReader;
use serde::Deserialize;
use sqlx::types::ipnetwork::IpNetwork;
use sqlx::{migrate::MigrateDatabase, PgPool, Pool, Sqlite, SqlitePool};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

const DB_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";

extern crate prog_rs;

use prog_rs::prelude::*;

async fn process_reputation(reputation: Reputation, db: &PgPool) {
    // Do something with record
    // println!("{:?}", reputation);
    // Insert the data into our database
    match sqlx::query!(r#"INSERT INTO dave_team_cymru_repfeed (stamp, addr, notes, cc, reputation_key, proto, family, asn, category, reputation_score, port) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#
        ,reputation.stamp
        ,reputation.addr
        ,reputation.notes
        ,reputation.cc
        ,reputation.reputation_key
        ,reputation.proto
        ,reputation.family
        ,reputation.asn
        ,reputation.category
        ,reputation.reputation_score
        ,reputation.port)
        .execute(db).await {
            Ok(_) => (),
            Err(e) => println!("Error inserting record: {}", e ),
        }
}

async fn parse_xml(
    reader: BufReader<GzDecoder<prog_rs::FileProgress>>,
    db: PgPool,
) -> quick_xml::Result<()> {
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

async fn read_gz_file<P: AsRef<Path>>(path: P, db: PgPool) -> std::io::Result<()> {
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
    let db = PgPool::connect(DB_URL).await.unwrap();
    read_gz_file("data.xml.gz", db).await.unwrap();
}
#[derive(Clone, Deserialize, Debug)]
struct Reputation {
    #[serde(deserialize_with = "from_timestamp")]
    stamp: NaiveDateTime,
    addr: IpNetwork,
    notes: Option<String>,
    cc: String,
    reputation_key: String,
    proto: Option<i32>,
    family: Option<String>,
    #[serde(deserialize_with = "from_str_asn")]
    asn: Option<i32>,
    category: String,
    reputation_score: i32,
    port: Option<i32>,
}

fn from_str_asn<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "NA" {
        Ok(None)
    } else {
        Ok(Some(s.parse::<i32>().unwrap()))
    }
}

fn from_timestamp<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let naive = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").unwrap();
    let datetime: DateTime<chrono::offset::Utc> = DateTime::from_utc(naive, chrono::offset::Utc);
    Ok(datetime.naive_utc())
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

#[test]
fn test_another_example() {
    let data = "<reputation>
        <stamp>2023-07-30 14:04:30</stamp>
        <addr>178.52.163.60</addr>
        <notes>dsthost: hzmksreiuojy.ru; destination_port_numbers: 80;</notes>
        <cc>SY</cc>
        <reputation_key>A1B0C1D7E0F0G0H0I0J0K0</reputation_key>
        <family>andromeda</family>
        <asn>NA</asn>
        <category>bot</category>
        <reputation_score>2</reputation_score>
    </reputation>";
    let res: Result<Reputation, _> = quick_xml::de::from_str(data);
    match res {
        Ok(rep) => assert_eq!(rep.cc, "SY"),
        Err(_) => assert!(false),
    }
}
