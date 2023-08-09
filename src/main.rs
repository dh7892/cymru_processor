use chrono::{DateTime, NaiveDateTime};
use flate2::read::GzDecoder;
use quick_xml::de::from_str;
use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlReader;
use serde::Deserialize;
use sqlx::types::ipnetwork::IpNetwork;
use sqlx::PgPool;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

extern crate prog_rs;

use prog_rs::prelude::*;

// Given a vec of representations, insert them into the database in an effieicnt
// transaction
async fn process_reputations(reputations: &Vec<Reputation>, db: &PgPool) -> sqlx::Result<()> {
    let mut transaction = db.begin().await?;

    let mut sql = r#"INSERT INTO sink.dave_team_cymru_repfeed 
    (   
        stamp,
        addr,
        notes,
        cc, 
        rep_days_in_feed,
        rep_count_of_active_detections ,
        rep_count_of_passive_detections,
        rep_detection_type,
        rep_ssl_usage,
        rep_controller_instruction_decoded,
        rep_ddos_command_observed,
        rep_non_standard_port,
        rep_number_of_unique_domain_names_on_same_ip,
        rep_number_of_distinct_controllers_on_same_ip,
        rep_other_bad_ips_in_24,
        proto,
        family,
        asn,
        category,
        reputation_score,
        port
     )
     VALUES "#
        .to_string();

    let num_pararms = 21;

    for (index, _) in reputations.iter().enumerate() {
        sql.push_str(&format!("( ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}),",
        index * num_pararms + 1,
        index * num_pararms + 2,
        index * num_pararms + 3,
        index * num_pararms + 4,
        index * num_pararms + 5,
        index * num_pararms + 6,
        index * num_pararms + 7,
        index * num_pararms + 8,
        index * num_pararms + 9,
        index * num_pararms + 10,
        index * num_pararms + 11,
        index * num_pararms + 12,
        index * num_pararms + 13,
        index * num_pararms + 14,
        index * num_pararms + 15,
        index * num_pararms + 16,
        index * num_pararms + 17,
        index * num_pararms + 18,
        index * num_pararms + 19,
        index * num_pararms + 20,
        index * num_pararms + 21,
        ));
    }
    // Remove the trailing comma
    sql.pop();

    let mut query = sqlx::query(&sql);

    for rep in reputations {
        query = query.bind(&rep.stamp);
        query = query.bind(&rep.addr);
        query = query.bind(&rep.notes);
        query = query.bind(&rep.cc);
        query = query.bind(&rep.reputation_key.DaysInFeed);
        query = query.bind(&rep.reputation_key.CountOfActiveDetections);
        query = query.bind(&rep.reputation_key.CountOfPassiveDetections);
        query = query.bind(&rep.reputation_key.DetectionType);
        query = query.bind(&rep.reputation_key.SSLUsage);
        query = query.bind(&rep.reputation_key.ControllerInstructionDecoded);
        query = query.bind(&rep.reputation_key.DDoSCommandObserved);
        query = query.bind(&rep.reputation_key.NonStandardPort);
        query = query.bind(&rep.reputation_key.NumberOfUniqueDomainNamesOnSameIP);
        query = query.bind(&rep.reputation_key.NumberOfDistinctControllersOnSameIP);
        query = query.bind(&rep.reputation_key.OtherBadIPsIn24);
        query = query.bind(&rep.proto);
        query = query.bind(&rep.family);
        query = query.bind(&rep.asn);
        query = query.bind(&rep.category);
        query = query.bind(&rep.reputation_score);
        query = query.bind(&rep.port);
    }

    query.execute(&mut *transaction).await?;

    transaction.commit().await?;

    Ok(())
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
    let mut reputations = Vec::new();
    let batch_size = 1000;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == quick_xml::name::QName(b"reputation") => {
                txt.clear();
                txt.extend(b"<reputation>");
                counter += 1;
                let mut event_buf = Vec::new();
                loop {
                    if reputations.len() == batch_size {
                        match process_reputations(&reputations, &db).await {
                            Ok(_) => (),
                            Err(e) => println!("Error processing batch {}: {}", counter, e),
                        }
                        reputations.clear();
                    }
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
                    Ok(reputation) => {
                        reputations.push(reputation);
                    }
                    Err(e) => {
                        println!(
                            "Text in current element: {}",
                            std::str::from_utf8(&txt).unwrap()
                        );
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
    // Clear final batch (may not be a full batch)
    if !reputations.is_empty() {
        match process_reputations(&reputations, &db).await {
            Ok(_) => (),
            Err(e) => println!("Error processing final batch: {}", e),
        }
        reputations.clear();
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

#[derive(Clone, Deserialize, Debug)]
struct Reputation {
    #[serde(deserialize_with = "from_timestamp")]
    stamp: NaiveDateTime,
    addr: IpNetwork,
    notes: Option<String>,
    cc: String,
    reputation_key: ReputationKey,
    proto: Option<i32>,
    family: Option<String>,
    #[serde(deserialize_with = "from_str_asn")]
    asn: Option<i32>,
    category: String,
    reputation_score: i32,
    port: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, sqlx::Type)]
enum DetectionType {
    other = 0,
    netflow = 1,
    sinkhole = 2,
    darknet = 3,
    honeypot = 4,
    human_verified = 5,
    active_probe = 6,
    reported_by_3rd_party = 7,
    unverified_malware_c2 = 8,
}

#[derive(Clone, Debug)]
struct ReputationKey {
    DaysInFeed: i32,
    CountOfActiveDetections: i32,
    CountOfPassiveDetections: i32,
    DetectionType: DetectionType,
    SSLUsage: bool,
    ControllerInstructionDecoded: bool,
    DDoSCommandObserved: bool,
    NonStandardPort: bool,
    NumberOfUniqueDomainNamesOnSameIP: i32,
    NumberOfDistinctControllersOnSameIP: i32,
    OtherBadIPsIn24: i32,
}

impl<'de> Deserialize<'de> for ReputationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Split the string on any non-digit character
        let fields = s.split(|c: char| !c.is_digit(10)).collect::<Vec<&str>>();
        // We should have 11 fields as we have a split on the A at the start
        if fields.len() != 12 {
            return Err(serde::de::Error::custom(
                "ReputationKey must have 11 fields",
            ));
        }
        let DaysInFeed = fields[1].parse::<i32>().unwrap();
        let CountOfActiveDetections = fields[2].parse::<i32>().unwrap();
        let CountOfPassiveDetections = fields[3].parse::<i32>().unwrap();
        let DetectionTypeInt = fields[4].parse::<i32>().unwrap();
        let DetectionType = match DetectionTypeInt {
            0 => DetectionType::other,
            1 => DetectionType::netflow,
            2 => DetectionType::sinkhole,
            3 => DetectionType::darknet,
            4 => DetectionType::honeypot,
            5 => DetectionType::human_verified,
            6 => DetectionType::active_probe,
            7 => DetectionType::reported_by_3rd_party,
            8 => DetectionType::unverified_malware_c2,
            _ => return Err(serde::de::Error::custom("Invalid DetectionType")),
        };
        let SSLUsage = fields[5].parse::<i32>().unwrap() == 1;
        let ControllerInstructionDecoded = fields[6].parse::<i32>().unwrap() == 1;
        let DDoSCommandObserved = fields[7].parse::<i32>().unwrap() == 1;
        let NonStandardPort = fields[8].parse::<i32>().unwrap() == 1;
        let NumberOfUniqueDomainNamesOnSameIP = fields[9].parse::<i32>().unwrap();
        let NumberOfDistinctControllersOnSameIP = fields[10].parse::<i32>().unwrap();
        let OtherMaliciousControllersInSameDay = fields[11].parse::<i32>().unwrap();

        Ok(ReputationKey {
            DaysInFeed,
            CountOfActiveDetections,
            CountOfPassiveDetections,
            DetectionType,
            SSLUsage,
            ControllerInstructionDecoded,
            DDoSCommandObserved,
            NonStandardPort,
            NumberOfUniqueDomainNamesOnSameIP,
            NumberOfDistinctControllersOnSameIP,
            OtherBadIPsIn24: OtherMaliciousControllersInSameDay,
        })
    }
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

#[tokio::main]
async fn main() {
    let db_url = std::env::var("DATABASE_URL").expect("DB_URL must be set");
    let db = PgPool::connect(&db_url).await.unwrap();
    read_gz_file("cut.xml.gz", db).await.unwrap();
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

#[test]
fn test_rep_key() {
    let data = "<reputation_key>A1B0C1D7E0F0G0H0I0J0K0</reputation_key>";
    let res: Result<ReputationKey, _> = quick_xml::de::from_str(data);
    match res {
        Ok(rep) => (),
        Err(_) => assert!(false),
    }
}
