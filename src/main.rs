use flate2::read::GzDecoder;
use quick_xml::de::{Deserializer, from_str};
use quick_xml::events::Event;
use quick_xml::se::to_string;
use quick_xml::reader::Reader as XmlReader;
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

fn process_reputation(reputation: Reputation) {
    // Do something with record
    println!("{:?}", reputation);
}

fn parse_xml(reader: BufReader<GzDecoder<File>>) -> quick_xml::Result<()> {
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
                        Ok(Event::End(ref e)) if e.name() == quick_xml::name::QName(b"reputation") => {
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
                    Ok(reputation) => process_reputation(reputation),
                    Err(e) => {
                        println!("Text in current element: {}", std::str::from_utf8(&txt).unwrap());
                        panic!("Error at position {}: {:?}", reader.buffer_position(), e)
                    },
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                println!("Text in current element: {}", std::str::from_utf8(&txt).unwrap());
                panic!("Error at position {}: {:?}", reader.buffer_position(), e)
            },
            _ => (),
        }
        buf.clear();
    }
    Ok(())
}

fn read_gz_file<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    match parse_xml(reader) {
        Ok(_) => println!("Finished processing the file."),
        Err(e) => eprintln!("Failed to process file: {}", e),
    }

    Ok(())
}

fn main() {
    read_gz_file("cut.xml.gz").unwrap();
}


#[derive(Deserialize, Debug)]
struct Reputation{
    stamp: String,
    addr: String,
    notes: String,
    cc: String,
    reputation_key: String,
    proto: Option<String>,
    family: Option<String>,
    asn: String,
    category: String,
    reputation_score: String,
    port: String,

}

#[test]
fn test_deserialize_xml(){
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
fn test_deserialize_reputation(){
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