// SPDX-License-Identifier: Apache-2.0

use crate::s3::error::S3Error;
use crate::s3::time::s3_xml_timestamp;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use time::OffsetDateTime;

pub const XML_CONTENT_TYPE: &str = "application/xml";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListBucketsXml {
    pub buckets: Vec<ListBucketXml>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListBucketXml {
    pub name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListObjectsV2Xml {
    pub bucket: String,
    pub entries: Vec<ListObjectsV2XmlEntry>,
    pub max_keys: usize,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ListObjectsV2XmlEntry {
    Object(ListObjectXml),
    CommonPrefix(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListObjectXml {
    pub key: String,
    pub etag: String,
    pub content_length: u64,
    pub last_modified: OffsetDateTime,
}

pub fn error_response_xml(error: &S3Error) -> String {
    let mut writer = Writer::new(Vec::new());

    write_xml_declaration(&mut writer);
    writer
        .write_event(Event::Start(BytesStart::new("Error")))
        .expect("writing XML start element to memory cannot fail");

    write_text_element(&mut writer, "Code", error.code.as_str());
    write_text_element(&mut writer, "Message", &error.message);
    write_text_element(&mut writer, "Resource", &error.resource);
    write_text_element(&mut writer, "RequestId", error.request_id.as_str());

    writer
        .write_event(Event::End(BytesEnd::new("Error")))
        .expect("writing XML end element to memory cannot fail");

    String::from_utf8(writer.into_inner()).expect("quick-xml writes valid UTF-8")
}

pub fn list_buckets_response_xml(listing: &ListBucketsXml) -> String {
    let mut writer = Writer::new(Vec::new());

    write_xml_declaration(&mut writer);
    writer
        .write_event(Event::Start(BytesStart::new("ListAllMyBucketsResult")))
        .expect("writing XML start element to memory cannot fail");
    writer
        .write_event(Event::Start(BytesStart::new("Buckets")))
        .expect("writing XML start element to memory cannot fail");

    for bucket in &listing.buckets {
        writer
            .write_event(Event::Start(BytesStart::new("Bucket")))
            .expect("writing XML start element to memory cannot fail");
        write_text_element(&mut writer, "Name", bucket.name.as_str());
        writer
            .write_event(Event::End(BytesEnd::new("Bucket")))
            .expect("writing XML end element to memory cannot fail");
    }

    writer
        .write_event(Event::End(BytesEnd::new("Buckets")))
        .expect("writing XML end element to memory cannot fail");
    writer
        .write_event(Event::End(BytesEnd::new("ListAllMyBucketsResult")))
        .expect("writing XML end element to memory cannot fail");

    String::from_utf8(writer.into_inner()).expect("quick-xml writes valid UTF-8")
}

pub fn list_objects_v2_response_xml(
    listing: &ListObjectsV2Xml,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    continuation_token: Option<&str>,
    encoding_type: Option<&str>,
) -> String {
    let mut writer = Writer::new(Vec::new());

    write_xml_declaration(&mut writer);
    writer
        .write_event(Event::Start(BytesStart::new("ListBucketResult")))
        .expect("writing XML start element to memory cannot fail");
    write_text_element(&mut writer, "Name", listing.bucket.as_str());
    write_text_element(&mut writer, "Prefix", prefix.unwrap_or(""));
    if let Some(delimiter) = delimiter {
        write_text_element(&mut writer, "Delimiter", delimiter);
    }
    if let Some(encoding_type) = encoding_type {
        write_text_element(&mut writer, "EncodingType", encoding_type);
    }
    write_text_element(&mut writer, "KeyCount", &listing.entries.len().to_string());
    write_text_element(&mut writer, "MaxKeys", &listing.max_keys.to_string());
    if let Some(token) = continuation_token {
        write_text_element(&mut writer, "ContinuationToken", token);
    }
    write_text_element(
        &mut writer,
        "IsTruncated",
        if listing.is_truncated {
            "true"
        } else {
            "false"
        },
    );

    for entry in &listing.entries {
        match entry {
            ListObjectsV2XmlEntry::Object(object) => write_object_xml(&mut writer, object),
            ListObjectsV2XmlEntry::CommonPrefix(prefix) => {
                writer
                    .write_event(Event::Start(BytesStart::new("CommonPrefixes")))
                    .expect("writing XML start element to memory cannot fail");
                write_text_element(&mut writer, "Prefix", prefix);
                writer
                    .write_event(Event::End(BytesEnd::new("CommonPrefixes")))
                    .expect("writing XML end element to memory cannot fail");
            }
        }
    }

    if listing.is_truncated {
        if let Some(token) = &listing.next_continuation_token {
            write_text_element(&mut writer, "NextContinuationToken", token);
        }
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListBucketResult")))
        .expect("writing XML end element to memory cannot fail");

    String::from_utf8(writer.into_inner()).expect("quick-xml writes valid UTF-8")
}

fn write_object_xml(writer: &mut Writer<Vec<u8>>, object: &ListObjectXml) {
    writer
        .write_event(Event::Start(BytesStart::new("Contents")))
        .expect("writing XML start element to memory cannot fail");
    write_text_element(writer, "Key", object.key.as_str());
    write_text_element(
        writer,
        "LastModified",
        &s3_xml_timestamp(object.last_modified),
    );
    write_text_element(writer, "ETag", &object.etag);
    write_text_element(writer, "Size", &object.content_length.to_string());
    write_text_element(writer, "StorageClass", "STANDARD");
    writer
        .write_event(Event::End(BytesEnd::new("Contents")))
        .expect("writing XML end element to memory cannot fail");
}

fn write_xml_declaration(writer: &mut Writer<Vec<u8>>) {
    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("writing XML declaration to memory cannot fail");
}

fn write_text_element(writer: &mut Writer<Vec<u8>>, name: &str, value: &str) {
    writer
        .write_event(Event::Start(BytesStart::new(name)))
        .expect("writing XML start element to memory cannot fail");
    writer
        .write_event(Event::Text(BytesText::from_escaped(escape_text(value))))
        .expect("writing XML text to memory cannot fail");
    writer
        .write_event(Event::End(BytesEnd::new(name)))
        .expect("writing XML end element to memory cannot fail");
}

fn escape_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\r' => escaped.push_str("&#13;"),
            _ => escaped.push(character),
        }
    }
    escaped
}
