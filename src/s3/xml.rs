// SPDX-License-Identifier: Apache-2.0

use crate::s3::error::S3Error;
use crate::storage::{BucketSummary, ObjectListing};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;

pub const XML_CONTENT_TYPE: &str = "application/xml";

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

pub fn list_buckets_response_xml(buckets: &[BucketSummary]) -> String {
    let mut writer = Writer::new(Vec::new());

    write_xml_declaration(&mut writer);
    writer
        .write_event(Event::Start(BytesStart::new("ListAllMyBucketsResult")))
        .expect("writing XML start element to memory cannot fail");
    writer
        .write_event(Event::Start(BytesStart::new("Buckets")))
        .expect("writing XML start element to memory cannot fail");

    for bucket in buckets {
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

pub fn list_objects_v2_response_xml(listing: &ObjectListing, prefix: Option<&str>) -> String {
    let mut writer = Writer::new(Vec::new());

    write_xml_declaration(&mut writer);
    writer
        .write_event(Event::Start(BytesStart::new("ListBucketResult")))
        .expect("writing XML start element to memory cannot fail");
    write_text_element(&mut writer, "Name", listing.bucket.as_str());
    if let Some(prefix) = prefix {
        write_text_element(&mut writer, "Prefix", prefix);
    }
    write_text_element(&mut writer, "KeyCount", &listing.objects.len().to_string());

    for object in &listing.objects {
        writer
            .write_event(Event::Start(BytesStart::new("Contents")))
            .expect("writing XML start element to memory cannot fail");
        write_text_element(&mut writer, "Key", object.key.as_str());
        write_text_element(&mut writer, "Size", &object.content_length.to_string());
        writer
            .write_event(Event::End(BytesEnd::new("Contents")))
            .expect("writing XML end element to memory cannot fail");
    }

    if let Some(token) = &listing.next_continuation_token {
        write_text_element(&mut writer, "NextContinuationToken", token);
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListBucketResult")))
        .expect("writing XML end element to memory cannot fail");

    String::from_utf8(writer.into_inner()).expect("quick-xml writes valid UTF-8")
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
        .write_event(Event::Text(BytesText::new(value)))
        .expect("writing XML text to memory cannot fail");
    writer
        .write_event(Event::End(BytesEnd::new(name)))
        .expect("writing XML end element to memory cannot fail");
}
