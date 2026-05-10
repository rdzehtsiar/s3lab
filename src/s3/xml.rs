// SPDX-License-Identifier: Apache-2.0

use crate::s3::error::S3Error;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;

pub const XML_CONTENT_TYPE: &str = "application/xml";

pub fn error_response_xml(error: &S3Error) -> String {
    let mut writer = Writer::new(Vec::new());

    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("writing XML declaration to memory cannot fail");
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
