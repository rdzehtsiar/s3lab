// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, Response, StatusCode};

const INDEX_HTML: &str = include_str!("ui_assets/index.html");
const APP_CSS: &str = include_str!("ui_assets/app.css");
const APP_JS: &str = include_str!("ui_assets/app.js");

pub async fn inspector_root() -> Response<Body> {
    asset_response(INDEX_HTML, "text/html; charset=utf-8")
}

pub async fn inspector_asset(path: &str) -> Response<Body> {
    match path {
        "app.css" => asset_response(APP_CSS, "text/css; charset=utf-8"),
        "app.js" => asset_response(APP_JS, "application/javascript; charset=utf-8"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Body::from("asset not found\n"))
            .expect("static asset not found response is valid"),
    }
}

fn asset_response(body: &'static str, content_type: &'static str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, HeaderValue::from_static(content_type))
        .header(CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .body(Body::from(body))
        .expect("embedded asset response is valid")
}
