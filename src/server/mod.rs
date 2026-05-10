// SPDX-License-Identifier: Apache-2.0

pub mod routes;
pub mod state;

use axum::Router;
use routes::handle_request;
use state::ServerState;

pub const PHASE1_SERVER_SCOPE: &str = "path-style-local-s3";

pub fn router(state: ServerState) -> Router {
    Router::new().fallback(handle_request).with_state(state)
}
