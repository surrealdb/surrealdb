use anyhow::Result;
use surrealdb_types::Duration;

use crate::api::request::ApiRequest;

pub mod res;
pub mod req;
pub mod common;

pub fn timeout(req: &mut ApiRequest, timeout: Duration) -> Result<()> {
    req.timeout = Some(timeout);
    Ok(())
}