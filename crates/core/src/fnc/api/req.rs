use anyhow::Result;
use surrealdb_types::SurrealValue;

use crate::{api::{middleware::api_x::{common::BodyStrategy, req::BodyParser}, request::ApiRequest}, fnc::args::FromPublic, sql::expression::convert_public_value_to_internal, val::Value};

pub async fn body((FromPublic(mut req), FromPublic(strategy)): (FromPublic<ApiRequest>, FromPublic<BodyStrategy>)) -> Result<Value> {
    let mut parser = BodyParser::from((&mut req, strategy));
    parser.process().await?;
    Ok(convert_public_value_to_internal(req.into_value()))
}