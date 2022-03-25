use http::Error as HttpError;
use hyper::Error as HyperError;
use reqwest::Error as ReqwestError;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use serde_pack::encode::Error as PackError;
use std::io::Error as IoError;
use surrealdb::Error as DbError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("The request body contains invalid data")]
	Request,

	#[error("{0}")]
	Db(#[from] DbError),

	#[error("IO error: {0}")]
	Io(#[from] IoError),

	#[error("HTTP Error: {0}")]
	Hyper(#[from] HyperError),

	#[error("HTTP Error: {0}")]
	Http(#[from] HttpError),

	#[error("JSON Error: {0}")]
	Json(#[from] JsonError),

	#[error("CBOR Error: {0}")]
	Cbor(#[from] CborError),

	#[error("PACK Error: {0}")]
	Pack(#[from] PackError),

	#[error("Reqwest Error: {0}")]
	Reqwest(#[from] ReqwestError),
}

impl warp::reject::Reject for Error {}
