use http::Error as HttpError;
use hyper::Error as HyperError;
use reqwest::Error as ReqwestError;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use serde_pack::encode::Error as PackError;
use std::io::Error as IoError;
use surrealdb::Error as DBError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("The request body contains invalid data")]
	RequestError,

	#[error("{0}")]
	DBError(#[from] DBError),

	#[error("IO error: {0}")]
	IoError(#[from] IoError),

	#[error("HTTP Error: {0}")]
	HyperError(#[from] HyperError),

	#[error("HTTP Error: {0}")]
	HttpError(#[from] HttpError),

	#[error("JSON Error: {0}")]
	JsonError(#[from] JsonError),

	#[error("CBOR Error: {0}")]
	CborError(#[from] CborError),

	#[error("PACK Error: {0}")]
	PackError(#[from] PackError),

	#[error("Reqwest Error: {0}")]
	ReqwestError(#[from] ReqwestError),
}

impl warp::reject::Reject for Error {}
