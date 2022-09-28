use crate::err::Error;
use serde::Serialize;
use warp::http::StatusCode;

#[derive(Serialize)]
struct Message {
	code: u16,
	#[serde(skip_serializing_if = "Option::is_none")]
	details: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	information: Option<String>,
}

pub async fn recover(err: warp::Rejection) -> Result<impl warp::Reply, warp::Rejection> {
	if let Some(err) = err.find::<Error>() {
		match err {
			Error::InvalidAuth => Ok(warp::reply::with_status(
				warp::reply::json(&Message {
					code: 403,
					details: Some("Authentication failed".to_string()),
					description: Some("Your authentication details are invalid. Reauthenticate using valid authentication parameters.".to_string()),
					information: Some(err.to_string()),
				}),
				StatusCode::FORBIDDEN,
			)),
			Error::InvalidType => Ok(warp::reply::with_status(
				warp::reply::json(&Message {
					code: 415,
					details: Some("Unsupported media type".to_string()),
					description: Some("The request needs to adhere to certain constraints. Refer to the documentation for supported content types.".to_string()),
					information: None,
				}),
				StatusCode::UNSUPPORTED_MEDIA_TYPE,
			)),
			Error::InvalidStorage => Ok(warp::reply::with_status(
				warp::reply::json(&Message {
					code: 500,
					details: Some("Health check failed".to_string()),
					description: Some("The database health check for this instance failed. There was an issue with the underlying storage engine.".to_string()),
					information: Some(err.to_string()),
				}),
				StatusCode::INTERNAL_SERVER_ERROR,
			)),
			_ => Ok(warp::reply::with_status(
				warp::reply::json(&Message {
					code: 400,
					details: Some("Request problems detected".to_string()),
					description: Some("There is a problem with your request. Refer to the documentation for further information.".to_string()),
					information: Some(err.to_string()),
				}),
				StatusCode::BAD_REQUEST,
			))
		}
	} else if err.is_not_found() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 404,
				details: Some("Requested resource not found".to_string()),
				description: Some("The requested resource does not exist. Check that you have entered the url correctly.".to_string()),
				information: None,
			}),
			StatusCode::NOT_FOUND,
		))
	} else if err.find::<warp::reject::MissingHeader>().is_some() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 412,
				details: Some("Request problems detected".to_string()),
				description: Some("The request appears to be missing a required header. Refer to the documentation for request requirements.".to_string()),
				information: None,
			}),
			StatusCode::PRECONDITION_FAILED,
		))
	} else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 413,
				details: Some("Payload too large".to_string()),
				description: Some("The request has exceeded the maximum payload size. Refer to the documentation for the request limitations.".to_string()),
				information: None,
			}),
			StatusCode::PAYLOAD_TOO_LARGE,
		))
	} else if err.find::<warp::reject::InvalidQuery>().is_some() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 501,
				details: Some("Not implemented".to_string()),
				description: Some("The server either does not recognize the query, or it lacks the ability to fulfill the request.".to_string()),
				information: None,
			}),
			StatusCode::NOT_IMPLEMENTED,
		))
	} else if err.find::<warp::reject::InvalidHeader>().is_some() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 501,
				details: Some("Not implemented".to_string()),
				description: Some("The server either does not recognize a request header, or it lacks the ability to fulfill the request.".to_string()),
				information: None,
			}),
			StatusCode::NOT_IMPLEMENTED,
		))
	} else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 405,
				details: Some("Requested method not allowed".to_string()),
				description: Some("The requested http method is not allowed for this resource. Refer to the documentation for allowed methods.".to_string()),
				information: None,
			}),
			StatusCode::METHOD_NOT_ALLOWED,
		))
	} else {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 500,
				details: Some("Internal server error".to_string()),
				description: Some("There was a problem with our servers, and we have been notified. Refer to the documentation for further information".to_string()),
				information: None,
			}),
			StatusCode::INTERNAL_SERVER_ERROR,
		))
	}
}
