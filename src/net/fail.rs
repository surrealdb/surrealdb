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
	if err.is_not_found() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 404,
				details: Some(format!("Requested resource not found")),
				description: Some(format!("The requested resource does not exist. Check that you have entered the url correctly.")),
				information: None,
			}),
			StatusCode::NOT_FOUND,
		))
	} else if let Some(err) = err.find::<Error>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 400,
				details: Some(format!("Request problems detected")),
				description: Some(format!("There is a problem with your request. Refer to the documentation for further information.")),
				information: Some(err.to_string()),
			}),
			StatusCode::BAD_REQUEST,
		))
	} else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 405,
				details: Some(format!("Request content length too large")),
				description: Some(format!("The requested http method is not allowed for this resource. Refer to the documentation for allowed methods.")),
				information: None,
			}),
			StatusCode::METHOD_NOT_ALLOWED,
		))
	} else if let Some(_) = err.find::<warp::reject::PayloadTooLarge>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 413,
				details: Some(format!("Request problems detected")),
				description: Some(format!("The request has exceeded the maximum payload size. Refer to the documentation for the request limitations.")),
				information: None,
			}),
			StatusCode::PAYLOAD_TOO_LARGE,
		))
	} else if let Some(_) = err.find::<warp::reject::UnsupportedMediaType>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 415,
				details: Some(format!("Unsupported content type requested")),
				description: Some(format!("The request needs to adhere to certain constraints. Refer to the documentation for supported content types.")),
				information: None,
			}),
			StatusCode::UNSUPPORTED_MEDIA_TYPE,
		))
	} else if let Some(_) = err.find::<warp::reject::MissingHeader>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 412,
				details: Some(format!("Request problems detected")),
				description: Some(format!("The request appears to be missing a required header. Refer to the documentation for request requirements.")),
				information: None,
			}),
			StatusCode::PRECONDITION_FAILED,
		))
	} else if let Some(_) = err.find::<warp::reject::InvalidQuery>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 501,
				details: Some(format!("Not Implemented")),
				description: Some(format!("The server either does not recognize the request method, or it lacks the ability to fulfill the request.")),
				information: None,
			}),
			StatusCode::NOT_IMPLEMENTED,
		))
	} else if let Some(_) = err.find::<warp::reject::InvalidHeader>() {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 501,
				details: Some(format!("Not Implemented")),
				description: Some(format!("The server either does not recognize the request method, or it lacks the ability to fulfill the request.")),
				information: None,
			}),
			StatusCode::NOT_IMPLEMENTED,
		))
	} else {
		Ok(warp::reply::with_status(
			warp::reply::json(&Message {
				code: 500,
				details: Some(format!("Internal server error")),
				description: Some(format!("There was a problem with our servers, and we have been notified. Refer to the documentation for further information")),
				information: None,
			}),
			StatusCode::INTERNAL_SERVER_ERROR,
		))
	}
}
