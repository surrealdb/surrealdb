/// Rejection response types.
pub mod rejection {
	use async_graphql::ParseRequestError;
	use axum::{
		body::{boxed, Body, BoxBody},
		http,
		http::StatusCode,
		response::IntoResponse,
	};

	/// Rejection used for [`GraphQLRequest`](GraphQLRequest).
	pub struct GraphQLRejection(pub ParseRequestError);

	impl IntoResponse for GraphQLRejection {
		fn into_response(self) -> http::Response<BoxBody> {
			match self.0 {
				ParseRequestError::PayloadTooLarge => http::Response::builder()
					.status(StatusCode::PAYLOAD_TOO_LARGE)
					.body(boxed(Body::empty()))
					.unwrap(),
				bad_request => http::Response::builder()
					.status(StatusCode::BAD_REQUEST)
					.body(boxed(Body::from(format!("{:?}", bad_request))))
					.unwrap(),
			}
		}
	}

	impl From<ParseRequestError> for GraphQLRejection {
		fn from(err: ParseRequestError) -> Self {
			GraphQLRejection(err)
		}
	}
}
