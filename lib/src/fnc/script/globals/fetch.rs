use crate::fnc::script::classes::request::request::Request;
use crate::fnc::script::classes::request::RequestInput;
use crate::fnc::script::classes::request::RequestOptions;
use crate::fnc::script::classes::response::response::Response;
use js::Rest;
use js::Result;

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub(crate) async fn fetch(input: RequestInput, args: Rest<RequestOptions>) -> Result<Response> {
	let request = Request::new(input, args);
	let mut req: surf::Request = request.clone().into();
	// set redirect
	let client = surf::client().with(surf::middleware::Redirect::new(5));
	// set keepalibe
	// client.config().set_http_keep_alive(request.keepalive);

	if let Some(body) = request.take_body().await {
		req.set_body(body);
	}

	let resp = client.send(req).await.map_err(|e| throw_js_exception!(e))?;
	Ok(Response::from_surf(resp))
}
