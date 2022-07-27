use crate::err::Error;
use crate::sql::json;
use crate::sql::object::Object;
use crate::sql::strand::Strand;
use crate::sql::value::Value;

pub async fn head(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new HEAD request
	let mut req = surf::head(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => Ok(Value::None),
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn get(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new GET request
	let mut req = surf::get(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn put(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new GET request
	let mut req = surf::put(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn post(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new GET request
	let mut req = surf::post(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn patch(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new GET request
	let mut req = surf::patch(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn delete(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Start a new GET request
	let mut req = surf::delete(uri.as_str());
	// Add the User-Agent header
	req = req.header("User-Agent", "SurrealDB");
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}
