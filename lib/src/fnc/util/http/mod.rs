use crate::err::Error;
use crate::sql::json;
use crate::sql::object::Object;
use crate::sql::strand::Strand;
use crate::sql::value::Value;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;

pub(crate) fn uri_is_valid(uri: &str) -> bool {
	reqwest::Url::parse(uri).is_ok()
}

pub async fn head(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new HEAD request
	let mut req = cli.head(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => Ok(Value::None),
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn get(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.get(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn put(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.put(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	if body.is_some() {
		req = req.json(&body);
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn post(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.post(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	if body.is_some() {
		req = req.json(&body);
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn patch(uri: Strand, body: Value, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.patch(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	if body.is_some() {
		req = req.json(&body);
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn delete(uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.delete(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}
