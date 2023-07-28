use indoc::formatdoc;
use surrealdb::Response;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
#[serial]
async fn live_updates() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/rpc");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// WebSocket upgrade
	let res = client
		.get(url)
		.header(header::CONNECTION, "Upgrade")
		.header(header::UPGRADE, "websocket")
		.header(header::SEC_WEBSOCKET_VERSION, "13")
		.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
		.send()
		.await?
		.upgrade()
		.await;
	assert!(res.is_ok(), "upgrade err: {}", res.unwrap_err());
	let mut ws = res.unwrap();
	let table_name = "table_FD40A9A361884C56B5908A934164884A".to_string();
	let a = formatdoc! {
		r#"{{
		"id": 1,
		"method": "live",
		"params": [
			"{table_name}"
		]
	}}"#, table_name = &table_name};
	println!("{:?}", a);
	ws.write(a.as_bytes()).await.unwrap();
	let mut read_ws_data: Vec<u8> = vec![];
	ws.read_buf(&mut read_ws_data).await.unwrap();
	println!("{:?}", read_ws_data);
	ws.read_buf(&mut read_ws_data).await.unwrap();
	println!("{:?}", read_ws_data);
	let msg: serde_json::Value = serde_json::from_slice(&read_ws_data).unwrap();
	panic!("{:?}", msg);
	Ok(())
}
