//! Connection layer for running the suite against a remote SurrealDB instance
//! over the API (`--endpoint`), shared by the test runner and the bench runner.
//!
//! SurrealQL goes through the SDK `engine::any::connect` (ws/http), which encodes
//! results as flatbuffers and therefore preserves the exact `surrealdb_types::Value`
//! types — so remote SurrealQL results compare identically to the embedded path.
//!
//! GraphQL and GQL have no SDK method, so they are POSTed to the server's
//! `/graphql` and `/gql` HTTP routes directly. GraphQL data is JSON-shaped on
//! both sides, so JSON is faithful; GQL results carry SurrealDB types, so the
//! `/gql` response is requested as `application/flatbuffers` and decoded with the
//! same codec the RPC layer uses, preserving types.
#![cfg(feature = "remote")]

use anyhow::{Context, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Credentials, Root, Signin, Signup};
use surrealdb_types::{Kind, Object, SurrealValue, ToSql, Value as SurValue};

/// HTTP header the server reads the target namespace from.
const HEADER_NS: &str = "surreal-ns";
/// HTTP header the server reads the target database from.
const HEADER_DB: &str = "surreal-db";

/// Wraps an arbitrary credentials [`Object`] (a test's `[env].signin` / `signup`
/// block, already in wire shape) so it can be passed to the SDK's typed
/// `signin`/`signup`, which require `impl Credentials<_>`. The object is sent
/// verbatim as the credential payload — the same object the embedded path hands
/// to `iam::signin`/`signup`.
pub struct RawCredentials(pub Object);

impl SurrealValue for RawCredentials {
	fn kind_of() -> Kind {
		Object::kind_of()
	}

	fn into_value(self) -> SurValue {
		SurValue::Object(self.0)
	}

	fn from_value(value: SurValue) -> std::result::Result<Self, surrealdb_types::Error> {
		Object::from_value(value).map(RawCredentials)
	}
}

impl Credentials<Signin> for RawCredentials {}
impl Credentials<Signup> for RawCredentials {}

/// A connection to a remote SurrealDB instance, plus the HTTP client and base URL
/// used for the GraphQL/GQL routes.
pub struct RemoteConn {
	pub db: Surreal<Any>,
	/// The endpoint URL, retained so a fresh session can be re-established between
	/// tests (the SDK cannot unset a session's namespace/database, so the only way
	/// to a clean session is to reconnect — see [`RemoteConn::reset_session`]).
	pub endpoint: String,
	/// The HTTP base (e.g. `http://127.0.0.1:8000`) derived from the endpoint,
	/// used to POST to `/graphql` and `/gql`.
	pub http_base: String,
	pub http: reqwest::Client,
	/// Root credentials, retained so the connection can be re-authenticated as
	/// root after a test signs in as another principal.
	pub user: String,
	pub pass: String,
}

/// Install the process-wide rustls crypto provider, before anything opens a TLS
/// connection.
///
/// rustls picks a provider on its own only when exactly one of its `ring` and
/// `aws-lc-rs` backends is compiled in. Both are, and not by choice: the SDK's
/// `rustls` feature reaches `ring` through `tokio-tungstenite`, while `reqwest`
/// and `hyper-rustls` reach `aws-lc-rs`. With both present rustls refuses to
/// choose and panics on the first handshake, so without this every `wss://` and
/// `https://` endpoint is unreachable — a plain `ws://` one never notices.
///
/// `aws_lc_rs` is the provider the SDK declares for non-wasm targets and the one
/// `surrealdb_server::tls` installs, so a remote run negotiates what the server
/// itself would.
///
/// Idempotent: `install_default` fails only when a provider is already
/// installed, which leaves a working stack, so the result is discarded.
fn install_crypto_provider() {
	let _ = rustls::crypto::CryptoProvider::install_default(
		rustls::crypto::aws_lc_rs::default_provider(),
	);
}

impl RemoteConn {
	/// Connect to `endpoint` (ws/wss/http/https) and sign in as root.
	pub async fn connect(endpoint: &str, user: &str, pass: &str) -> Result<RemoteConn> {
		install_crypto_provider();
		let db = surrealdb::engine::any::connect(endpoint.to_string())
			.await
			.with_context(|| format!("connecting to --endpoint `{endpoint}`"))?;
		let http_base = http_base_from_endpoint(endpoint)?;
		let conn = RemoteConn {
			db,
			endpoint: endpoint.to_string(),
			http_base,
			http: reqwest::Client::new(),
			user: user.to_string(),
			pass: pass.to_string(),
		};
		conn.signin_root().await?;
		Ok(conn)
	}

	/// Re-establish a completely fresh session before a test runs.
	///
	/// The embedded harness gives every test a brand-new `Session`. Over the API
	/// the connection is long-lived and accumulates session state — selected
	/// namespace/database, `LET` params, and auth (`ac`/`rd`/`tk`). The SDK has no
	/// way to *unset* the namespace/database (`use_ns`/`use_db` only set, never
	/// clear) and `invalidate` leaves `ns`/`db` untouched, so reconnecting is the
	/// only faithful reset. Without it, e.g. a test that expects "Specify a
	/// namespace to use" instead sees the previous test's namespace still selected.
	pub async fn reset_session(&mut self) -> Result<()> {
		self.db = surrealdb::engine::any::connect(self.endpoint.clone())
			.await
			.with_context(|| format!("reconnecting to --endpoint `{}`", self.endpoint))?;
		self.signin_root().await?;
		Ok(())
	}

	/// (Re-)authenticate the connection as root, undoing any test-under-test signin.
	pub async fn signin_root(&self) -> Result<()> {
		self.db
			.signin(Root {
				username: self.user.clone(),
				password: self.pass.clone(),
			})
			.await
			.context("signing in to --endpoint as root")?;
		Ok(())
	}

	/// POST a GraphQL document to `/graphql` and return the decoded response. The
	/// response is parsed as a standard GraphQL JSON envelope (`{ data, errors }`),
	/// which is faithful because GraphQL data is JSON-shaped on both transports.
	pub async fn graphql(
		&self,
		ns: &str,
		db: &str,
		body: serde_json::Value,
	) -> Result<async_graphql::Response> {
		let resp = self
			.http
			.post(format!("{}/graphql", self.http_base))
			.basic_auth(&self.user, Some(&self.pass))
			.header(HEADER_NS, ns)
			.header(HEADER_DB, db)
			.header(reqwest::header::ACCEPT, "application/json")
			.json(&body)
			.send()
			.await
			.context("POST /graphql")?;
		let bytes = resp.bytes().await.context("reading /graphql response")?;
		serde_json::from_slice(&bytes).context("decoding /graphql JSON response")
	}

	/// POST an GQL query to `/gql` and return the per-statement results.
	///
	/// `application/flatbuffers` is requested so SurrealDB-typed values round-trip
	/// exactly; the body decodes to the same `Vec<QueryResult>`-shaped array the
	/// `/sql` route produces.
	pub async fn gql(&self, ns: &str, db: &str, source: &str) -> Result<GqlOutcome> {
		let resp = self
			.http
			.post(format!("{}/gql", self.http_base))
			.basic_auth(&self.user, Some(&self.pass))
			.header(HEADER_NS, ns)
			.header(HEADER_DB, db)
			// The server's `/gql` route only recognises its own media-type strings
			// (see `surrealdb_core::api::format`); use the constant so this can't
			// drift from `application/vnd.surrealdb.flatbuffers`.
			.header(reqwest::header::ACCEPT, surrealdb_rpc::format::FLATBUFFERS)
			.body(source.to_string())
			.send()
			.await
			.context("POST /gql")?;
		let status = resp.status();
		let bytes = resp.bytes().await.context("reading /gql response")?;
		if !status.is_success() {
			// A route-level rejection is the single result. Include the status so a
			// missing route (404 — server built without the `gql` feature) or a
			// disabled capability (403 — server not started with
			// `--allow-experimental gql`) is actionable rather than an empty error.
			let body = String::from_utf8_lossy(&bytes);
			let body = body.trim();
			let msg = if body.is_empty() {
				format!(
					"remote /gql returned HTTP {} with no body (the endpoint must be built with the `gql` feature and started with `--allow-experimental gql`)",
					status.as_u16()
				)
			} else {
				format!("remote /gql returned HTTP {}: {body}", status.as_u16())
			};
			return Ok(GqlOutcome::Rejected(msg));
		}
		let value = surrealdb_types::decode::<SurValue>(&bytes)
			.map_err(|e| anyhow::anyhow!("decoding /gql flatbuffers response: {e}"))?;
		Ok(GqlOutcome::Ran(query_result_array_to_results(value)))
	}
}

/// What the endpoint's `/gql` route did with a request.
///
/// The two are worth telling apart because only one of them means the endpoint
/// formed a view of the query: a rejection is the route declining it before
/// anything ran, so it is the endpoint's counterpart to a local parse failure,
/// while results mean it ran and these are what it produced.
#[cfg(feature = "remote")]
pub enum GqlOutcome {
	/// Refused at the route, before execution — malformed syntax, a missing
	/// capability (403) or a missing route (404). The string carries the status
	/// so the cause stays actionable.
	Rejected(String),
	/// Executed; these are its per-statement results.
	Ran(Vec<std::result::Result<SurValue, String>>),
}

/// Ask the endpoint which SurrealDB version it runs.
///
/// The runner's version gates — `[test] version`, `importing-version` and
/// `introduced` — are defined against the datastore that runs the tests, which
/// for a remote run is the server rather than this build. `/version` answers
/// without credentials and before any session exists, so a gate can be resolved
/// while the test set is still being assembled.
///
/// This is the first request a remote run makes, so over `https`/`wss` it is
/// also the process's first TLS handshake and must install the crypto provider
/// itself — [`RemoteConn::connect`] has not run yet.
pub async fn endpoint_version(endpoint: &str) -> Result<semver::Version> {
	install_crypto_provider();
	let base = http_base_from_endpoint(endpoint)?;
	let body = reqwest::Client::new()
		.get(format!("{base}/version"))
		.send()
		.await
		.with_context(|| format!("asking --endpoint `{endpoint}` for its version"))?
		.error_for_status()
		.with_context(|| format!("--endpoint `{endpoint}` refused the version request"))?
		.text()
		.await
		.with_context(|| format!("reading the version --endpoint `{endpoint}` reported"))?;

	// Rendered as `surrealdb-<semver>`.
	let raw = body.trim();
	let raw = raw.strip_prefix("surrealdb-").unwrap_or(raw);
	semver::Version::parse(raw)
		.with_context(|| format!("parsing the version --endpoint `{endpoint}` reported: `{body}`"))
}

/// The HTTP base the harness's own routes — `/version`, `/graphql`, `/gql` —
/// hang off, for an endpoint the SDK was handed: `ws`/`http` → `http`,
/// `wss`/`https` → `https`.
///
/// Derived by performing the SDK's own `Url::join("rpc")` and then dropping that
/// final segment, so the base is the RPC route's parent *by construction*. The
/// join is relative resolution, not concatenation, which is the part worth
/// deriving rather than reimplementing: a path with a trailing slash keeps its
/// prefix (`https://host/surreal/` → `/surreal/rpc`) while one without loses its
/// last segment (`https://host/surreal` → `/rpc`). Spelling that rule out in
/// string operations is how the two drift apart, and then the harness posts to a
/// mount the SDK never talks to.
///
/// A reverse-proxy prefix therefore has to be given with its trailing slash, the
/// same way the SDK needs it.
pub fn http_base_from_endpoint(endpoint: &str) -> Result<String> {
	let (scheme, rest) = endpoint
		.split_once("://")
		.context("--endpoint must include a scheme (ws:// or http://)")?;
	let http_scheme = match scheme {
		"ws" | "http" => "http",
		"wss" | "https" => "https",
		other => bail!("unsupported --endpoint scheme `{other}` (expected ws/wss/http/https)"),
	};
	if rest.is_empty() {
		bail!("--endpoint `{endpoint}` names no host");
	}

	let url = reqwest::Url::parse(&format!("{http_scheme}://{rest}"))
		.with_context(|| format!("--endpoint `{endpoint}` is not a URL"))?;
	// Exactly what the SDK connects to; `rpc` is its route constant.
	let mut rpc = url
		.join("rpc")
		.with_context(|| format!("resolving the RPC route of --endpoint `{endpoint}`"))?;
	rpc.set_query(None);
	rpc.set_fragment(None);
	rpc.path_segments_mut()
		.map_err(|()| anyhow::anyhow!("--endpoint `{endpoint}` has no path to resolve against"))?
		.pop();

	Ok(rpc.as_str().trim_end_matches('/').to_owned())
}

/// Extract per-statement results from a SDK query response into the same
/// `Vec<Result<Value, String>>` shape the embedded path produces.
pub fn extract_results(
	mut resp: surrealdb::IndexedResults,
) -> Vec<std::result::Result<SurValue, String>> {
	let n = resp.num_statements();
	// `take_errors` removes the errored entries; capture them keyed by index first.
	let mut errors = resp.take_errors();
	let mut out = Vec::with_capacity(n);
	for i in 0..n {
		if let Some(err) = errors.remove(&i) {
			out.push(Err(err.to_string()));
		} else {
			match resp.take::<SurValue>(i) {
				Ok(v) => out.push(Ok(v)),
				Err(e) => out.push(Err(e.to_string())),
			}
		}
	}
	out
}

/// Map a flatbuffers-decoded `Vec<QueryResult>` value (an array of `{ status,
/// result, ... }` objects, as produced by `QueryResult::into_value`) into the
/// per-statement result shape. `status == "ERR"` → the `result` field is the
/// error message; otherwise it is the value.
fn query_result_array_to_results(value: SurValue) -> Vec<std::result::Result<SurValue, String>> {
	let SurValue::Array(array) = value else {
		return vec![Err(format!("expected an array of query results, got: {value:?}"))];
	};
	array
		.into_iter()
		.map(|elem| {
			let SurValue::Object(mut obj) = elem else {
				return Err(format!("expected a query-result object, got: {elem:?}"));
			};
			let is_err = matches!(obj.get("status"), Some(SurValue::String(s)) if s == "ERR");
			if is_err {
				match obj.remove("result") {
					Some(SurValue::String(s)) => Err(s),
					Some(other) => Err(other.to_sql()),
					// Malformed/edge: an error envelope without a `result` message.
					// Surface the remaining fields (`kind`, etc.) rather than a bare
					// `NONE`, so the failure is at least diagnosable.
					None => Err(SurValue::Object(obj).to_sql()),
				}
			} else {
				Ok(obj.remove("result").unwrap_or(SurValue::None))
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Array, Object, Value as SurValue};

	use super::{http_base_from_endpoint, query_result_array_to_results};

	#[test]
	fn ws_and_http_map_to_http() {
		assert_eq!(
			http_base_from_endpoint("ws://127.0.0.1:8000").unwrap(),
			"http://127.0.0.1:8000"
		);
		assert_eq!(
			http_base_from_endpoint("http://127.0.0.1:8000").unwrap(),
			"http://127.0.0.1:8000"
		);
	}

	#[test]
	fn wss_and_https_map_to_https() {
		assert_eq!(
			http_base_from_endpoint("wss://db.example.com").unwrap(),
			"https://db.example.com"
		);
		assert_eq!(
			http_base_from_endpoint("https://db.example.com").unwrap(),
			"https://db.example.com"
		);
	}

	/// The SDK's ws endpoint may carry the `/rpc` route itself, which the HTTP
	/// routes must not inherit, or every `/graphql` POST would go to
	/// `/rpc/graphql`.
	///
	/// With a trailing slash it is a prefix *named* `rpc`, not the route: the SDK
	/// joins that to `/rpc/rpc`, so the base is `/rpc`. That spelling is almost
	/// certainly a mistake, but the harness following the SDK into it is what
	/// makes the resulting failure coherent instead of the two halves of the run
	/// talking to different mounts.
	#[test]
	fn the_rpc_route_is_dropped() {
		assert_eq!(
			http_base_from_endpoint("ws://127.0.0.1:8000/rpc").unwrap(),
			"http://127.0.0.1:8000"
		);
		assert_eq!(
			http_base_from_endpoint("ws://127.0.0.1:8000/rpc/").unwrap(),
			"http://127.0.0.1:8000/rpc"
		);
	}

	/// A reverse-proxy prefix is kept exactly where the SDK puts it. `Url::join`
	/// is relative resolution: a trailing slash keeps the prefix, and without one
	/// the last segment is replaced. The base has to agree with that or the
	/// harness posts to a mount the SDK never talks to — so the slashless form
	/// resolving to the host root is the SDK's behaviour being matched, not a
	/// prefix being dropped.
	#[test]
	fn the_base_is_the_rpc_route_s_parent() {
		// With the slash: the prefix is the base, as the SDK's `/surreal/rpc`.
		assert_eq!(http_base_from_endpoint("wss://host/surreal/").unwrap(), "https://host/surreal");
		assert_eq!(
			http_base_from_endpoint("wss://host/surreal/rpc").unwrap(),
			"https://host/surreal"
		);
		assert_eq!(http_base_from_endpoint("http://host:8000/a/b/").unwrap(), "http://host:8000/a/b");
		// Without it: the SDK joins to `https://host/rpc`, so the base is the root.
		assert_eq!(http_base_from_endpoint("wss://host/surreal").unwrap(), "https://host");
	}

	/// A query or fragment is not part of the route the HTTP paths hang off. The
	/// SDK's join drops both, and so does this.
	#[test]
	fn a_query_or_fragment_is_dropped() {
		// `host/surreal` has no trailing slash, so the join replaces `surreal`.
		assert_eq!(http_base_from_endpoint("ws://host/surreal?x=1").unwrap(), "http://host");
		assert_eq!(http_base_from_endpoint("ws://host/surreal/?x=1").unwrap(), "http://host/surreal");
		assert_eq!(http_base_from_endpoint("ws://host/rpc#f").unwrap(), "http://host");
	}

	/// An `ERR` envelope carries its message in `result`; every other status is a
	/// value. Getting this backwards would report a failing statement as a passing
	/// one whose value happens to be a string.
	#[test]
	fn an_err_status_becomes_the_statement_error() {
		let mut ok = Object::new();
		ok.insert("status", "OK");
		ok.insert("result", 1i64);
		let mut err = Object::new();
		err.insert("status", "ERR");
		err.insert("result", "boom");

		let value =
			SurValue::Array(Array::from_iter([SurValue::Object(ok), SurValue::Object(err)]));
		let results = query_result_array_to_results(value);

		assert_eq!(results.len(), 2);
		assert_eq!(results[0].as_ref().unwrap(), &SurValue::from_int(1));
		assert_eq!(results[1].as_ref().unwrap_err(), "boom");
	}
}
