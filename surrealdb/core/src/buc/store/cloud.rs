//! Cloud object-storage backends for buckets, unified on the Apache
//! [`object_store`](https://docs.rs/object_store) crate: AWS S3 (and
//! S3-compatible services such as MinIO, Backblaze B2, Wasabi and Cloudflare
//! R2), Google Cloud Storage, and Azure Blob Storage. Only compiled on
//! non-`wasm32` targets.
//!
//! The connection URLs are parsed here by hand rather than through
//! [`object_store::parse_url`] so the established SurrealDB URL contract is
//! preserved exactly:
//! - `object_store` treats the URL host as the bucket name and reads all other settings from
//!   separate options/environment variables. SurrealDB instead treats the host as an optional
//!   custom *endpoint*, the first path segment as the bucket, and accepts credentials/region/prefix
//!   inline.
//!
//! The parsed parameters are fed into `object_store`'s typed builders
//! ([`AmazonS3Builder`], [`GoogleCloudStorageBuilder`],
//! [`MicrosoftAzureBuilder`]), so the same `object_store` client powers every
//! cloud provider while the URL format stays stable.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as OsPath;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore as OsStore, ObjectStoreExt, PutPayload};
use url::Url;

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};
use crate::buc::Error;

/// Attempt to connect to a cloud object-storage backend from a `BACKEND` URL.
///
/// Returns `Ok(None)` when the URL scheme is not a recognised cloud scheme, so
/// the caller can fall through to the other backends. Returns `Err` when the
/// scheme matches but the URL is malformed or the client cannot be built.
pub(crate) fn connect(url: &str) -> Result<Option<Arc<dyn ObjectStore>>, Error> {
	let Ok(parsed) = Url::parse(url) else {
		return Ok(None);
	};

	let (store, prefix) = match parsed.scheme() {
		"s3" | "s3+http" | "s3+https" => {
			let params = S3Params::parse(&parsed)?;
			let store = params.build()?;
			(store, params.prefix)
		}
		"gs" | "gcs" => {
			let params = GcsParams::parse(&parsed)?;
			let store = params.build()?;
			(store, params.prefix)
		}
		"az" | "azure" => {
			let params = AzureParams::parse(&parsed)?;
			let store = params.build()?;
			(store, params.prefix)
		}
		_ => return Ok(None),
	};

	Ok(Some(finalize(store, prefix)))
}

/// Wrap the raw `object_store` client in a key prefix (when the URL carried
/// one) and then in the SurrealDB [`ObjectStore`] adapter.
fn finalize(store: Arc<dyn OsStore>, prefix: Option<String>) -> Arc<dyn ObjectStore> {
	match prefix {
		Some(prefix) if !prefix.trim_matches('/').is_empty() => {
			let prefixed = PrefixStore::new(store, OsPath::from(prefix.as_str()));
			Arc::new(ObjectStoreAdapter {
				inner: Arc::new(prefixed),
			})
		}
		_ => Arc::new(ObjectStoreAdapter {
			inner: store,
		}),
	}
}

/// Read a single query-parameter value from a URL.
fn query_value(url: &Url, key: &str) -> Option<String> {
	url.query_pairs().find(|(k, _)| k == key).map(|(_, value)| value.into_owned())
}

/// Read a boolean query flag. Present with no value (`?anonymous`), `=true` or
/// `=1` count as true; anything else (including absence) counts as false.
fn query_flag(url: &Url, key: &str) -> bool {
	url.query_pairs().any(|(k, v)| k == key && (v.is_empty() || v == "true" || v == "1"))
}

/// The first non-empty path segment of a URL (the bucket / container name).
fn first_path_segment(url: &Url) -> Option<String> {
	url.path().split('/').find(|segment| !segment.is_empty()).map(|segment| segment.to_string())
}

/// Parsed parameters for an S3 / S3-compatible backend.
///
/// ### URL format (unchanged from earlier SurrealDB releases)
/// - `s3://[access_key:secret_key@]hostname/bucket-name?region=&prefix=`
/// - `s3:/bucket-name?region=&prefix=&access_key=&secret_key=`
/// - `s3+http://…` / `s3+https://…` to force the endpoint protocol
///
/// A hostname (when present) is treated as a **custom endpoint** for an
/// S3-compatible service, and path-style addressing is used. With no hostname
/// the standard AWS endpoint for the region is used.
#[derive(Debug)]
struct S3Params {
	bucket: String,
	region: String,
	access_key: Option<String>,
	secret_key: Option<String>,
	endpoint: Option<String>,
	allow_http: bool,
	prefix: Option<String>,
}

impl S3Params {
	fn parse(url: &Url) -> Result<Self, Error> {
		let bucket = first_path_segment(url).ok_or_else(|| {
			Error::InvalidBucketUrl("Missing bucket name in URL path".to_string())
		})?;

		// Credentials from URL userinfo, falling back to query parameters.
		let access_key = if url.username().is_empty() {
			query_value(url, "access_key")
		} else {
			Some(url.username().to_string())
		};
		let secret_key = match url.password() {
			Some(password) => Some(password.to_string()),
			None => query_value(url, "secret_key"),
		};

		let region = query_value(url, "region").unwrap_or_else(|| "us-east-1".to_string());
		let prefix = query_value(url, "prefix");

		// A host means a custom endpoint (MinIO, R2, Backblaze, …). `host_str()`
		// omits the port, so re-append it when present (e.g. MinIO on `:9000`).
		let allow_http = url.scheme() == "s3+http";
		let endpoint = url.host_str().filter(|host| !host.is_empty()).map(|host| {
			let protocol = if allow_http {
				"http"
			} else {
				"https"
			};
			match url.port() {
				Some(port) => format!("{protocol}://{host}:{port}"),
				None => format!("{protocol}://{host}"),
			}
		});

		Ok(S3Params {
			bucket,
			region,
			access_key,
			secret_key,
			endpoint,
			allow_http,
			prefix,
		})
	}

	fn build(&self) -> Result<Arc<dyn OsStore>, Error> {
		// `from_env` seeds the standard AWS credential/config chain so instance
		// or environment credentials work when none are given in the URL.
		let mut builder = AmazonS3Builder::from_env()
			.with_bucket_name(self.bucket.as_str())
			.with_region(self.region.as_str());

		if let Some(endpoint) = &self.endpoint {
			builder = builder
				.with_endpoint(endpoint.as_str())
				.with_virtual_hosted_style_request(false)
				.with_allow_http(self.allow_http);
		}

		if let (Some(access_key), Some(secret_key)) = (&self.access_key, &self.secret_key) {
			builder = builder
				.with_access_key_id(access_key.as_str())
				.with_secret_access_key(secret_key.as_str());
		}

		let store = builder
			.build()
			.map_err(|e| Error::BucketUnavailable(format!("failed to create S3 client: {e}")))?;
		Ok(Arc::new(store))
	}
}

/// Parsed parameters for a Google Cloud Storage backend.
///
/// ### URL format
/// - `gs://bucket-name?prefix=&service_account=/path/to/key.json`
/// - `gcs://bucket-name?service_account_key=<inline-json>`
/// - `gs://bucket-name?anonymous`
///
/// The bucket is the URL host. Credentials fall back to the standard Google
/// environment variables when none are supplied.
#[derive(Debug)]
struct GcsParams {
	bucket: String,
	service_account_path: Option<String>,
	service_account_key: Option<String>,
	anonymous: bool,
	prefix: Option<String>,
}

impl GcsParams {
	fn parse(url: &Url) -> Result<Self, Error> {
		let bucket = url
			.host_str()
			.filter(|host| !host.is_empty())
			.map(|host| host.to_string())
			.ok_or_else(|| Error::InvalidBucketUrl("Missing bucket name in GCS URL".to_string()))?;

		Ok(GcsParams {
			bucket,
			service_account_path: query_value(url, "service_account"),
			service_account_key: query_value(url, "service_account_key"),
			anonymous: query_flag(url, "anonymous"),
			prefix: query_value(url, "prefix"),
		})
	}

	fn build(&self) -> Result<Arc<dyn OsStore>, Error> {
		let mut builder =
			GoogleCloudStorageBuilder::from_env().with_bucket_name(self.bucket.as_str());

		if let Some(path) = &self.service_account_path {
			builder = builder.with_service_account_path(path.as_str());
		}
		if let Some(key) = &self.service_account_key {
			builder = builder.with_service_account_key(key.as_str());
		}
		if self.anonymous {
			builder = builder.with_skip_signature(true);
		}

		let store = builder
			.build()
			.map_err(|e| Error::BucketUnavailable(format!("failed to create GCS client: {e}")))?;
		Ok(Arc::new(store))
	}
}

/// Parsed parameters for an Azure Blob Storage backend.
///
/// ### URL format
/// - `az://account/container?access_key=&prefix=`
/// - `azure://account/container?sas_token=<token>`
/// - `az://account/container?use_emulator=true` (Azurite)
/// - `az://account/container?anonymous`
///
/// The account is the URL host and the container is the first path segment.
/// Credentials fall back to the standard Azure environment variables.
#[derive(Debug)]
struct AzureParams {
	account: String,
	container: String,
	access_key: Option<String>,
	sas_token: Option<String>,
	anonymous: bool,
	use_emulator: bool,
	prefix: Option<String>,
}

impl AzureParams {
	fn parse(url: &Url) -> Result<Self, Error> {
		let account = url
			.host_str()
			.filter(|host| !host.is_empty())
			.map(|host| host.to_string())
			.ok_or_else(|| {
			Error::InvalidBucketUrl("Missing account name in Azure URL host".to_string())
		})?;

		let container = first_path_segment(url).ok_or_else(|| {
			Error::InvalidBucketUrl("Missing container name in Azure URL path".to_string())
		})?;

		Ok(AzureParams {
			account,
			container,
			access_key: query_value(url, "access_key"),
			sas_token: query_value(url, "sas_token"),
			anonymous: query_flag(url, "anonymous"),
			use_emulator: query_flag(url, "use_emulator"),
			prefix: query_value(url, "prefix"),
		})
	}

	fn build(&self) -> Result<Arc<dyn OsStore>, Error> {
		let mut builder = MicrosoftAzureBuilder::from_env()
			.with_account(self.account.as_str())
			.with_container_name(self.container.as_str());

		if let Some(access_key) = &self.access_key {
			builder = builder.with_access_key(access_key.as_str());
		}
		if let Some(sas_token) = &self.sas_token {
			let pairs: Vec<(String, String)> =
				url::form_urlencoded::parse(sas_token.trim_start_matches('?').as_bytes())
					.map(|(k, v)| (k.into_owned(), v.into_owned()))
					.collect();
			builder = builder.with_sas_authorization(pairs);
		}
		if self.anonymous {
			builder = builder.with_skip_signature(true);
		}
		if self.use_emulator {
			builder = builder.with_use_emulator(true);
		}

		let store = builder
			.build()
			.map_err(|e| Error::BucketUnavailable(format!("failed to create Azure client: {e}")))?;
		Ok(Arc::new(store))
	}
}

/// Adapts an [`object_store::ObjectStore`] to the SurrealDB [`ObjectStore`]
/// trait used by the `file::*` functions.
///
/// A URL key prefix (when present) is applied by wrapping `inner` in an
/// [`object_store::prefix::PrefixStore`] at construction time, so this adapter
/// only translates between [`ObjectKey`] and [`object_store::path::Path`] and
/// maps errors. The conditional operations (`*_if_not_exists`) and `rename` are
/// implemented here via head/copy/delete rather than `object_store`'s native
/// conditional operations, which require per-backend configuration and are not
/// uniformly supported.
struct ObjectStoreAdapter {
	inner: Arc<dyn OsStore>,
}

impl ObjectStoreAdapter {
	/// Convert an [`ObjectKey`] (leading-slash, `/`-delimited) into an
	/// `object_store` [`Path`](OsPath) (no leading slash). `OsPath::from`
	/// ignores empty segments, so the leading slash is dropped.
	fn to_path(key: &ObjectKey) -> OsPath {
		OsPath::from(key.as_str())
	}

	/// Format a backend error into the `Result<_, String>` the trait expects.
	fn err(op: &str, e: &object_store::Error) -> String {
		format!("object store {op} operation failed: {e}")
	}

	/// Whether an `object_store` error is a "not found" error.
	fn is_not_found(e: &object_store::Error) -> bool {
		matches!(e, object_store::Error::NotFound { .. })
	}
}

impl ObjectStore for ObjectStoreAdapter {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			self.inner
				.put(&path, PutPayload::from(data))
				.await
				.map_err(|e| Self::err("put", &e))?;
			Ok(())
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			match self.inner.head(&path).await {
				Ok(_) => return Ok(()),
				Err(e) if Self::is_not_found(&e) => {}
				Err(e) => return Err(Self::err("put_if_not_exists (head)", &e)),
			}
			self.inner
				.put(&path, PutPayload::from(data))
				.await
				.map_err(|e| Self::err("put_if_not_exists", &e))?;
			Ok(())
		})
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			match self.inner.get(&path).await {
				Ok(result) => {
					let bytes = result.bytes().await.map_err(|e| Self::err("get (read)", &e))?;
					Ok(Some(bytes))
				}
				Err(e) if Self::is_not_found(&e) => Ok(None),
				Err(e) => Err(Self::err("get", &e)),
			}
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			match self.inner.head(&path).await {
				Ok(meta) => Ok(Some(ObjectMeta {
					size: meta.size,
					updated: meta.last_modified,
					key: key.to_owned(),
				})),
				Err(e) if Self::is_not_found(&e) => Ok(None),
				Err(e) => Err(Self::err("head", &e)),
			}
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			// `delete` is idempotent: a missing key is not an error.
			match self.inner.delete(&path).await {
				Ok(()) => Ok(()),
				Err(e) if Self::is_not_found(&e) => Ok(()),
				Err(e) => Err(Self::err("delete", &e)),
			}
		})
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		Box::pin(async move {
			let path = Self::to_path(key);
			match self.inner.head(&path).await {
				Ok(_) => Ok(true),
				Err(e) if Self::is_not_found(&e) => Ok(false),
				Err(e) => Err(Self::err("exists", &e)),
			}
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let from = Self::to_path(key);
			let to = Self::to_path(target);
			match self.inner.copy(&from, &to).await {
				Ok(()) => Ok(()),
				Err(e) if Self::is_not_found(&e) => Ok(()),
				Err(e) => Err(Self::err("copy", &e)),
			}
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let from = Self::to_path(key);
			let to = Self::to_path(target);
			match self.inner.head(&to).await {
				Ok(_) => return Ok(()),
				Err(e) if Self::is_not_found(&e) => {}
				Err(e) => return Err(Self::err("copy_if_not_exists (head target)", &e)),
			}
			match self.inner.copy(&from, &to).await {
				Ok(()) => Ok(()),
				Err(e) if Self::is_not_found(&e) => Ok(()),
				Err(e) => Err(Self::err("copy_if_not_exists", &e)),
			}
		})
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let from = Self::to_path(key);
			let to = Self::to_path(target);
			match self.inner.copy(&from, &to).await {
				Ok(()) => {}
				Err(e) if Self::is_not_found(&e) => return Ok(()),
				Err(e) => return Err(Self::err("rename (copy)", &e)),
			}
			self.inner.delete(&from).await.map_err(|e| Self::err("rename (delete source)", &e))?;
			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let from = Self::to_path(key);
			let to = Self::to_path(target);
			match self.inner.head(&to).await {
				Ok(_) => return Ok(()),
				Err(e) if Self::is_not_found(&e) => {}
				Err(e) => return Err(Self::err("rename_if_not_exists (head target)", &e)),
			}
			match self.inner.copy(&from, &to).await {
				Ok(()) => {}
				Err(e) if Self::is_not_found(&e) => return Ok(()),
				Err(e) => return Err(Self::err("rename_if_not_exists (copy)", &e)),
			}
			self.inner
				.delete(&from)
				.await
				.map_err(|e| Self::err("rename_if_not_exists (delete source)", &e))?;
			Ok(())
		})
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let prefix_path = opts.prefix.as_ref().map(Self::to_path);
			let offset_path = opts.start.as_ref().map(Self::to_path);

			let mut stream = match &offset_path {
				Some(offset) => self.inner.list_with_offset(prefix_path.as_ref(), offset),
				None => self.inner.list(prefix_path.as_ref()),
			};

			let mut objects = Vec::new();
			while let Some(item) = stream.next().await {
				// Check the limit before consuming so `limit = 0` returns nothing.
				if let Some(limit) = opts.limit
					&& objects.len() >= limit
				{
					break;
				}
				let meta = item.map_err(|e| Self::err("list", &e))?;
				objects.push(ObjectMeta {
					key: ObjectKey::new(meta.location.to_string()),
					size: meta.size,
					updated: meta.last_modified,
				});
			}
			Ok(objects)
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn s3(url: &str) -> Result<S3Params, Error> {
		S3Params::parse(&Url::parse(url).expect("valid url"))
	}
	fn gcs(url: &str) -> Result<GcsParams, Error> {
		GcsParams::parse(&Url::parse(url).expect("valid url"))
	}
	fn azure(url: &str) -> Result<AzureParams, Error> {
		AzureParams::parse(&Url::parse(url).expect("valid url"))
	}

	/// Non-cloud schemes fall through so the memory/file backends handle them.
	#[test]
	fn connect_ignores_non_cloud_schemes() {
		for url in ["memory://", "file:///tmp/data", "http://example.com/x", "not a url"] {
			assert!(
				connect(url).expect("parse should not error").is_none(),
				"expected None for {url}"
			);
		}
	}

	// ---- S3 (contract preserved from earlier releases) ----

	#[test]
	fn s3_aws_style_uses_named_region_without_endpoint() {
		let p = s3("s3:/my-bucket?access_key=foo&secret_key=bar&region=us-west-2").unwrap();
		assert_eq!(p.bucket, "my-bucket");
		assert_eq!(p.region, "us-west-2");
		assert_eq!(p.endpoint, None);
		assert!(!p.allow_http);
		assert_eq!(p.access_key.as_deref(), Some("foo"));
		assert_eq!(p.secret_key.as_deref(), Some("bar"));
		assert_eq!(p.prefix, None);
	}

	#[test]
	fn s3_region_defaults_to_us_east_1() {
		let p = s3("s3:/my-bucket?access_key=foo&secret_key=bar").unwrap();
		assert_eq!(p.region, "us-east-1");
	}

	#[test]
	fn s3_userinfo_credentials_and_custom_https_endpoint() {
		let p = s3("s3://foo:bar@s3.eu-central-003.backblazeb2.com/my-bucket?prefix=/archive/2022")
			.unwrap();
		assert_eq!(p.bucket, "my-bucket");
		assert_eq!(p.prefix.as_deref(), Some("/archive/2022"));
		assert_eq!(p.endpoint.as_deref(), Some("https://s3.eu-central-003.backblazeb2.com"));
		assert!(!p.allow_http);
		assert_eq!(p.access_key.as_deref(), Some("foo"));
		assert_eq!(p.secret_key.as_deref(), Some("bar"));
	}

	/// Regression: the endpoint must keep a non-default port (`host_str()` drops
	/// it) so S3-compatible services such as MinIO on `:9000` work.
	#[test]
	fn s3_plus_http_preserves_port_and_allows_http() {
		let p = s3("s3+http://foo:bar@minio.local:9000/my-bucket?prefix=/tmp").unwrap();
		assert_eq!(p.bucket, "my-bucket");
		assert_eq!(p.endpoint.as_deref(), Some("http://minio.local:9000"));
		assert!(p.allow_http);
		assert_eq!(p.prefix.as_deref(), Some("/tmp"));
	}

	#[test]
	fn s3_query_parameter_credentials() {
		let p =
			s3("s3://s3.example.com/my-bucket?access_key=AKIA&secret_key=SECRET&region=eu-west-1")
				.unwrap();
		assert_eq!(p.access_key.as_deref(), Some("AKIA"));
		assert_eq!(p.secret_key.as_deref(), Some("SECRET"));
		assert_eq!(p.endpoint.as_deref(), Some("https://s3.example.com"));
	}

	#[test]
	fn s3_missing_bucket_is_error() {
		let err = s3("s3://key:secret@s3.example.com/").unwrap_err();
		assert!(matches!(err, Error::InvalidBucketUrl(_)), "unexpected error: {err}");
	}

	// ---- GCS ----

	#[test]
	fn gcs_bucket_is_host_with_service_account() {
		let p = gcs("gs://my-bucket?prefix=/data&service_account=/keys/sa.json").unwrap();
		assert_eq!(p.bucket, "my-bucket");
		assert_eq!(p.prefix.as_deref(), Some("/data"));
		assert_eq!(p.service_account_path.as_deref(), Some("/keys/sa.json"));
		assert_eq!(p.service_account_key, None);
		assert!(!p.anonymous);
	}

	#[test]
	fn gcs_scheme_alias_and_anonymous() {
		let p = gcs("gcs://my-bucket?anonymous").unwrap();
		assert_eq!(p.bucket, "my-bucket");
		assert!(p.anonymous);
	}

	#[test]
	fn gcs_missing_bucket_is_error() {
		let err = gcs("gs:///").unwrap_err();
		assert!(matches!(err, Error::InvalidBucketUrl(_)), "unexpected error: {err}");
	}

	// ---- Azure ----

	#[test]
	fn azure_account_host_and_container_path() {
		let p = azure("az://myaccount/mycontainer?access_key=KEY&prefix=/p").unwrap();
		assert_eq!(p.account, "myaccount");
		assert_eq!(p.container, "mycontainer");
		assert_eq!(p.access_key.as_deref(), Some("KEY"));
		assert_eq!(p.prefix.as_deref(), Some("/p"));
		assert!(!p.use_emulator);
	}

	#[test]
	fn azure_scheme_alias_and_emulator() {
		let p = azure("azure://devstoreaccount1/cont?use_emulator=true").unwrap();
		assert_eq!(p.account, "devstoreaccount1");
		assert_eq!(p.container, "cont");
		assert!(p.use_emulator);
	}

	#[test]
	fn azure_missing_container_is_error() {
		let err = azure("az://onlyaccount").unwrap_err();
		assert!(matches!(err, Error::InvalidBucketUrl(_)), "unexpected error: {err}");
	}
}
