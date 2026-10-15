//! Client for the Silo package host, as an installing client uses it.
//!
//! The contract is `surrealism/SILO.md`. The archive at
//!
//! ```text
//! {endpoint}/{organisation}/{package}/{major}.{minor}.{patch}.surli
//! ```
//!
//! is the source of truth, and the only URL the server ever reads. The JSON
//! package API under `{endpoint}/api/v1/packages/` is optional: it resolves
//! "latest" and describes a version without a download. When it does not
//! answer for an exact version, this client downloads the archive and inspects
//! it locally, so a plain mirror still installs.
//!
//! Every resolution ends at one exact version. A `DEFINE EXTENSION` statement
//! never holds a range, so the same statement always names the same code.

use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use bytes::{Bytes, BytesMut};
use reqwest::{Client, StatusCode, Url};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use surrealism_runtime::package::Digest;
use tracing::instrument;

use super::archive::{ArchiveFacts, preview_archive};

/// The endpoint used when `surrealism_silo_endpoint` is not set. The server
/// has the same default, and the CLI reads the same
/// `SURREAL_SURREALISM_SILO_ENDPOINT`, so that the client and the server it
/// installs into agree on the host.
pub(crate) const DEFAULT_ENDPOINT: &str = "https://silo.surrealdb.com";

/// The largest archive accepted, equal to the server's own limit: an archive
/// the server would refuse is not worth inspecting.
pub(super) const MAX_PACKAGE_BYTES: u64 = 256 * 1024 * 1024;

/// The longest one request may take in total. A stalled connection fails
/// sooner, after [`super::net::READ_TIMEOUT`] without data; this bounds only a
/// slow but steady one, and allows [`MAX_PACKAGE_BYTES`] at about 150 KB/s.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// The largest JSON body accepted from the package API.
const MAX_JSON_BYTES: u64 = 4 * 1024 * 1024;

/// Reject an organisation or package name that cannot appear verbatim in a
/// URL path segment.
///
/// The same rule as the server's in `surrealdb/core/src/surrealism/silo.rs`:
/// a name the server would refuse is refused here before any request is made,
/// and no name can reach outside the path it names.
pub(super) fn validate_segment(kind: &str, value: &str) -> Result<()> {
	if value.is_empty() {
		bail!("A silo {kind} must not be empty");
	}
	if !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
		bail!(
			"A silo {kind} may only contain ASCII letters, digits, `-` and `_`, but found `{value}`"
		);
	}
	Ok(())
}

/// Reject a version that a `DEFINE` statement cannot express.
///
/// A silo source is written `<major.minor.patch>`, so a pre-release or build
/// suffix has no spelling in a statement and can never be installed.
pub(super) fn ensure_exact(version: &Version) -> Result<()> {
	ensure!(
		version.pre.is_empty() && version.build.is_empty(),
		"A silo package version must be `major.minor.patch`, but found `{version}`"
	);
	Ok(())
}

fn parse_exact_version(value: &str) -> Result<Version> {
	let version = Version::parse(value)
		.with_context(|| format!("`{value}` is not a `major.minor.patch` version"))?;
	ensure_exact(&version)?;
	Ok(version)
}

fn exact_version<'de, D: Deserializer<'de>>(de: D) -> Result<Version, D::Error> {
	let value = String::deserialize(de)?;
	parse_exact_version(&value).map_err(serde::de::Error::custom)
}

fn optional_exact_version<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Version>, D::Error> {
	Option::<String>::deserialize(de)?
		.map(|value| parse_exact_version(&value).map_err(serde::de::Error::custom))
		.transpose()
}

/// The digest that pins a package version: `sha256:` and the lowercase hex
/// SHA-256 of the archive's `surrealism/MANIFEST`. It is the value a
/// `DEFINE EXTENSION … DIGEST` clause holds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub(crate) struct ManifestDigest(String);

impl From<&Digest> for ManifestDigest {
	fn from(digest: &Digest) -> Self {
		Self(digest.to_string())
	}
}

impl ManifestDigest {
	pub(crate) fn as_str(&self) -> &str {
		&self.0
	}
}

impl TryFrom<String> for ManifestDigest {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self> {
		let valid = value.strip_prefix("sha256:").is_some_and(|hex| {
			hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
		});
		ensure!(
			valid,
			"`{value}` is not a digest of the form `sha256:` and 64 lowercase hex digits"
		);
		Ok(Self(value))
	}
}

impl std::fmt::Display for ManifestDigest {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.0)
	}
}

/// What a person asked to install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VersionRequest {
	/// The highest version that is not yanked. Answered only by the API.
	Latest,
	/// One exact version.
	Exact(Version),
}

/// `GET {endpoint}/api/v1/packages/{org}/{pkg}`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PackageIndex {
	/// The highest version that is not yanked, or `None` when every version is.
	#[serde(default, deserialize_with = "optional_exact_version")]
	pub latest: Option<Version>,
	#[serde(default)]
	pub versions: Vec<VersionEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VersionEntry {
	#[serde(deserialize_with = "exact_version")]
	pub version: Version,
	pub digest: ManifestDigest,
}

/// `GET {endpoint}/api/v1/packages/{org}/{pkg}/{x.y.z}`, or the same facts
/// read from the archive itself.
///
/// The API also describes the release's requests and exports (see SILO.md).
/// An installing client reads those from the archive it verifies, so they
/// are not read here, and their shape cannot fail an install.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PackageDetails {
	pub digest: ManifestDigest,
	/// Who signed the manifest. Always `None` for a locally inspected
	/// archive: the raw signature is in [`ArchiveFacts::signature`].
	#[serde(default)]
	pub signer: Option<Signer>,
	#[serde(default)]
	pub yanked: bool,
	#[serde(default)]
	pub advisories: Vec<Advisory>,
}

/// Who Silo says signed the release: a Sigstore identity (`issuer`,
/// `identity` and the numeric IDs) or an SSH `key`. Every field is optional,
/// so a signer shape this client does not know reads as one that matches
/// nothing rather than failing the install.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct Signer {
	pub issuer: Option<String>,
	pub identity: Option<String>,
	pub repository_id: Option<String>,
	pub owner_id: Option<String>,
	/// An SSH public key, `<type> <base64> [comment]`.
	pub key: Option<String>,
	/// The signer matches a publisher registered for this package on Silo.
	pub registered_publisher: bool,
}

impl Signer {
	/// Whether Silo names exactly the signer the archive's signature proved:
	/// every Sigstore field, or the key's type and blob.
	pub(crate) fn matches(&self, proved: &super::signer::Signer) -> bool {
		use super::signer::{Signer as Proved, key_type_and_blob};
		match proved {
			Proved::Sigstore(s) => {
				self.key.is_none()
					&& self.issuer.as_deref() == Some(s.issuer.as_str())
					&& self.identity.as_deref() == Some(s.identity.as_str())
					&& self.repository_id == s.repository_id
					&& self.owner_id == s.owner_id
			}
			Proved::Key(key) => {
				self.issuer.is_none()
					&& self.key.as_deref().map(key_type_and_blob) == Some(key_type_and_blob(key))
			}
		}
	}

	/// One line for the prompt.
	pub(crate) fn describe(&self) -> String {
		match (&self.key, &self.identity) {
			(Some(key), _) => format!("key {}", super::signer::ssh_key_fingerprint(key)),
			(None, Some(identity)) => super::signer::describe_identity(identity),
			(None, None) => "an unrecognised signer".into(),
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Advisory {
	pub id: String,
	pub summary: String,
	#[serde(default)]
	pub url: Option<String>,
}

/// Where a [`Resolved`]'s details came from.
#[derive(Debug)]
pub(crate) enum DetailsSource {
	/// The package API answered.
	Api,
	/// The archive was downloaded and inspected locally. `bytes` is the
	/// archive as downloaded, so a caller that verifies its signature need not
	/// fetch it again.
	Archive {
		facts: Box<ArchiveFacts>,
		bytes: Bytes,
	},
}

/// A request resolved to one exact version, with what the install prompt
/// shows about it.
#[derive(Debug)]
pub(crate) struct Resolved {
	pub organisation: String,
	pub package: String,
	pub version: Version,
	pub details: PackageDetails,
	pub source: DetailsSource,
}

impl Resolved {
	/// The source as a `DEFINE EXTENSION … FROM` clause writes it.
	pub(crate) fn source_clause(&self) -> String {
		let Version {
			major,
			minor,
			patch,
			..
		} = self.version;
		format!("silo::{}::{}<{major}.{minor}.{patch}>", self.organisation, self.package)
	}

	/// What the install prompt must show before asking for approval. None of
	/// these refuse the install: a yanked version still installs when it was
	/// asked for by exact version.
	pub(crate) fn warnings(&self) -> Vec<String> {
		let mut warnings = Vec::new();
		if self.details.yanked {
			warnings.push(format!("{} has been yanked by its publisher", self.source_clause()));
		}
		for advisory in &self.details.advisories {
			let mut line = format!("Advisory {}: {}", advisory.id, advisory.summary);
			if let Some(url) = &advisory.url {
				line.push_str(&format!(" ({url})"));
			}
			warnings.push(line);
		}
		warnings
	}
}

/// Refuse an archive whose package file names a different package than the
/// URL it was fetched from.
fn check_coordinates(
	facts: &ArchiveFacts,
	organisation: &str,
	package: &str,
	version: &Version,
) -> Result<()> {
	ensure!(
		facts.organisation == organisation && facts.name == package && &facts.version == version,
		"The archive for `{organisation}/{package}` {version} describes itself as `{}/{}` {}",
		facts.organisation,
		facts.name,
		facts.version
	);
	Ok(())
}

/// The same facts the package API reports, read from the archive.
///
/// A local archive cannot say whether it was yanked, has advisories, or was
/// signed by a registered publisher: only Silo knows those.
fn details_from(facts: &ArchiveFacts) -> PackageDetails {
	PackageDetails {
		digest: ManifestDigest::from(&facts.digest),
		signer: None,
		yanked: false,
		advisories: Vec::new(),
	}
}

/// Resolves and describes packages against one Silo endpoint.
pub(crate) struct SiloClient {
	/// The endpoint, checked to hold no credentials, so it may appear in
	/// errors and trace spans.
	endpoint: String,
	http: Client,
	/// [`MAX_PACKAGE_BYTES`]; tests lower it.
	max_package_bytes: u64,
	/// [`MAX_JSON_BYTES`]; tests lower it.
	max_json_bytes: u64,
}

impl SiloClient {
	/// A client for `endpoint`, with any trailing `/` removed. The endpoint
	/// must be HTTPS (plain HTTP only on loopback) and hold no credentials;
	/// see [`super::net`].
	pub(crate) fn new(endpoint: &str) -> Result<Self> {
		let url = super::net::check_endpoint("silo endpoint", endpoint)?;
		let endpoint = endpoint.trim_end_matches('/').to_string();
		let http = super::net::client(REQUEST_TIMEOUT, url.scheme() == "http")?;
		Ok(Self {
			endpoint,
			http,
			max_package_bytes: MAX_PACKAGE_BYTES,
			max_json_bytes: MAX_JSON_BYTES,
		})
	}

	fn url(&self, path: &str) -> Result<Url> {
		let url = format!("{}/{path}", self.endpoint);
		Url::parse(&url).with_context(|| format!("Invalid silo URL `{url}`"))
	}

	/// The archive URL, the only one the server reads.
	pub(crate) fn archive_url(
		&self,
		organisation: &str,
		package: &str,
		version: &Version,
	) -> Result<Url> {
		validate_segment("organisation", organisation)?;
		validate_segment("package", package)?;
		ensure_exact(version)?;
		self.url(&format!("{organisation}/{package}/{version}.surli"))
	}

	/// Resolve a request to one exact version and describe it.
	///
	/// [`VersionRequest::Latest`] needs the package API, and fails with a hint
	/// to name an exact version when the endpoint does not answer. An exact
	/// version is described by [`Self::details`].
	#[instrument(level = "debug", skip(self), fields(endpoint = %self.endpoint))]
	pub(crate) async fn resolve(
		&self,
		organisation: &str,
		package: &str,
		request: &VersionRequest,
	) -> Result<Resolved> {
		let index = match request {
			VersionRequest::Exact(version) => {
				return self.details(organisation, package, version).await;
			}
			VersionRequest::Latest => self.versions(organisation, package).await?,
		};
		let Some(index) = index else {
			bail!(
				"The package `{organisation}/{package}` was not found at `{}`. An endpoint without the package API, such as a mirror, can only install an exact version: name one, as `silo::{organisation}::{package}<x.y.z>`",
				self.endpoint
			);
		};
		let Some(version) = index.latest else {
			bail!(
				"Every version of `{organisation}/{package}` has been yanked; name an exact version to install one anyway"
			);
		};
		let listed = index.versions.into_iter().find(|v| v.version == version);
		let resolved = self.details(organisation, package, &version).await?;
		if let Some(listed) = listed {
			ensure!(
				listed.digest == resolved.details.digest,
				"The silo endpoint `{}` reports two digests for `{organisation}/{package}` {version}: {} in the version list and {} in its details",
				self.endpoint,
				listed.digest,
				resolved.details.digest
			);
		}
		Ok(resolved)
	}

	/// List a package's versions, or `None` when the API does not answer.
	#[instrument(level = "debug", skip(self), fields(endpoint = %self.endpoint))]
	pub(crate) async fn versions(
		&self,
		organisation: &str,
		package: &str,
	) -> Result<Option<PackageIndex>> {
		validate_segment("organisation", organisation)?;
		validate_segment("package", package)?;
		let url = self.url(&format!("api/v1/packages/{organisation}/{package}"))?;
		self.get_json(&url).await
	}

	/// Describe one exact version: from the package API when it answers, and
	/// otherwise from the archive, downloaded and inspected locally.
	///
	/// Only a `404` from the archive URL means the version does not exist.
	#[instrument(level = "debug", skip(self), fields(endpoint = %self.endpoint))]
	pub(crate) async fn details(
		&self,
		organisation: &str,
		package: &str,
		version: &Version,
	) -> Result<Resolved> {
		let archive_url = self.archive_url(organisation, package, version)?;
		let api_url = self.url(&format!("api/v1/packages/{organisation}/{package}/{version}"))?;
		let resolved = |details, source| Resolved {
			organisation: organisation.to_string(),
			package: package.to_string(),
			version: version.clone(),
			details,
			source,
		};

		if let Some(details) = self.get_json::<PackageDetails>(&api_url).await? {
			return Ok(resolved(details, DetailsSource::Api));
		}

		let bytes = self.archive(organisation, package, version).await?;
		let facts = preview_archive(bytes.clone())
			.await
			.with_context(|| format!("Failed to inspect the package at `{archive_url}`"))?;
		check_coordinates(&facts, organisation, package, version)?;
		let details = details_from(&facts);
		Ok(resolved(
			details,
			DetailsSource::Archive {
				facts: Box::new(facts),
				bytes,
			},
		))
	}

	/// Download one exact version's archive from the URL the server reads.
	///
	/// A `404` is reported as "not found"; the body is capped at the server's
	/// own package limit.
	#[instrument(level = "debug", skip(self), fields(endpoint = %self.endpoint))]
	pub(crate) async fn archive(
		&self,
		organisation: &str,
		package: &str,
		version: &Version,
	) -> Result<Bytes> {
		let url = self.archive_url(organisation, package, version)?;
		let Some(bytes) = self.get(&url, self.max_package_bytes).await? else {
			bail!(
				"The package `silo::{organisation}::{package}<{version}>` was not found at `{}`",
				self.endpoint
			);
		};
		Ok(bytes)
	}

	async fn get_json<T: DeserializeOwned>(&self, url: &Url) -> Result<Option<T>> {
		let Some(body) = self.get(url, self.max_json_bytes).await? else {
			return Ok(None);
		};
		serde_json::from_slice(&body)
			.map(Some)
			.with_context(|| format!("Invalid response from the silo package API at `{url}`"))
	}

	/// Fetch `url` with a size ceiling. `None` on `404`; an error on any other
	/// status that is not a success.
	async fn get(&self, url: &Url, max_bytes: u64) -> Result<Option<Bytes>> {
		let mut response = self
			.http
			.get(url.clone())
			.send()
			.await
			.with_context(|| format!("Failed to fetch `{url}`"))?;
		let status = response.status();
		if status == StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !status.is_success() {
			bail!("Failed to fetch `{url}`: HTTP status {status}");
		}
		read_capped(&mut response, max_bytes).await.map(Some)
	}
}

/// The body of `response`, refused once it grows past `max_bytes`, so a
/// server cannot make the client buffer more than that.
pub(super) async fn read_capped(response: &mut reqwest::Response, max_bytes: u64) -> Result<Bytes> {
	let url = response.url().clone();
	if let Some(len) = response.content_length() {
		ensure!(len <= max_bytes, "`{url}` is {len} bytes, over the {max_bytes} byte limit");
	}
	// `content-length` is advisory, so the cap is enforced as the body grows.
	let mut body =
		BytesMut::with_capacity(response.content_length().unwrap_or(0).min(max_bytes) as usize);
	while let Some(chunk) =
		response.chunk().await.with_context(|| format!("Failed to read `{url}`"))?
	{
		ensure!(
			body.len() as u64 + chunk.len() as u64 <= max_bytes,
			"`{url}` is over the {max_bytes} byte limit"
		);
		body.extend_from_slice(&chunk);
	}
	Ok(body.freeze())
}

#[cfg(test)]
mod tests {
	use serde_json::json;
	use surrealdb_types::Kind;
	use surrealism_runtime::exports::{ExportsManifest, FunctionExport};
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	use super::*;

	const WASM: &[u8] = &super::super::archive::fixture::COMPONENT_PREAMBLE;
	const WASM_PATH: &str = "surrealism/mod.wasm";
	const PACKAGE_TOML_PATH: &str = "surrealism/package.toml";
	const LEGACY_TOML_PATH: &str = "surrealism/surrealism.toml";
	const EXPORTS_PATH: &str = "surrealism/exports.toml";

	const DIGEST: &str = "sha256:5d1a000000000000000000000000000000000000000000000000000000000000";

	fn package_toml(version: &str) -> String {
		format!(
			r#"
[package]
organisation = "acme"
name = "color"
version = "{version}"

[capabilities]
allow_net = ["api.color.io:443"]
allow_functions = ["http::get"]
max_execution_time = 5000
"#
		)
	}

	fn exports_toml() -> String {
		ExportsManifest {
			functions: vec![
				FunctionExport {
					name: Some("blend".into()),
					args: vec![("a".into(), Kind::String), ("b".into(), Kind::String)],
					returns: Kind::String,
					args_text: None,
					returns_text: None,
					writeable: false,
					declares_transaction: false,
					comment: Some("Blend two colours".into()),
				},
				FunctionExport {
					name: Some("recolor".into()),
					args: vec![],
					returns: Kind::None,
					args_text: None,
					returns_text: None,
					writeable: true,
					declares_transaction: true,
					comment: None,
				},
			],
		}
		.to_toml()
		.unwrap()
	}

	/// A `.surli` holding these files and their manifest.
	fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
		super::super::archive::fixture::pack(files, None)
	}

	fn package_files(version: &str) -> Vec<(&'static str, Vec<u8>)> {
		vec![
			(WASM_PATH, WASM.to_vec()),
			(PACKAGE_TOML_PATH, package_toml(version).into_bytes()),
			(EXPORTS_PATH, exports_toml().into_bytes()),
		]
	}

	fn package_archive(version: &str) -> Vec<u8> {
		let files = package_files(version);
		archive(&files.iter().map(|(p, d)| (*p, d.as_slice())).collect::<Vec<_>>())
	}

	/// The digest of [`package_files`]' manifest.
	fn expected_digest(version: &str) -> ManifestDigest {
		let files = package_files(version);
		let files: Vec<_> = files.iter().map(|(p, d)| (*p, d.as_slice())).collect();
		ManifestDigest::from(&super::super::archive::fixture::manifest(&files).digest())
	}

	fn details_json(yanked: bool) -> serde_json::Value {
		json!({
			"digest": DIGEST,
			"signer": {
				"issuer": "https://token.actions.githubusercontent.com",
				"identity": "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.2.0",
				"repository_id": "812345678",
				"owner_id": "98765432",
				"registered_publisher": true
			},
			"requests": { "net": ["api.color.io:443"], "functions": ["http::get"], "write": true },
			"exports": [
				{ "name": "blend", "args": [{ "name": "a", "kind": "string" }], "returns": "string", "writes": false },
				{ "name": "recolor", "args": [], "returns": "none", "writes": true, "comment": null }
			],
			"yanked": yanked,
			"advisories": [],
			"field_added_later": { "ignored": true }
		})
	}

	async fn mock_json_text(server: &MockServer, at: &str, body: String) {
		Mock::given(method("GET"))
			.and(path(at))
			.respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
			.mount(server)
			.await;
	}

	async fn mock_json(server: &MockServer, at: &str, body: serde_json::Value) {
		Mock::given(method("GET"))
			.and(path(at))
			.respond_with(ResponseTemplate::new(200).set_body_json(body))
			.mount(server)
			.await;
	}

	/// Fail the test if the archive is ever downloaded.
	async fn forbid_archive(server: &MockServer, version: &str) {
		Mock::given(method("GET"))
			.and(path(format!("/acme/color/{version}.surli")))
			.respond_with(ResponseTemplate::new(500))
			.expect(0)
			.mount(server)
			.await;
	}

	#[tokio::test]
	async fn resolves_latest_from_the_api() {
		let server = MockServer::start().await;
		mock_json(
			&server,
			"/api/v1/packages/acme/color",
			json!({
				"latest": "1.2.0",
				"versions": [
					{ "version": "1.2.0", "digest": DIGEST, "yanked": false, "published": "2026-10-01T12:00:00Z" },
					{ "version": "1.1.0", "digest": "sha256:0b7e000000000000000000000000000000000000000000000000000000000000", "yanked": true }
				]
			}),
		)
		.await;
		mock_json(&server, "/api/v1/packages/acme/color/1.2.0", details_json(false)).await;
		forbid_archive(&server, "1.2.0").await;

		let client = SiloClient::new(&format!("{}/", server.uri())).unwrap();
		let resolved = client.resolve("acme", "color", &VersionRequest::Latest).await.unwrap();

		assert_eq!(resolved.version, Version::new(1, 2, 0));
		assert_eq!(resolved.source_clause(), "silo::acme::color<1.2.0>");
		assert!(matches!(resolved.source, DetailsSource::Api));
		assert_eq!(resolved.details.digest.as_str(), DIGEST);
		let signer = resolved.details.signer.as_ref().unwrap();
		assert!(signer.registered_publisher);
		assert_eq!(signer.repository_id.as_deref(), Some("812345678"));
		assert!(resolved.warnings().is_empty());
	}

	#[tokio::test]
	async fn latest_refuses_a_digest_that_disagrees_with_the_list() {
		let server = MockServer::start().await;
		mock_json(
			&server,
			"/api/v1/packages/acme/color",
			json!({ "latest": "1.2.0", "versions": [
				{ "version": "1.2.0", "digest": "sha256:ffff000000000000000000000000000000000000000000000000000000000000" }
			]}),
		)
		.await;
		mock_json(&server, "/api/v1/packages/acme/color/1.2.0", details_json(false)).await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.resolve("acme", "color", &VersionRequest::Latest).await.unwrap_err();
		assert!(err.to_string().contains("two digests"), "{err:#}");
	}

	#[tokio::test]
	async fn resolves_an_exact_version_from_the_api() {
		let server = MockServer::start().await;
		mock_json(&server, "/api/v1/packages/acme/color/1.2.0", details_json(false)).await;
		forbid_archive(&server, "1.2.0").await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let request = VersionRequest::Exact(Version::new(1, 2, 0));
		let resolved = client.resolve("acme", "color", &request).await.unwrap();

		assert_eq!(resolved.version, Version::new(1, 2, 0));
		assert!(matches!(resolved.source, DetailsSource::Api));
		assert_eq!(resolved.details.digest.as_str(), DIGEST);
	}

	#[tokio::test]
	async fn falls_back_to_the_archive_without_the_api() {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/acme/color/1.2.0.surli"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(package_archive("1.2.0")))
			.expect(1)
			.mount(&server)
			.await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let request = VersionRequest::Exact(Version::new(1, 2, 0));
		let resolved = client.resolve("acme", "color", &request).await.unwrap();

		let DetailsSource::Archive {
			facts,
			bytes,
		} = &resolved.source
		else {
			panic!("expected the archive to be inspected");
		};
		assert_eq!(bytes.as_ref(), package_archive("1.2.0").as_slice());
		assert_eq!(resolved.details.digest, expected_digest("1.2.0"));
		assert!(facts.signature.is_none());
		assert!(resolved.details.signer.is_none());
		assert_eq!(facts.requests.net, vec!["api.color.io:443".to_string()]);
		assert_eq!(facts.requests.timeout, Some(Duration::from_secs(5)));
		assert!(facts.requests.write);
		let blend = &facts.exports[0];
		assert_eq!(blend.name.as_deref(), Some("blend"));
		assert_eq!(blend.args, vec![("a".into(), "string".into()), ("b".into(), "string".into())]);
		assert_eq!(blend.comment.as_deref(), Some("Blend two colours"));
		assert!(facts.exports[1].writes);
	}

	/// A redirect that leaves the endpoint's own loopback origin, here to
	/// another port, is not followed; to plain HTTP it never is (see
	/// `net::tests`).
	#[tokio::test]
	async fn a_redirect_off_the_origin_is_refused() {
		let elsewhere = MockServer::start().await;
		Mock::given(method("GET"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(package_archive("1.2.0")))
			.expect(0)
			.mount(&elsewhere)
			.await;
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/acme/color/1.2.0.surli"))
			.respond_with(
				ResponseTemplate::new(302).insert_header(
					"location",
					format!("{}/acme/color/1.2.0.surli", elsewhere.uri()),
				),
			)
			.mount(&server)
			.await;
		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.archive("acme", "color", &Version::new(1, 2, 0)).await.unwrap_err();
		assert!(format!("{err:#}").contains("302"), "{err:#}");
	}

	#[tokio::test]
	async fn responses_over_the_caps_are_refused() {
		let server = MockServer::start().await;
		// A package API answer over MAX_JSON_BYTES.
		let huge = format!(
			r#"{{"digest": "{DIGEST}", "padding": "{}"}}"#,
			"x".repeat(MAX_JSON_BYTES as usize)
		);
		mock_json_text(&server, "/api/v1/packages/acme/color/1.2.0", huge).await;
		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.details("acme", "color", &Version::new(1, 2, 0)).await.unwrap_err();
		assert!(format!("{err:#}").contains("byte limit"), "{err:#}");

		// An archive over MAX_PACKAGE_BYTES, lowered here so the test stays small.
		Mock::given(method("GET"))
			.and(path("/acme/color/1.2.0.surli"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 4096]))
			.mount(&server)
			.await;
		let mut client = SiloClient::new(&server.uri()).unwrap();
		assert_eq!(client.max_package_bytes, MAX_PACKAGE_BYTES);
		client.max_package_bytes = 1024;
		let err = client.archive("acme", "color", &Version::new(1, 2, 0)).await.unwrap_err();
		assert!(format!("{err:#}").contains("byte limit"), "{err:#}");
	}

	#[test]
	fn an_endpoint_with_credentials_or_plain_http_is_refused() {
		let err = SiloClient::new("https://user:hunter2@mirror.example").err().unwrap();
		assert!(!format!("{err:#}").contains("hunter2"), "{err:#}");
		assert!(SiloClient::new("http://mirror.example").is_err());
		assert!(SiloClient::new("http://127.0.0.1:9000").is_ok(), "loopback may use http");
	}

	#[tokio::test]
	async fn the_archive_downloads_from_the_static_url() {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/acme/color/1.2.0.surli"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(package_archive("1.2.0")))
			.expect(1)
			.mount(&server)
			.await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let bytes = client.archive("acme", "color", &Version::new(1, 2, 0)).await.unwrap();
		assert_eq!(bytes.as_ref(), package_archive("1.2.0").as_slice());

		let err = client.archive("acme", "color", &Version::new(9, 9, 9)).await.unwrap_err();
		assert!(err.to_string().contains("was not found"), "{err:#}");
		assert!(client.archive("acme", "../x", &Version::new(1, 2, 0)).await.is_err());
	}

	#[tokio::test]
	async fn latest_without_the_api_asks_for_an_exact_version() {
		let server = MockServer::start().await;
		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.resolve("acme", "color", &VersionRequest::Latest).await.unwrap_err();
		assert!(err.to_string().contains("silo::acme::color<x.y.z>"), "{err:#}");
	}

	#[tokio::test]
	async fn an_exact_version_missing_everywhere_is_not_found() {
		let server = MockServer::start().await;
		let client = SiloClient::new(&server.uri()).unwrap();
		let request = VersionRequest::Exact(Version::new(9, 9, 9));
		let err = client.resolve("acme", "color", &request).await.unwrap_err();
		assert!(err.to_string().contains("was not found"), "{err:#}");
	}

	#[tokio::test]
	async fn an_api_failure_is_not_hidden_by_the_fallback() {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/api/v1/packages/acme/color/1.2.0"))
			.respond_with(ResponseTemplate::new(503))
			.mount(&server)
			.await;
		forbid_archive(&server, "1.2.0").await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.details("acme", "color", &Version::new(1, 2, 0)).await.unwrap_err();
		assert!(err.to_string().contains("503"), "{err:#}");
	}

	#[tokio::test]
	async fn a_yanked_version_resolves_with_a_warning() {
		let server = MockServer::start().await;
		let mut details = details_json(true);
		details["advisories"] =
			json!([{ "id": "SILO-2026-0007", "summary": "blend() panics on empty input" }]);
		mock_json(&server, "/api/v1/packages/acme/color/1.2.0", details).await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let request = VersionRequest::Exact(Version::new(1, 2, 0));
		let resolved = client.resolve("acme", "color", &request).await.unwrap();

		assert!(resolved.details.yanked);
		assert_eq!(
			resolved.warnings(),
			[
				"silo::acme::color<1.2.0> has been yanked by its publisher",
				"Advisory SILO-2026-0007: blend() panics on empty input",
			]
		);
	}

	#[tokio::test]
	async fn every_version_yanked_leaves_no_latest() {
		let server = MockServer::start().await;
		mock_json(
			&server,
			"/api/v1/packages/acme/color",
			json!({ "latest": null, "versions": [
				{ "version": "1.1.0", "digest": DIGEST, "yanked": true }
			]}),
		)
		.await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.resolve("acme", "color", &VersionRequest::Latest).await.unwrap_err();
		assert!(err.to_string().contains("yanked"), "{err:#}");
	}

	#[tokio::test]
	async fn a_version_with_a_suffix_is_refused() {
		let server = MockServer::start().await;
		mock_json(
			&server,
			"/api/v1/packages/acme/color",
			json!({ "latest": "2.0.0-rc.1", "versions": [] }),
		)
		.await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.resolve("acme", "color", &VersionRequest::Latest).await.unwrap_err();
		assert!(format!("{err:#}").contains("major.minor.patch"), "{err:#}");

		let request = VersionRequest::Exact(Version::parse("2.0.0-rc.1").unwrap());
		assert!(client.resolve("acme", "color", &request).await.is_err());
	}

	#[tokio::test]
	async fn an_archive_naming_another_package_is_refused() {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/acme/color/1.2.0.surli"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(package_archive("1.3.0")))
			.mount(&server)
			.await;

		let client = SiloClient::new(&server.uri()).unwrap();
		let err = client.details("acme", "color", &Version::new(1, 2, 0)).await.unwrap_err();
		assert!(err.to_string().contains("describes itself as"), "{err:#}");
	}

	#[test]
	fn segments_are_validated_before_any_request() {
		let client = SiloClient::new("https://silo.example.com").unwrap();
		let version = Version::new(1, 0, 0);
		for bad in ["", "..", "a/b", "a b", "a%2Fb", "ä"] {
			assert!(client.archive_url(bad, "color", &version).is_err(), "organisation `{bad}`");
			assert!(client.archive_url("acme", bad, &version).is_err(), "package `{bad}`");
		}
		assert_eq!(
			client.archive_url("acme", "color_2-x", &version).unwrap().as_str(),
			"https://silo.example.com/acme/color_2-x/1.0.0.surli"
		);
	}

	#[test]
	fn the_legacy_package_file_is_read_when_alone() {
		let files = [
			(WASM_PATH, WASM.to_vec()),
			(LEGACY_TOML_PATH, package_toml("1.2.0").into_bytes()),
			(EXPORTS_PATH, exports_toml().into_bytes()),
		];
		let bytes = archive(&files.iter().map(|(p, d)| (*p, d.as_slice())).collect::<Vec<_>>());

		let facts = super::super::archive::preview_bytes(&bytes).unwrap();
		assert_eq!(facts.version, Version::new(1, 2, 0));
	}

	#[test]
	fn digests_must_be_well_formed() {
		assert!(ManifestDigest::try_from(DIGEST.to_string()).is_ok());
		for bad in [
			"5d1a",
			"sha256:5D1A000000000000000000000000000000000000000000000000000000000000",
			"sha512:5d1a000000000000000000000000000000000000000000000000000000000000",
			"sha256:5d1a",
		] {
			assert!(ManifestDigest::try_from(bad.to_string()).is_err(), "{bad}");
		}
	}
}
