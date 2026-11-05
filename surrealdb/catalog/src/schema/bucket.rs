use anyhow::Context as _;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use crate::catalog::{FromStored, Permission, StoredPermission};
use crate::expr::statements::define::{DefineBucketStatement, DefineKind};
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 1)]
#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Stored,
)]
#[stored(since = "4.0", transparent)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BucketId(pub u32);

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredBucketDefinition {
	#[stored(n = 2)]
	pub id: Option<BucketId>,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub backend: Option<Strand>,
	#[stored(n = 8)]
	pub permissions: StoredPermission,
	#[stored(n = 10)]
	pub readonly: bool,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	/// Identity of this bucket definition for presigned-URL signing.
	///
	/// An input to the bucket's signing key derivation, so replacing it changes
	/// that key and every URL already issued for this bucket stops verifying.
	/// It is the only revocation that does not require waiting out the URLs'
	/// own expiry or changing the node-wide `SURREAL_BUCKET_PRESIGN_SECRET`.
	///
	/// Random and never reused, rather than a counter. The value only has to be
	/// unique -- it is an input to a PRF keyed by the presign secret, so nothing
	/// is learned by reading it -- and a counter would need an invariant this
	/// row cannot uphold: `REMOVE BUCKET` deletes the row, and `DEFINE BUCKET`
	/// carries no syntax for the value, so any path that loses it must produce
	/// a value no issued URL was signed under. A fresh random value does; a
	/// counter restarting at zero re-validates every URL signed under zero.
	///
	/// Buckets written before this field existed read back as nil, which is a
	/// valid identity: it is stable, and it is still separated from every other
	/// bucket by the name and ids that share the derivation chain.
	#[revision(start = 2)]
	#[stored(n = 12, default = "Uuid::nil")]
	pub presign_nonce: Uuid,
}
impl_kv_value_catalog!(StoredBucketDefinition);

/// The level a bucket definition lives at, and the ids that locate it there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BucketLevel {
	Root,
	Ns(crate::catalog::NamespaceId),
	Db(crate::catalog::NamespaceId, crate::catalog::DatabaseId),
}

impl BucketDefinition {
	/// Gives a database bucket stored with no backend the backend it is served
	/// from: the global bucket, under `/{ns}/{db}/{bu}`. `DEFINE BUCKET` stores a
	/// backend for every bucket it writes outside an import; a bucket stored with
	/// none reads as this explicit `global://` path, so its objects stay where
	/// they were written.
	pub fn with_stored_backend(
		mut self,
		ns: crate::catalog::NamespaceId,
		db: crate::catalog::DatabaseId,
	) -> Self {
		if self.backend.is_none() {
			self.backend = Some(Strand::new(Self::legacy_global_backend(ns, db, &self.name)));
		}
		self
	}

	/// The backend of database bucket `bu` stored with none: its path in the
	/// global bucket.
	pub fn legacy_global_backend(
		ns: crate::catalog::NamespaceId,
		db: crate::catalog::DatabaseId,
		bu: &str,
	) -> String {
		format!("global://{ns}/{db}/{bu}")
	}

	/// Redacts inline credentials embedded in the bucket backend URL so that
	/// `INFO FOR DB` and `EXPORT` do not expose secret keys, tokens, or passwords.
	pub fn redacted(mut self) -> Self {
		if let Some(ref backend) = self.backend
			&& let Some(redacted) = redact_backend_url(backend.as_str())
		{
			self.backend = Some(Strand::new(redacted));
		}
		self
	}

	/// The redacted `DEFINE BUCKET` statement that defines this bucket at `base`.
	pub fn to_runtime_definition_on(&self, base: runtime::Base) -> DefineBucketStatement {
		let redacted = self.clone().redacted();
		DefineBucketStatement {
			kind: DefineKind::Default,
			name: runtime::Expr::Idiom(runtime::Idiom::field(redacted.name)),
			base,
			backend: redacted.backend.map(|v| runtime::Expr::Literal(runtime::Literal::String(v))),
			permissions: redacted.permissions,
			readonly: redacted.readonly,
			comment: redacted
				.comment
				.map(|v| runtime::Expr::Literal(runtime::Literal::String(v.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
		}
	}
}

/// Redacts userinfo passwords (`scheme://user:password@host`) and sensitive
/// query parameters (`secret_key`, `access_key`, `service_account_key`,
/// `sas_token`, etc.) in a bucket backend URL, returning `None` if no
/// credentials are present.
fn redact_backend_url(url: &str) -> Option<String> {
	let mut changed = false;

	let (before_query, query_and_fragment) = match url.find('?') {
		Some(idx) => (&url[..idx], Some(&url[idx + 1..])),
		None => (url, None),
	};

	let redacted_base = if let Some((scheme, rest)) = before_query.split_once("://") {
		let authority_end = rest.find('/').unwrap_or(rest.len());
		let authority = &rest[..authority_end];
		let path = &rest[authority_end..];
		if let Some((userinfo, host)) = authority.rsplit_once('@') {
			changed = true;
			if let Some((user, _pass)) = userinfo.split_once(':') {
				format!("{scheme}://{user}:[REDACTED]@{host}{path}")
			} else {
				format!("{scheme}://[REDACTED]@{host}{path}")
			}
		} else {
			before_query.to_string()
		}
	} else {
		before_query.to_string()
	};

	let Some(qf) = query_and_fragment else {
		return changed.then_some(redacted_base);
	};

	let (query, fragment) = match qf.find('#') {
		Some(idx) => (&qf[..idx], Some(&qf[idx..])),
		None => (qf, None),
	};

	let mut redacted_pairs = Vec::new();
	for pair in query.split('&') {
		if let Some((k, _v)) = pair.split_once('=')
			&& is_sensitive_query_param(k)
		{
			changed = true;
			redacted_pairs.push(format!("{k}=[REDACTED]"));
		} else {
			redacted_pairs.push(pair.to_string());
		}
	}

	if !changed {
		return None;
	}

	let mut out = format!("{redacted_base}?{}", redacted_pairs.join("&"));
	if let Some(frag) = fragment {
		out.push_str(frag);
	}
	Some(out)
}

fn is_sensitive_query_param(key: &str) -> bool {
	let k = key.to_ascii_lowercase();
	let k = k.as_str();
	k.starts_with("x-amz-")
		|| k.starts_with("x-goog-")
		|| k.contains("secret")
		|| k.contains("token")
		|| k.contains("sig")
		|| k.contains("key")
		|| k.contains("pass")
		|| k.contains("cred")
		|| k.contains("auth")
		|| matches!(
			k,
			"access_key"
				| "access_key_id"
				| "aws_access_key_id"
				| "secret_key"
				| "secret_access_key"
				| "aws_secret_access_key"
				| "session_token"
				| "aws_session_token"
				| "service_account_key"
				| "account_key"
				| "sas" | "sas_token"
				| "sig" | "signature"
				| "password" | "token"
				| "key" | "secret"
		)
}

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		let redacted = self.redacted();
		Value::from(map! {
			"name" => redacted.name.into(),
			"permissions" => redacted.permissions.structure(),
			"backend", if let Some(backend) = redacted.backend => backend.into(),
			"readonly" => redacted.readonly.into(),
			// Only rendered once the bucket has an identity to show. A nil
			// value carries no information, and omitting it keeps `INFO FOR DB`
			// unchanged for a deployment that predates this field.
			"presign_nonce", if !redacted.presign_nonce.is_nil() => redacted.presign_nonce.to_string().into(),
			"comment", if let Some(comment) = redacted.comment => comment.into(),
		})
	}
}

impl ToSql for BucketDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition_on(runtime::Base::Db).fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredBucketDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketDefinition {
	pub id: Option<BucketId>,
	pub readonly: bool,
	pub name: Strand,
	pub backend: Option<Strand>,
	pub permissions: Permission,
	pub comment: Option<String>,
	/// See [`StoredBucketDefinition::presign_nonce`].
	pub presign_nonce: Uuid,
}

impl FromStored for BucketDefinition {
	type Stored = StoredBucketDefinition;

	fn from_stored(stored: &StoredBucketDefinition) -> anyhow::Result<BucketDefinition> {
		fn build(stored: &StoredBucketDefinition) -> anyhow::Result<BucketDefinition> {
			Ok(BucketDefinition {
				id: stored.id,
				readonly: stored.readonly,
				name: stored.name.clone(),
				backend: stored.backend.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
				presign_nonce: stored.presign_nonce,
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of bucket `{}` no longer compiles", stored.name)
		})
	}
}

impl BucketDefinition {
	pub fn to_stored(&self) -> StoredBucketDefinition {
		StoredBucketDefinition {
			id: self.id,
			name: self.name.clone(),
			backend: self.backend.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
			readonly: self.readonly,
			comment: self.comment.clone(),
			presign_nonce: self.presign_nonce,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::redact_backend_url;

	#[test]
	fn redacts_s3_gcs_azure_credentials_in_backend_urls() {
		assert_eq!(redact_backend_url("memory"), None);
		assert_eq!(redact_backend_url("file:/some_directory"), None);
		assert_eq!(redact_backend_url("s3://host/bucket?region=us-east-1"), None);

		assert_eq!(
			redact_backend_url(
				"s3://AKIA123:supersecret@s3.amazonaws.com/my-bucket?region=us-east-1"
			),
			Some("s3://AKIA123:[REDACTED]@s3.amazonaws.com/my-bucket?region=us-east-1".to_string())
		);
		assert_eq!(
			redact_backend_url("s3://s3.amazonaws.com/my-bucket?access_key=AKIA123&secret_key=shh&region=eu-west-1"),
			Some("s3://s3.amazonaws.com/my-bucket?access_key=[REDACTED]&secret_key=[REDACTED]&region=eu-west-1".to_string())
		);
		assert_eq!(
			redact_backend_url("gs://my-bucket/prefix?service_account_key=/etc/key.json"),
			Some("gs://my-bucket/prefix?service_account_key=[REDACTED]".to_string())
		);
		assert_eq!(
			redact_backend_url("azure://acct/container?sas_token=sv%3D2022&prefix=data"),
			Some("azure://acct/container?sas_token=[REDACTED]&prefix=data".to_string())
		);
		assert_eq!(
			redact_backend_url(
				"s3://s3.amazonaws.com/my-bucket?X-Amz-Credential=AKIA123%2F20260101&X-Amz-Signature=deadbeef&aws_session_token=tok&region=us-east-1"
			),
			Some(
				"s3://s3.amazonaws.com/my-bucket?X-Amz-Credential=[REDACTED]&X-Amz-Signature=[REDACTED]&aws_session_token=[REDACTED]&region=us-east-1".to_string()
			)
		);
		assert_eq!(
			redact_backend_url(
				"gs://my-bucket/prefix?X-Goog-Credential=svc%40proj&X-Goog-Signature=sig123"
			),
			Some(
				"gs://my-bucket/prefix?X-Goog-Credential=[REDACTED]&X-Goog-Signature=[REDACTED]"
					.to_string()
			)
		);
	}
}
