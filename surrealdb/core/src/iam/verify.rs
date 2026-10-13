use std::str::{self, FromStr};
use std::sync::{Arc, Once};

use anyhow::{Result, bail};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use chrono::Utc;
use jsonwebtoken::{DecodingKey, TokenData, Validation, decode};
use surrealdb_types::ToSql;

use crate::catalog::Error as CatalogError;
use crate::catalog::providers::{
	AuthorisationProvider, DatabaseProvider, NamespaceProvider, UserProvider,
};
use crate::dbs::Session;
use crate::err::Error;
#[cfg(feature = "jwks")]
use crate::expr::Error as ExprError;
use crate::iam::access::{
	access_session, authenticate_generic, authenticate_record, reset_previous_principal,
	session_data_for_record,
};
use crate::iam::issue::expiration;
#[cfg(feature = "jwks")]
use crate::iam::jwks;
use crate::iam::token::Claims;
use crate::iam::{self, Actor, Auth, Error as AuthError, Level, Role};
use crate::kvs::Datastore;
use crate::kvs::TransactionType::*;
use crate::types::PublicValue;
use crate::{catalog, syn};

/// Returns the decoding key as wel as the method by which to verify the key against
fn decode_key(alg: catalog::Algorithm, key: &[u8]) -> Result<(DecodingKey, Validation)> {
	let (dec, mut val) = match alg {
		catalog::Algorithm::Hs256 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS256))
		}
		catalog::Algorithm::Hs384 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS384))
		}
		catalog::Algorithm::Hs512 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS512))
		}
		catalog::Algorithm::EdDSA => {
			(DecodingKey::from_ed_pem(key)?, Validation::new(jsonwebtoken::Algorithm::EdDSA))
		}
		catalog::Algorithm::Es256 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES256))
		}
		catalog::Algorithm::Es384 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		catalog::Algorithm::Es512 => {
			static ES512_WARN: Once = Once::new();
			ES512_WARN.call_once(|| {
				warn!("ES512 is not currently supported by the underlying cryptography library and will fall back to ES384. Please update your access definition to use ES384 or another supported algorithm.");
			});
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		catalog::Algorithm::Ps256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS256))
		}
		catalog::Algorithm::Ps384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS384))
		}
		catalog::Algorithm::Ps512 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS512))
		}
		catalog::Algorithm::Rs256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS256))
		}
		catalog::Algorithm::Rs384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS384))
		}
		catalog::Algorithm::Rs512 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS512))
		}
	};

	// Without configured audiences the `aud` claim is not validated, matching
	// access methods that do not define an AUDIENCE clause. Callers enable
	// validation via `apply_audience_validation` when the access method
	// defines an audience list.
	val.validate_aud = false;

	Ok((dec, val))
}

/// Enables audience validation when the access method defines an audience
/// list: the token must carry an `aud` claim (string or array) that
/// intersects the configured values. Access methods without configured
/// audiences ignore the claim entirely.
fn apply_audience_validation(val: &mut Validation, jwt: &catalog::JwtAccess) {
	if let Some(audience) = &jwt.audience {
		val.set_audience(audience);
		val.validate_aud = true;
		// A configured audience makes the claim mandatory; otherwise a
		// token that simply omits `aud` would bypass the restriction.
		val.required_spec_claims.insert("aud".to_string());
	}
}

/// Decodes JWT claims without cryptographic verification.
///
/// SAFETY: This is used exclusively to extract routing information (namespace,
/// database, access method) needed to look up the correct verification key.
/// The token MUST be fully verified via [`verify_token`] before any claims
/// are trusted for authorization decisions.
///
/// A malformed token yields [`AuthError::InvalidAuth`]: the decoder's own
/// account of what was wrong with the token stays in the debug log and never
/// reaches the caller.
fn decode_claims_unverified(token: &str) -> Result<TokenData<Claims>, AuthError> {
	jsonwebtoken::dangerous::insecure_decode::<Claims>(token).map_err(|err| {
		debug!("Error decoding authentication token claims: {err}");
		err.into()
	})
}

/// The actor identity for a session authenticated by a non-record access
/// method.
///
/// `Auth::id()` is the identity half of every principal fingerprint the engine
/// takes — the MCP transport's per-request subject re-check, and
/// `AuthPrincipalSnapshot`, which tears a stream down when its session stops
/// acting as the principal it was opened for. Naming the access method alone
/// makes that identity identical for every holder of a token issued by it, so
/// two different users of one `DEFINE ACCESS` are indistinguishable to both.
///
/// The token's own `sub` claim is therefore folded in, scoped by the access
/// method so subjects cannot collide across methods. `sub` is the standard JWT
/// subject and, unlike `id`, is not part of the claim shape that selects record
/// or user authentication, so it is free to name the principal here.
///
/// The access name is length-prefixed rather than bare-joined with `/`, because
/// an access name is an ident that may itself contain `/` (`DEFINE ACCESS
/// `a/b``). A bare `"{access}/{sub}"` would then be ambiguous — access `a` with
/// subject `b/c` and access `a/b` with subject `c` would both render `a/b/c` —
/// which is exactly the cross-method collision this identity exists to prevent.
/// The `<len>:` prefix fixes the access span, so the encoding is injective over
/// `(access, sub)` and over `(access, no-sub)`.
///
/// A token carrying no `sub` names the access method alone, which is the most
/// that can be said about it — so every holder of a sub-less token for one
/// method shares an actor identity, and the per-subject protections that
/// fingerprint `Auth::id()` (the MCP transport's session binding,
/// `AuthPrincipalSnapshot`) cannot tell those holders apart. That is a property
/// of the token, not something this function can repair, so it warns (once per
/// process, since `token` runs per request on stateless transports) rather than
/// inventing an identity: an operator relying on subject binding must issue
/// tokens with a `sub` claim. Callers that do not rely on it are unaffected.
fn access_actor_id(access: &str, claims: &Claims) -> String {
	match claims.sub.as_deref() {
		Some(subject) => format!("{}:{access}/{subject}", access.len()),
		None => {
			// `token` runs on every request that carries a token, so warn at most
			// once per process rather than per request. Mirrors the ES512 notice.
			static SUBLESS_WARN: Once = Once::new();
			SUBLESS_WARN.call_once(|| {
				warn!(
					"Access method '{access}' authenticated a token with no 'sub' claim: every sub-less token for an access method shares one actor identity, so per-subject session binding (such as the MCP transport's) cannot distinguish their holders. Issue tokens with a 'sub' claim to identify the principal."
				);
			});
			format!("{}:{access}", access.len())
		}
	}
}

pub async fn basic(
	kvs: &Datastore,
	session: &mut Session,
	user: &str,
	pass: &str,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<()> {
	// Log the authentication type
	trace!("Attempting basic authentication");
	// Check if the parameters exist
	match (ns, db) {
		// DB signin
		(Some(ns), Some(db)) => match verify_db_creds(kvs, ns, db, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as database user '{}'", user);
				// Every fallible step is taken before the session is touched: a
				// partial failure must leave the session in its
				// pre-authentication state.
				let session_exp = expiration(u.session_duration)?;
				let au = Auth::new(Actor::from_role_names(
					u.name.to_string(),
					&u.roles,
					Level::Database(ns.to_owned(), db.to_owned()),
				)?);
				reset_previous_principal(session);
				session.exp = session_exp;
				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		// NS signin
		(Some(ns), None) => match verify_ns_creds(kvs, ns, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as namespace user '{}'", user);
				// Every fallible step is taken before the session is touched: a
				// partial failure must leave the session in its
				// pre-authentication state.
				let session_exp = expiration(u.session_duration)?;
				let au = Auth::new(Actor::from_role_names(
					u.name.to_string(),
					&u.roles,
					Level::Namespace(ns.to_owned()),
				)?);
				reset_previous_principal(session);
				session.exp = session_exp;
				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		// Root signin
		(None, None) => match verify_root_creds(kvs, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as root user '{}'", user);
				// Every fallible step is taken before the session is touched: a
				// partial failure must leave the session in its
				// pre-authentication state.
				let session_exp = expiration(u.session_duration)?;
				let au =
					Auth::new(Actor::from_role_names(u.name.to_string(), &u.roles, Level::Root)?);
				reset_previous_principal(session);
				session.exp = session_exp;
				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		(None, Some(db)) => {
			debug!(
				"Attempted basic authentication in database '{db}' without specifying a namespace"
			);
			Err(anyhow::Error::new(AuthError::InvalidAuth))
		}
	}
}

pub async fn token(kvs: &Datastore, session: &mut Session, token: &str) -> Result<()> {
	// Log the authentication type
	trace!("Attempting token authentication");
	// Decode the token without verifying to extract routing claims
	let token_data = decode_claims_unverified(token)?;
	// Convert the token to a SurrealQL object value
	let value = crate::val::Value::from(token_data.claims.clone().into_claims_object());
	// Check if the auth token can be used
	if let Some(nbf) = token_data.claims.nbf
		&& nbf > Utc::now().timestamp()
	{
		debug!("Token verification failed due to the 'nbf' claim containing a future time");
		bail!(AuthError::InvalidAuth);
	}
	// Check if the auth token has expired
	if let Some(exp) = token_data.claims.exp
		&& exp < Utc::now().timestamp()
	{
		debug!("Token verification failed due to the 'exp' claim containing a past time");
		bail!(AuthError::ExpiredToken);
	}
	// Check the token authentication claims
	match &token_data.claims {
		// Check if this is record access
		Claims {
			ns: Some(ns),
			db: Some(db),
			ac: Some(ac),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating with record access method `{}`", ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			let db_def = match catch!(tx, tx.get_db_by_name(ns, db, None).await) {
				Some(db) => db,
				None => {
					let _ = tx.cancel().await;
					return Err(CatalogError::DbNotFound {
						name: db.clone(),
					}
					.into());
				}
			};
			// Parse the record id
			let mut rid = match syn::record_id(id) {
				Ok(rid) => rid,
				Err(e) => {
					let _ = tx.cancel().await;
					return Err(e);
				}
			};
			// Get the database access method
			let Some(de) = catch!(
				tx,
				tx.get_db_access(db_def.namespace_id, db_def.database_id, ac, None).await
			) else {
				let _ = tx.cancel().await;
				return Err(CatalogError::AccessDbNotFound {
					ac: ac.clone(),
					ns: ns.clone(),
					db: db.clone(),
				}
				.into());
			};
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Record(at) => {
					let mut cf = match &at.jwt.verify {
						catalog::JwtAccessVerify::Key(key) => {
							iam::verify::decode_key(key.alg, key.key.as_bytes())
						}
						#[cfg(feature = "jwks")]
						catalog::JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
									name: "token".to_string(),
									message: "Missing token header 'kid'".to_string(),
								}))
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => bail!(AuthError::AccessMethodMismatch),
					}?;
					apply_audience_validation(&mut cf.1, &at.jwt);
					cf
				}
				_ => bail!(AuthError::AccessMethodMismatch),
			};
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for finding the signin record
				let mut sess = access_session(&de, Some(ns.as_str()), Some(db.as_str()), session)?;
				sess.rd = Some(
					crate::val::convert_value_to_public_value(crate::val::Value::RecordId(
						rid.clone().into(),
					))
					.expect("record id conversion should succeed"),
				);
				sess.tk = Some(
					crate::val::convert_value_to_public_value(
						token_data.claims.clone().into_claims_object().into(),
					)
					.expect("claims conversion should succeed"),
				);
				rid = authenticate_record(kvs, &sess, au).await?;
			}
			// CONTEXT clause. Evaluated after the identity has settled — and
			// after `verify_token` above — so `$auth` is the final record and
			// an unverified token never pays for it. Independent of the
			// AUTHENTICATE clause: an access method may define either.
			let data = session_data_for_record(
				kvs,
				&de,
				ns.as_str(),
				db.as_str(),
				PublicValue::RecordId(rid.clone()),
				&token_data.claims,
				session,
			)
			.await?;
			// Log the success
			debug!("Authenticated with record access method `{}`", ac);
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.data = data;
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.ac = Some(ac.to_owned());
			session.rd = Some(
				crate::val::convert_value_to_public_value(crate::val::Value::RecordId(
					rid.clone().into(),
				))
				.expect("record id conversion should succeed"),
			);
			session.exp = session_exp;
			session.au = Arc::new(Auth::new(Actor::new(
				rid.to_sql(),
				Default::default(),
				Level::Record(ns.clone(), db.clone(), rid.to_sql()),
			)));
			Ok(())
		}
		// Check if this is database access
		// This can also be record access with an authenticate clause
		Claims {
			ns: Some(ns),
			db: Some(db),
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with access method `{}`", db, ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			let db_def = match catch!(tx, tx.get_db_by_name(ns, db, None).await) {
				Some(db) => db,
				None => {
					let _ = tx.cancel().await;
					return Err(CatalogError::DbNotFound {
						name: db.clone(),
					}
					.into());
				}
			};

			// Get the database access method
			let de = catch!(
				tx,
				tx.get_db_access(db_def.namespace_id, db_def.database_id, ac, None).await
			);
			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(CatalogError::AccessDbNotFound {
					ac: ac.clone(),
					ns: ns.clone(),
					db: db.clone(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			match &de.access_type {
				// If the access type is Jwt or Bearer, this is database access
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => {
					let mut cf = match &jwt.verify {
						catalog::JwtAccessVerify::Key(key) => {
							decode_key(key.alg, key.key.as_bytes())
						}
						#[cfg(feature = "jwks")]
						catalog::JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
									name: "token".to_string(),
									message: "Missing token header 'kid'".to_string(),
								}))
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => bail!(AuthError::AccessMethodMismatch),
					}?;
					apply_audience_validation(&mut cf.1, jwt);
					// Verify the token
					verify_token(token, &cf.0, &cf.1)?;
					// AUTHENTICATE clause
					if let Some(au) = &de.authenticate {
						// Setup the system session for executing the clause
						let mut sess =
							access_session(&de, Some(ns.as_str()), Some(db.as_str()), session)?;
						sess.tk = Some(
							crate::val::convert_value_to_public_value(
								token_data.claims.clone().into_claims_object().into(),
							)
							.expect("claims conversion should succeed"),
						);
						authenticate_generic(kvs, &sess, au).await?;
					}
					// Parse the roles
					let roles = match &token_data.claims.roles {
						// If no role is provided, grant the viewer role
						None => vec![Role::Viewer],
						// If roles are provided, parse them
						Some(roles) => roles
							.iter()
							.map(|r| -> Result<Role> {
								Role::from_str(r.as_str())
									.map_err(Error::IamError)
									.map_err(anyhow::Error::new)
							})
							.collect::<Result<Vec<_>, _>>()?,
					};
					// Log the success
					debug!("Authenticated to database `{}` with access method `{}`", db, ac);
					// Every fallible step is taken before the session is touched: a partial
					// failure must leave the session in its pre-authentication state.
					let session_exp = expiration(de.session_duration)?;
					// Set the session
					reset_previous_principal(session);
					session.tk = Some(
						crate::val::convert_value_to_public_value(value)
							.expect("value conversion should succeed"),
					);
					session.ns = Some(ns.to_owned());
					session.db = Some(db.to_owned());
					session.ac = Some(ac.to_owned());
					session.exp = session_exp;
					session.au = Arc::new(Auth::new(Actor::new(
						access_actor_id(&de.name, &token_data.claims),
						roles,
						Level::Database(ns.clone(), db.clone()),
					)));
				}
				// If the access type is Record, this is record access
				// Record access without an "id" claim is only possible if there is an AUTHENTICATE
				// clause The clause can make up for the missing "id" claim by resolving other
				// claims to a specific record
				catalog::AccessType::Record(at) => match &de.authenticate {
					Some(au) => {
						trace!("Access method `{}` is record access with AUTHENTICATE clause", ac);
						let mut cf = match &at.jwt.verify {
							catalog::JwtAccessVerify::Key(key) => {
								decode_key(key.alg, key.key.as_bytes())
							}
							#[cfg(feature = "jwks")]
							catalog::JwtAccessVerify::Jwks(jwks) => {
								if let Some(kid) = token_data.header.kid {
									jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
								} else {
									Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
										name: "token".to_string(),
										message: "Missing token header 'kid'".to_string(),
									}))
								}
							}
							#[cfg(not(feature = "jwks"))]
							_ => bail!(AuthError::AccessMethodMismatch),
						}?;
						apply_audience_validation(&mut cf.1, &at.jwt);

						// Verify the token
						verify_token(token, &cf.0, &cf.1)?;
						// AUTHENTICATE clause
						// Setup the system session for finding the signin record
						let mut sess =
							access_session(&de, Some(ns.as_str()), Some(db.as_str()), session)?;
						sess.tk = Some(
							crate::val::convert_value_to_public_value(
								token_data.claims.clone().into_claims_object().into(),
							)
							.expect("claims conversion should succeed"),
						);
						let rid = authenticate_record(kvs, &sess, au).await?;
						// CONTEXT clause, evaluated with the settled identity.
						let data = session_data_for_record(
							kvs,
							&de,
							ns.as_str(),
							db.as_str(),
							PublicValue::RecordId(rid.clone()),
							&token_data.claims,
							session,
						)
						.await?;
						// Log the success
						debug!("Authenticated with record access method `{}`", ac);
						// Every fallible step is taken before the session is touched: a partial
						// failure must leave the session in its pre-authentication state.
						let session_exp = expiration(de.session_duration)?;
						// Set the session
						reset_previous_principal(session);
						session.tk = Some(
							crate::val::convert_value_to_public_value(value)
								.expect("value conversion should succeed"),
						);
						session.data = data;
						session.ns = Some(ns.to_owned());
						session.db = Some(db.to_owned());
						session.ac = Some(ac.to_owned());
						session.rd = Some(
							crate::val::convert_value_to_public_value(crate::val::Value::RecordId(
								rid.clone().into(),
							))
							.expect("record id conversion should succeed"),
						);
						session.exp = session_exp;
						session.au = Arc::new(Auth::new(Actor::new(
							rid.to_sql(),
							Default::default(),
							Level::Record(ns.clone(), db.clone(), rid.to_sql()),
						)));
					}
					_ => bail!(AuthError::AccessMethodMismatch),
				},
			};
			Ok(())
		}
		// Check if this is database authentication with user credentials
		Claims {
			ns: Some(ns),
			db: Some(db),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with user `{}`", db, id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			let db_def = match catch!(tx, tx.get_db_by_name(ns, db, None).await) {
				Some(db) => db,
				None => {
					let _ = tx.cancel().await;
					return Err(CatalogError::DbNotFound {
						name: db.clone(),
					}
					.into());
				}
			};

			// Get the database user
			let de = match catch!(
				tx,
				tx.get_db_user(db_def.namespace_id, db_def.database_id, id, None).await.map_err(
					|e| {
						debug!("Error while authenticating to database `{db}`: {e}");
						anyhow::Error::new(AuthError::InvalidAuth)
					}
				)
			) {
				Some(de) => de,
				None => {
					let _ = tx.cancel().await;
					return Err(AuthError::InvalidAuth.into());
				}
			};
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with user `{}` using token", db, id);
			let roles: Vec<Role> = de
				.roles
				.iter()
				.map(|e| Role::from_str(e).map_err(Error::from))
				.collect::<Result<_, _>>()?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.exp = session_exp;
			session.au = Arc::new(Auth::new(Actor::new(
				id.clone(),
				roles,
				Level::Database(ns.clone(), db.clone()),
			)));
			Ok(())
		}
		// Check if this is namespace access
		Claims {
			ns: Some(ns),
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with access method `{}`", ns, ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			let ns_def = match catch!(tx, tx.get_ns_by_name(ns, None).await) {
				Some(ns) => ns,
				None => {
					let _ = tx.cancel().await;
					return Err(CatalogError::NsNotFound {
						name: ns.clone(),
					}
					.into());
				}
			};

			// Get the namespace access method
			let de = catch!(tx, tx.get_ns_access(ns_def.namespace_id, ac, None).await);
			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(CatalogError::AccessNsNotFound {
					ac: ac.clone(),
					ns: ns.clone(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => {
					let mut cf = match &jwt.verify {
						catalog::JwtAccessVerify::Key(key) => {
							decode_key(key.alg, key.key.as_bytes())
						}
						#[cfg(feature = "jwks")]
						catalog::JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								bail!(ExprError::InvalidFunctionArguments {
									name: "token".to_string(),
									message: "Missing token header 'kid'".to_string()
								})
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => bail!(AuthError::AccessMethodMismatch),
					}?;
					apply_audience_validation(&mut cf.1, jwt);
					cf
				}
				_ => bail!(AuthError::AccessMethodMismatch),
			};
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = access_session(&de, Some(ns.as_str()), None, session)?;
				sess.tk = Some(
					crate::val::convert_value_to_public_value(
						token_data.claims.clone().into_claims_object().into(),
					)
					.expect("claims conversion should succeed"),
				);
				authenticate_generic(kvs, &sess, au).await?;
			}
			// Parse the roles
			let roles = match &token_data.claims.roles {
				// If no role is provided, grant the viewer role
				None => vec![Role::Viewer],
				// If roles are provided, parse them
				Some(roles) => roles
					.iter()
					.map(|r| -> Result<Role> {
						Role::from_str(r.as_str())
							.map_err(Error::IamError)
							.map_err(anyhow::Error::new)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			debug!("Authenticated to namespace `{}` with access method `{}`", ns, ac);
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.ns = Some(ns.to_owned());
			session.ac = Some(ac.to_owned());
			session.exp = session_exp;
			session.au = Arc::new(Auth::new(Actor::new(
				access_actor_id(&de.name, &token_data.claims),
				roles,
				Level::Namespace(ns.clone()),
			)));
			Ok(())
		}
		// Check if this is namespace authentication with user credentials
		Claims {
			ns: Some(ns),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with user `{}`", ns, id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			let ns_def = match catch!(tx, tx.get_ns_by_name(ns, None).await) {
				Some(ns) => ns,
				None => {
					let _ = tx.cancel().await;
					return Err(CatalogError::NsNotFound {
						name: ns.clone(),
					}
					.into());
				}
			};
			// Get the namespace user
			let de = match catch!(
				tx,
				tx.get_ns_user(ns_def.namespace_id, id, None).await.map_err(|e| {
					debug!("Error while authenticating to namespace `{ns}`: {e}");
					anyhow::Error::new(AuthError::InvalidAuth)
				})
			) {
				Some(de) => de,
				None => {
					let _ = tx.cancel().await;
					return Err(AuthError::InvalidAuth.into());
				}
			};
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to namespace `{}` with user `{}` using token", ns, id);
			let roles: Vec<Role> = de
				.roles
				.iter()
				.map(|e| Role::from_str(e).map_err(Error::from))
				.collect::<Result<_, _>>()?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.ns = Some(ns.to_owned());
			session.exp = session_exp;
			session.au =
				Arc::new(Auth::new(Actor::new(id.clone(), roles, Level::Namespace(ns.clone()))));
			Ok(())
		}
		// Check if this is root access
		Claims {
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to root with access method `{}`", ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			// Get the root access method
			let de = catch!(tx, tx.get_root_access(ac, None).await);

			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(CatalogError::AccessRootNotFound {
					ac: ac.clone(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => {
					let mut cf = match &jwt.verify {
						catalog::JwtAccessVerify::Key(key) => {
							decode_key(key.alg, key.key.as_bytes())
						}
						#[cfg(feature = "jwks")]
						catalog::JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								bail!(ExprError::InvalidFunctionArguments {
									name: "token".to_string(),
									message: "Missing token header 'kid'".to_string()
								})
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => bail!(AuthError::AccessMethodMismatch),
					}?;
					apply_audience_validation(&mut cf.1, jwt);
					cf
				}
				_ => bail!(AuthError::AccessMethodMismatch),
			};
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = access_session(&de, None, None, session)?;
				sess.tk = Some(
					crate::val::convert_value_to_public_value(
						token_data.claims.clone().into_claims_object().into(),
					)
					.expect("claims conversion should succeed"),
				);
				authenticate_generic(kvs, &sess, au).await?;
			}
			// Parse the roles
			let roles = match &token_data.claims.roles {
				// If no role is provided, grant the viewer role
				None => vec![Role::Viewer],
				// If roles are provided, parse them
				Some(roles) => roles
					.iter()
					.map(|r| -> Result<Role> {
						Role::from_str(r.as_str())
							.map_err(Error::IamError)
							.map_err(anyhow::Error::new)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			debug!("Authenticated to root with access method `{}`", ac);
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.ac = Some(ac.to_owned());
			session.exp = session_exp;
			session.au = Arc::new(Auth::new(Actor::new(
				access_actor_id(&de.name, &token_data.claims),
				roles,
				Level::Root,
			)));
			Ok(())
		}
		// Check if this is root authentication with user credentials
		Claims {
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to root level with user `{}`", id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read).await?;
			// Get the namespace user
			let de = catch!(
				tx,
				tx.expect_root_user(id).await.map_err(|e| {
					debug!("Error while authenticating to root: {e}");
					anyhow::Error::new(AuthError::InvalidAuth)
				})
			);
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to root level with user `{}` using token", id);
			let roles: Vec<Role> = de
				.roles
				.iter()
				.map(|e| Role::from_str(e).map_err(Error::from))
				.collect::<Result<_, _>>()?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(de.session_duration)?;
			// Set the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(value)
					.expect("value conversion should succeed"),
			);
			session.exp = session_exp;
			session.au = Arc::new(Auth::new(Actor::new(id.clone(), roles, Level::Root)));
			Ok(())
		}
		// There was an auth error
		_ => Err(anyhow::Error::new(AuthError::InvalidAuth)),
	}
}

pub async fn verify_root_creds(
	ds: &Datastore,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read).await?;
	// Fetch the specified user from storage
	let user = catch!(
		tx,
		tx.expect_root_user(user).await.map_err(|e| {
			debug!("Error retrieving user for authentication to root: {e}");
			anyhow::Error::new(AuthError::InvalidAuth)
		})
	);
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

pub async fn verify_ns_creds(
	ds: &Datastore,
	ns: &str,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read).await?;
	let ns_def = match catch!(tx, tx.get_ns_by_name(ns, None).await) {
		Some(ns) => ns,
		None => {
			let _ = tx.cancel().await;
			return Err(CatalogError::NsNotFound {
				name: ns.to_string(),
			}
			.into());
		}
	};

	// Fetch the specified user from storage
	let user = catch!(
		tx,
		tx.get_ns_user(ns_def.namespace_id, user, None).await.map_err(|e| {
			debug!("Error retrieving user for authentication to namespace `{ns}`: {e}");
			anyhow::Error::new(AuthError::InvalidAuth)
		})
	);
	let user = match user {
		Some(user) => user,
		None => {
			let _ = tx.cancel().await;
			return Err(AuthError::InvalidAuth.into());
		}
	};
	// Ensure that the transaction is cancelled
	tx.cancel().await?;

	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

pub async fn verify_db_creds(
	ds: &Datastore,
	ns: &str,
	db: &str,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read).await?;
	let db_def = match catch!(tx, tx.get_db_by_name(ns, db, None).await) {
		Some(db) => db,
		None => {
			let _ = tx.cancel().await;
			return Err(CatalogError::DbNotFound {
				name: db.to_string(),
			}
			.into());
		}
	};

	// Fetch the specified user from storage
	let user = catch!(
		tx,
		tx.get_db_user(db_def.namespace_id, db_def.database_id, user, None).await.map_err(|e| {
			debug!("Error retrieving user for authentication to database `{ns}/{db}`: {e}");
			anyhow::Error::new(AuthError::InvalidAuth)
		})
	);
	let user = match user {
		Some(user) => user,
		None => {
			let _ = tx.cancel().await;
			return Err(AuthError::InvalidAuth.into());
		}
	};
	// Ensure that the transaction is cancelled
	tx.cancel().await?;

	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

/// A resolved SCRAM verifier plus the identity needed to establish the session
/// once a transport has verified the client proof. Returned by [`scram_lookup`].
///
/// SCRAM is challenge-response, so — unlike [`basic`] — the transport drives the
/// exchange itself (using [`ScramAuth::credential`]) and only then calls
/// [`ScramAuth::apply`] to authenticate the session. Password verification never
/// happens here; possession of a valid client proof is the proof of identity.
pub struct ScramAuth {
	credential: catalog::ScramCredential,
	user: catalog::UserDefinition,
	level: Level,
}

impl ScramAuth {
	/// The per-user SCRAM salt, sent to the client in the server-first message.
	pub fn salt(&self) -> &[u8] {
		&self.credential.salt
	}

	/// The PBKDF2 iteration count, sent to the client in the server-first message.
	pub fn iterations(&self) -> u32 {
		self.credential.iterations
	}

	/// Verify a client proof against this verifier for the caller-assembled
	/// `AuthMessage`. Returns `true` when the proof is valid.
	pub fn verify_client_proof(&self, auth_message: &[u8], client_proof: &[u8]) -> bool {
		self.credential.verify_client_proof(auth_message, client_proof)
	}

	/// The server signature (`v=`) for the caller-assembled `AuthMessage`, proving
	/// the server also holds the verifier.
	pub fn server_signature(&self, auth_message: &[u8]) -> [u8; 32] {
		self.credential.server_signature(auth_message)
	}

	/// Establish `session.au` / `session.exp` for the SCRAM-verified user. Call
	/// only after the client proof has been verified via [`Self::verify_client_proof`].
	pub fn apply(&self, session: &mut Session) -> Result<()> {
		// Every fallible step is taken before the session is touched: a partial
		// failure must leave the session in its pre-authentication state.
		let session_exp = expiration(self.user.session_duration)?;
		let au = Auth::new(Actor::from_role_names(
			self.user.name.to_string(),
			&self.user.roles,
			self.level.clone(),
		)?);
		reset_previous_principal(session);
		session.exp = session_exp;
		session.au = Arc::new(au);
		Ok(())
	}
}

/// Resolve a user's SCRAM verifier for the given scope, trying database, then
/// namespace, then root level (mirroring the fallback a transport with no
/// explicit auth-level field uses). Returns the first level at which the named
/// user exists *and* has SCRAM material, or `None` when none does — in which
/// case the transport falls back to another mechanism (e.g. cleartext against
/// the Argon2 hash). Does not verify any password.
pub async fn scram_lookup(
	kvs: &Datastore,
	user: &str,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<Option<ScramAuth>> {
	let tx = kvs.transaction(Read).await?;
	let result = scram_lookup_inner(&tx, user, ns, db).await;
	let _ = tx.cancel().await;
	result
}

async fn scram_lookup_inner(
	tx: &crate::kvs::Transaction,
	user: &str,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<Option<ScramAuth>> {
	// Database level.
	if let (Some(ns), Some(db)) = (ns, db)
		&& let Some(db_def) = tx.get_db_by_name(ns, db, None).await?
		&& let Some(u) = tx.get_db_user(db_def.namespace_id, db_def.database_id, user, None).await?
		&& let Some(scram) = u.scram.clone()
	{
		return Ok(Some(ScramAuth {
			credential: scram,
			user: (*u).clone(),
			level: Level::Database(ns.to_owned(), db.to_owned()),
		}));
	}
	// Namespace level.
	if let Some(ns) = ns
		&& let Some(ns_def) = tx.get_ns_by_name(ns, None).await?
		&& let Some(u) = tx.get_ns_user(ns_def.namespace_id, user, None).await?
		&& let Some(scram) = u.scram.clone()
	{
		return Ok(Some(ScramAuth {
			credential: scram,
			user: (*u).clone(),
			level: Level::Namespace(ns.to_owned()),
		}));
	}
	// Root level.
	if let Some(u) = tx.get_root_user(user, None).await?
		&& let Some(scram) = u.scram.clone()
	{
		return Ok(Some(ScramAuth {
			credential: scram,
			user: (*u).clone(),
			level: Level::Root,
		}));
	}
	Ok(None)
}

fn verify_pass(pass: &str, hash: &str) -> Result<()> {
	// Compute the hash and verify the password
	let hash =
		PasswordHash::new(hash).map_err(|e| anyhow::anyhow!("Invalid password hash: {}", e))?;
	// Attempt to verify the password using Argon2
	match Argon2::default().verify_password(pass.as_ref(), &hash) {
		Ok(_) => Ok(()),
		_ => Err(anyhow::Error::new(AuthError::InvalidPass)),
	}
}

fn verify_token(token: &str, key: &DecodingKey, validation: &Validation) -> Result<()> {
	match decode::<Claims>(token, key, validation) {
		Ok(_) => Ok(()),
		Err(err) => {
			// Only transparently return certain token verification errors
			match err.kind() {
				jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
					Err(anyhow::Error::new(AuthError::ExpiredToken))
				}
				_ => {
					debug!("Error verifying authentication token: {err}");
					Err(anyhow::Error::new(AuthError::InvalidAuth))
				}
			}
		}
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use argon2::password_hash::{PasswordHasher, SaltString};
	use chrono::Duration;
	use jsonwebtoken::{EncodingKey, encode};
	use rand_core::OsRng;
	use rstest::rstest;

	use super::*;
	use crate::err::exec_error;
	use crate::exec::Error as ExecError;
	use crate::iam::token::{Audience, HEADER};
	use crate::sql::statements::define::DefineKind;
	use crate::sql::statements::define::user::PassType;
	use crate::sql::{Ast, Literal};

	struct TestLevel {
		level: &'static str,
		ns: Option<&'static str>,
		db: Option<&'static str>,
	}

	const AVAILABLE_ROLES: [Role; 3] = [Role::Viewer, Role::Editor, Role::Owner];

	#[rstest]
	#[case::without_roles_or_expiration("pass", vec![Role::Viewer], None, true)]
	#[case::with_roles_and_expiration("pass", vec![Role::Editor, Role::Owner], Some(Duration::days(1)), true)]
	#[case::with_invalid_password("invalid", vec![], None, false)]
	#[tokio::test]
	async fn test_basic(
		#[case] password: &'static str,
		#[case] roles: Vec<Role>,
		#[case] expiration: Option<Duration>,
		#[case] expect_ok: bool,
		#[values(
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		)]
		level: TestLevel,
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		let roles_clause = if roles.is_empty() {
			String::new()
		} else {
			let roles: Vec<&str> = roles
				.iter()
				.map(|r| match r {
					Role::Viewer => "VIEWER",
					Role::Editor => "EDITOR",
					Role::Owner => "OWNER",
					_ => unreachable!("unknown role"),
				})
				.collect();
			format!("ROLES {}", roles.join(", "))
		};

		let duration_clause = if let Some(duration) = expiration {
			format!("DURATION FOR SESSION {}s", duration.num_seconds())
		} else {
			String::new()
		};

		let define_user_query = format!(
			"DEFINE USER user ON {} PASSWORD 'pass' {} {}",
			level.level, roles_clause, duration_clause,
		);

		ds.execute(&define_user_query, &sess, None).await.unwrap();

		let mut sess = Session {
			ns: level.ns.map(String::from),
			db: level.db.map(String::from),
			..Default::default()
		};

		let res = basic(&ds, &mut sess, "user", password, level.ns, level.db).await;

		if expect_ok {
			assert!(res.is_ok(), "Failed to signin: {:?}", res);
			assert_eq!(sess.au.id(), "user");

			// Check auth level
			assert_eq!(sess.au.level().ns(), level.ns);
			assert_eq!(sess.au.level().db(), level.db);
			match level.level {
				"ROOT" => assert!(sess.au.is_root()),
				"NS" => assert!(sess.au.is_ns()),
				"DB" => assert!(sess.au.is_db()),
				_ => panic!("Unsupported level"),
			}

			// Check roles
			for role in AVAILABLE_ROLES {
				let has_role = sess.au.has_role(role);
				let should_have_role = roles.contains(&role);
				assert_eq!(has_role, should_have_role, "Role {role:?} check failed");
			}

			// Check expiration
			if let Some(exp_duration) = expiration {
				let exp = sess.exp.unwrap();
				let min_exp = (Utc::now() + exp_duration - Duration::seconds(10)).timestamp();
				let max_exp = (Utc::now() + exp_duration + Duration::seconds(10)).timestamp();
				assert!(
					exp > min_exp && exp < max_exp,
					"Session expiration is expected to match the defined duration"
				);
			} else {
				assert_eq!(sess.exp, None, "Expiration is expected to be None");
			}
		} else {
			assert!(res.is_err(), "Unexpected successful signin");
		}
	}

	#[tokio::test]
	async fn test_basic_nonexistent_role() {
		use crate::iam::PolicyError;
		use crate::sql::statements::define::{DefineStatement, DefineUserStatement};
		use crate::sql::{Base, Expr, TopLevelExpr};
		let test_levels = vec![
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		];

		for level in &test_levels {
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");

			let base = match level.level {
				"ROOT" => Base::Root,
				"NS" => Base::Ns,
				"DB" => Base::Db,
				_ => panic!("Unsupported level"),
			};

			let user = DefineUserStatement {
				kind: DefineKind::Default,
				base,
				name: crate::sql::Expr::Idiom(crate::sql::Idiom::field("user".to_string())),
				// This is the Argon2id hash for "pass" with a random salt.
				pass_type: PassType::Hash(
					"$argon2id$v=19$m=16,t=2,p=1$VUlHTHVOYjc5d0I1dGE3OQ$sVtmRNH+Xtiijk0uXL2+4w"
						.to_string(),
				),
				scram: None,
				roles: vec!["nonexistent".to_owned()],
				token_duration: Expr::Literal(Literal::None),
				session_duration: Expr::Literal(Literal::None),
				comment: Expr::Literal(Literal::None),
			};

			let ast = Ast {
				expressions: vec![TopLevelExpr::Expr(Expr::Define(Box::new(
					DefineStatement::User(user),
				)))],
			};

			// Use pre-parsed definition, which bypasses the existent role check during parsing.
			ds.process(ast, &sess, None).await.unwrap();

			let mut sess = Session {
				ns: level.ns.map(String::from),
				db: level.db.map(String::from),
				..Default::default()
			};

			// Basic authentication using the newly defined user.
			let res = basic(&ds, &mut sess, "user", "pass", level.ns, level.db).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				PolicyError::InvalidRole(_) => {}
				e => panic!("Unexpected error, expected PolicyError::InvalidRole found {e}"),
			}
		}
	}

	#[rstest]
	#[case::with_no_roles(None, "secret", vec![Role::Viewer], false)]
	#[case::with_roles(Some(vec!["editor", "owner"]), "secret", vec![Role::Editor, Role::Owner], false)]
	#[case::with_nonexistent_roles(Some(vec!["viewer", "nonexistent"]), "secret", vec![], true)]
	#[case::with_invalid_token_signature(None, "invalid", vec![], true)]
	#[tokio::test]
	async fn test_token(
		#[case] roles: Option<Vec<&'static str>>,
		#[case] key: &'static str,
		#[case] expect_roles: Vec<Role>,
		#[case] expect_error: bool,
		#[values(
			 TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			}
		)]
		level: TestLevel,
	) {
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		// Define the access token for that level
		ds.execute(
			format!(
				r#"
				DEFINE ACCESS token ON {} TYPE JWT
					ALGORITHM HS512 KEY 'secret' DURATION FOR SESSION 30d
				;
			"#,
				level.level
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Prepare the claims object
		let mut claims = claims.clone();
		claims.ns = level.ns.map(|s| s.to_string());
		claims.db = level.db.map(|s| s.to_string());
		claims.roles = roles.clone().map(|roles| roles.into_iter().map(String::from).collect());

		// Create the token
		let key = EncodingKey::from_secret(key.as_ref());
		let enc = encode(&HEADER, &claims, &key).unwrap();

		// Authenticate with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;

		if expect_error {
			assert!(res.is_err(), "Unexpected success");
		} else {
			assert!(res.is_ok(), "Failed to sign in with token");
			assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
			assert_eq!(sess.db, level.db.map(|s| s.to_string()));
			// Sub-less token: the identity is the access name, length-prefixed.
			assert_eq!(sess.au.id(), "5:token");

			// Check roles
			for role in AVAILABLE_ROLES {
				let has_role = sess.au.has_role(role);
				let should_have_role = expect_roles.contains(&role);
				assert_eq!(has_role, should_have_role, "Role {:?} check failed", role);
			}

			// Ensure that the expiration is set correctly
			let exp = sess.exp.unwrap();
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}
	}

	#[rstest]
	#[case::matching_audience(Some(Audience::Single("surrealdb-test".to_string())), false)]
	#[case::mismatched_audience(Some(Audience::Single("invalid".to_string())), true)]
	#[case::missing_audience(None, true)]
	#[case::array_with_matching_audience(Some(Audience::Multiple(vec!["other".to_string(), "surrealdb-test".to_string()])), false)]
	#[case::array_without_matching_audience(Some(Audience::Multiple(vec!["other".to_string(), "invalid".to_string()])), true)]
	#[tokio::test]
	async fn test_token_with_audience(
		#[case] aud: Option<Audience>,
		#[case] expect_error: bool,
		#[values(
			 TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			}
		)]
		level: TestLevel,
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		// Define the access token for that level with a required audience
		ds.execute(
			format!(
				r#"
				DEFINE ACCESS token ON {} TYPE JWT
					ALGORITHM HS512 KEY 'secret' AUDIENCE "surrealdb-test"
					DURATION FOR SESSION 30d
				;
			"#,
				level.level
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Prepare the claims object
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			aud,
			ns: level.ns.map(|s| s.to_string()),
			db: level.db.map(|s| s.to_string()),
			..Claims::default()
		};

		// Create the token
		let key = EncodingKey::from_secret("secret".as_ref());
		let enc = encode(&HEADER, &claims, &key).unwrap();

		// Authenticate with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;

		if expect_error {
			assert!(res.is_err(), "Unexpected success with audience case");
		} else {
			assert!(res.is_ok(), "Failed to sign in with token: {:?}", res);
			// Sub-less token: the identity is the access name, length-prefixed.
			assert_eq!(sess.au.id(), "5:token");
		}
	}

	#[rstest]
	#[tokio::test]
	async fn test_token_without_configured_audience(
		#[values(
			 TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			}
		)]
		level: TestLevel,
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		// Define the access token for that level without an audience
		ds.execute(
			format!(
				r#"
				DEFINE ACCESS token ON {} TYPE JWT
					ALGORITHM HS512 KEY 'secret' DURATION FOR SESSION 30d
				;
			"#,
				level.level
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Any `aud` claim is ignored when the access method defines no audience
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			aud: Some(Audience::Single("anything".to_string())),
			ns: level.ns.map(|s| s.to_string()),
			db: level.db.map(|s| s.to_string()),
			..Claims::default()
		};

		// Create the token
		let key = EncodingKey::from_secret("secret".as_ref());
		let enc = encode(&HEADER, &claims, &key).unwrap();

		// Authenticate with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;
		assert!(res.is_ok(), "Failed to sign in with token: {:?}", res);
		// Sub-less token: the identity is the access name, length-prefixed.
		assert_eq!(sess.au.id(), "5:token");
	}

	#[rstest]
	#[case::matching_audience(Some(Audience::Single("surrealdb-test".to_string())), false)]
	#[case::mismatched_audience(Some(Audience::Single("invalid".to_string())), true)]
	#[case::missing_audience(None, true)]
	#[tokio::test]
	async fn test_token_record_with_audience(
		#[case] aud: Option<Audience>,
		#[case] expect_error: bool,
	) {
		let secret = "secret";
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY '{secret}' AUDIENCE "surrealdb-test"
				DURATION FOR SESSION 30d;

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Prepare the claims object
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			id: Some(syn::record_id("user:test").unwrap().to_sql()),
			aud,
			..Claims::default()
		};

		// Create the token
		let key = EncodingKey::from_secret(secret.as_ref());
		let enc = encode(&HEADER, &claims, &key).unwrap();

		// Authenticate with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;

		if expect_error {
			assert!(res.is_err(), "Unexpected success with audience case");
		} else {
			assert!(res.is_ok(), "Failed to sign in with token: {:?}", res);
			assert_eq!(sess.au.id(), "user:test");
		}
	}

	#[rstest]
	#[case::with_no_roles(vec!["user:test"], None, "secret", false)]
	#[case::with_roles(vec!["user:test"], Some(vec!["editor", "owner"]), "secret", false)]
	#[case::with_invalid_token_signature(vec!["user:test"], None, "invalid", true)]
	#[case::with_generic_id(vec!["user:2k9qnabxuxh8k4d5gfto"], None, "secret", false)]
	#[case::with_numeric_ids(vec!["user:1", "user:2", "user:100", "user:10000000"], None, "secret", false)]
	#[case::with_alphanumeric_ids(vec!["user:username", "user:username1", "user:username10", "user:username100"], None, "secret", false)]
	#[case::with_ids_including_special_characters(vec!["user:`user.name`", "user:`user.name1`", "user:`user.name10`", "user:`user.name100`"], None, "secret", false)]
	#[case::with_uuid_ids(vec!["user:`83149446-95f5-4c0d-9f42-136e7b272456`"], None, "secret", false)]
	#[tokio::test]
	async fn test_token_record(
		#[case] ids: Vec<&'static str>,
		#[case] roles: Option<Vec<&'static str>>,
		#[case] key: &'static str,
		#[case] expect_error: bool,
	) {
		let secret = "secret";
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY '{secret}'
				DURATION FOR SESSION 30d;

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		for id in &ids {
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.id = Some(syn::record_id(id).unwrap().to_sql());
			claims.roles = roles.clone().map(|roles| roles.into_iter().map(String::from).collect());

			// Create the token
			let key = EncodingKey::from_secret(key.as_ref());
			let enc = encode(&HEADER, &claims, &key).unwrap();

			// Authenticate with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			if expect_error {
				assert!(res.is_err(), "Unexpected success");
			} else {
				assert!(res.is_ok(), "Failed to sign in with token");
				assert_eq!(sess.ns, Some("test".to_string()));
				assert_eq!(sess.db, Some("test".to_string()));
				assert_eq!(sess.au.id(), *id);

				// Ensure record users do not have roles
				for role in AVAILABLE_ROLES {
					assert!(
						!sess.au.has_role(role),
						"Auth user expected to not have role {:?} in case",
						role
					);
				}

				// Ensure that the expiration is set correctly
				let exp = sess.exp.unwrap();
				let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
				let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
				assert!(
					exp > min_exp && exp < max_exp,
					"Session expiration is expected to match the defined duration in case"
				);
			}
		}
	}

	#[tokio::test]
	async fn test_token_record_custom_claims() {
		use std::collections::HashMap;

		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY '{secret}'
				DURATION FOR SESSION 30d;

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Token with valid custom claims of different types
		//
		let now = Utc::now().timestamp();
		let later = (Utc::now() + Duration::hours(1)).timestamp();
		{
			let claims_json = format!(
				r#"
				{{
					"iss": "surrealdb-test",
					"iat": {now},
					"nbf": {now},
					"exp": {later},
					"ns": "test",
					"db": "test",
					"ac": "token",
					"id": "user:test",

					"string_claim": "test",
					"bool_claim": true,
					"int_claim": 123456,
					"float_claim": 123.456,
					"array_claim": [
						"test_1",
						"test_2"
					],
					"object_claim": {{
						"test_1": "value_1",
						"test_2": {{
							"test_2_1": "value_2_1",
							"test_2_2": "value_2_2"
						}}
					}}
				}}
				"#
			);
			let claims = serde_json::from_str::<Claims>(&claims_json).unwrap();
			// Create the token
			let enc = match encode(&HEADER, &claims, &key) {
				Ok(enc) => enc,
				Err(err) => panic!("Failed to encode token: {:?}", err),
			};
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
			let tk = match sess.tk {
				Some(crate::types::PublicValue::Object(tk)) => tk,
				_ => panic!("Session token is not an object"),
			};
			let string_claim = tk.get("string_claim").unwrap();
			assert_eq!(*string_claim, crate::types::PublicValue::String("test".to_string()));
			let bool_claim = tk.get("bool_claim").unwrap();
			assert_eq!(*bool_claim, crate::types::PublicValue::Bool(true));
			let int_claim = tk.get("int_claim").unwrap();
			assert_eq!(*int_claim, crate::types::PublicValue::Number(123456.into()));
			let float_claim = tk.get("float_claim").unwrap();
			assert_eq!(*float_claim, crate::types::PublicValue::Number(123.456.into()));
			let array_claim = tk.get("array_claim").unwrap();
			assert_eq!(
				*array_claim,
				crate::types::PublicValue::Array(vec!["test_1", "test_2"].into())
			);
			let object_claim = tk.get("object_claim").unwrap();
			let mut test_object: HashMap<String, crate::types::PublicValue> = HashMap::new();
			test_object.insert(
				"test_1".to_string(),
				crate::types::PublicValue::String("value_1".to_string()),
			);
			let mut test_object_child = HashMap::new();
			test_object_child.insert(
				"test_2_1".to_string(),
				crate::types::PublicValue::String("value_2_1".to_string()),
			);
			test_object_child.insert(
				"test_2_2".to_string(),
				crate::types::PublicValue::String("value_2_2".to_string()),
			);
			test_object.insert(
				"test_2".to_string(),
				crate::types::PublicValue::Object(test_object_child.into()),
			);
			assert_eq!(*object_claim, crate::types::PublicValue::Object(test_object.into()));
		}
	}

	#[cfg(feature = "jwks")]
	#[tokio::test]
	async fn test_token_record_jwks() {
		use base64::Engine;
		use base64::engine::general_purpose::STANDARD_NO_PAD;
		use jsonwebtoken::jwk::{Jwk, JwkSet};
		use rand::distr::{Alphanumeric, SampleString};
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		use crate::dbs::capabilities::{Capabilities, NetTarget, Targets};

		// Use unique path to prevent accidental cache reuse
		fn random_path() -> String {
			Alphanumeric.sample_string(&mut rand::rng(), 8)
		}

		// Key identifier used in both JWT and JWT
		let kid = "test_kid";
		// Secret used to both sign and verify with HMAC
		let secret = "jwt_secret";

		// JWKS object with single JWK object providing the HS512 secret used to verify
		let jwks = JwkSet {
			keys: vec![Jwk {
				common: jsonwebtoken::jwk::CommonParameters {
					public_key_use: None,
					key_operations: None,
					key_algorithm: Some(jsonwebtoken::jwk::KeyAlgorithm::HS512),
					key_id: Some(kid.to_string()),
					x509_url: None,
					x509_chain: None,
					x509_sha1_fingerprint: None,
					x509_sha256_fingerprint: None,
				},
				algorithm: jsonwebtoken::jwk::AlgorithmParameters::OctetKey(
					jsonwebtoken::jwk::OctetKeyParameters {
						key_type: jsonwebtoken::jwk::OctetKeyType::Octet,
						value: STANDARD_NO_PAD.encode(secret),
					},
				),
			}],
		};

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(1)
			.mount(&mock_server)
			.await;
		let server_url = mock_server.uri();

		// We allow requests to the local server serving the JWKS object
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::default().with_network_targets(
				Targets::<NetTarget>::Some([NetTarget::from_str("127.0.0.1").unwrap()].into()),
			))
			.build_with_path("memory")
			.await
			.unwrap();

		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT URL '{server_url}/{jwks_path}';

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Use custom JWT header that includes the key identifier
		let header_with_kid = jsonwebtoken::Header {
			kid: Some(kid.to_string()),
			alg: jsonwebtoken::Algorithm::HS512,
			..jsonwebtoken::Header::default()
		};

		// Sign the JWT with the same secret specified in the JWK
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			aud: Some(Audience::Single("surrealdb-test".to_string())),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			id: Some("user:test".to_string()),
			..Claims::default()
		};

		//
		// Test without roles defined
		// Roles should be ignored in record access
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&header_with_kid, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default session expiration is expected to be None");
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&header_with_kid, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}
	}

	#[test]
	fn test_verify_pass() {
		let salt = SaltString::generate(&mut OsRng);
		let hash = Argon2::default().hash_password("test".as_bytes(), &salt).unwrap().to_string();

		// Verify with the matching password
		verify_pass("test", &hash).unwrap();

		// Verify with a non matching password
		assert!(verify_pass("nonmatching", &hash).is_err());
	}

	#[tokio::test]
	async fn test_verify_creds_invalid() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Reject invalid ROOT credentials
		{
			assert!(verify_root_creds(&ds, "test", "test").await.is_err());
		}

		// Reject invalid NS credentials
		{
			assert!(verify_ns_creds(&ds, &ns, "test", "test").await.is_err());
		}

		// Reject invalid DB credentials
		{
			assert!(verify_db_creds(&ds, &ns, &db, "test", "test").await.is_err());
		}
	}

	#[tokio::test]
	async fn test_verify_creds_valid() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Define users
		{
			let sess = Session::owner();

			let sql = "DEFINE USER root ON ROOT PASSWORD 'root'";
			ds.execute(sql, &sess, None).await.unwrap();

			let sql = "USE NS N; DEFINE USER ns ON NS PASSWORD 'ns'";
			ds.execute(sql, &sess, None).await.unwrap();

			let sql = "USE NS N DB D; DEFINE USER db ON DB PASSWORD 'db'";
			ds.execute(sql, &sess, None).await.unwrap();
		}

		// Accept ROOT user
		{
			let res = verify_root_creds(&ds, "root", "root").await;
			res.unwrap();
		}

		// Accept NS user
		{
			let res = verify_ns_creds(&ds, &ns, "ns", "ns").await;
			res.unwrap();
		}

		// Accept DB user
		{
			let res = verify_db_creds(&ds, &ns, &db, "db", "db").await;
			res.unwrap();
		}
	}

	// Every holder of a token issued by one `DEFINE ACCESS` shared an actor
	// identity, because it was the access method's name. `Auth::id()` is the
	// identity half of the fingerprints the MCP transport's per-request subject
	// re-check and `AuthPrincipalSnapshot` both take, so two different users of
	// one access method were indistinguishable to both.
	#[tokio::test]
	async fn test_token_actor_id_distinguishes_subjects_of_one_access_method() {
		let secret = "jwt_secret_that_is_long_enough_for_hs512_signing_in_tests";
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			&format!(
				r#"DEFINE ACCESS api ON DATABASE TYPE JWT
					ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 1h;"#
			),
			&sess,
			None,
		)
		.await
		.unwrap();

		let key = EncodingKey::from_secret(secret.as_ref());
		// Two tokens for the same access method differing only in subject and
		// roles, exactly as two users of one method would be issued.
		let sign = |subject: &str, roles: Vec<String>| {
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("api".to_string()),
				sub: Some(subject.to_string()),
				roles: Some(roles),
				..Claims::default()
			};
			encode(&HEADER, &claims, &key).unwrap()
		};

		let mut alice_sess = Session::default();
		token(&ds, &mut alice_sess, &sign("alice", vec!["Editor".to_string()]))
			.await
			.expect("alice's token must verify");
		let mut bob_sess = Session::default();
		token(&ds, &mut bob_sess, &sign("bob", vec!["Viewer".to_string()]))
			.await
			.expect("bob's token must verify");

		assert_ne!(
			alice_sess.au.id(),
			bob_sess.au.id(),
			"two subjects of one access method must not share an actor identity"
		);
		// The identity is scoped by the access method, so subjects cannot
		// collide across methods.
		// The identity is the access name length-prefixed (so a `/` in an access
		// name can't collide two subjects across methods), then the subject.
		assert_eq!(alice_sess.au.id(), "3:api/alice");
		assert_eq!(bob_sess.au.id(), "3:api/bob");
		// The level still describes the access path both were granted.
		assert_eq!(alice_sess.au.level(), bob_sess.au.level());

		// The same subject presented twice is stable, so a legitimate caller is
		// not rejected by their own re-presented token.
		let mut again = Session::default();
		token(&ds, &mut again, &sign("alice", vec!["Editor".to_string()]))
			.await
			.expect("alice's token must verify again");
		assert_eq!(again.au.id(), alice_sess.au.id());

		// A token carrying no `sub` claim keeps naming the access method: that is
		// the most that can be said about it.
		let anonymous_subject = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("api".to_string()),
			..Claims::default()
		};
		let mut plain = Session::default();
		token(&ds, &mut plain, &encode(&HEADER, &anonymous_subject, &key).unwrap())
			.await
			.expect("a token with no subject claim must still verify");
		assert_eq!(plain.au.id(), "3:api");
	}

	// An access name is an ident that may contain '/', and `sub` is an arbitrary
	// claim, so a bare "{access}/{sub}" join is ambiguous: access "a" + subject
	// "b/c" and access "a/b" + subject "c" would both render "a/b/c", colliding
	// two subjects across methods — the very thing the per-subject identity
	// exists to prevent. The length prefix keeps the encoding injective.
	#[test]
	fn access_actor_id_encoding_is_injective() {
		let with_sub = |access: &str, sub: &str| {
			access_actor_id(
				access,
				&Claims {
					sub: Some(sub.to_owned()),
					..Claims::default()
				},
			)
		};
		let no_sub = |access: &str| access_actor_id(access, &Claims::default());

		assert_ne!(
			with_sub("a", "b/c"),
			with_sub("a/b", "c"),
			"a '/' in the access name must not collide two subjects across methods"
		);
		assert_ne!(
			no_sub("a/x"),
			with_sub("a", "x"),
			"a sub-less identity must not collide with a with-subject one"
		);
		// The same principal is stable across presentations.
		assert_eq!(with_sub("api", "alice"), with_sub("api", "alice"));
		assert_eq!(no_sub("api"), no_sub("api"));
	}

	#[tokio::test]
	async fn test_expired_token() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			// Token was issued two hours ago and expired one hour ago
			iat: Some((Utc::now() - Duration::hours(2)).timestamp()),
			nbf: Some((Utc::now() - Duration::hours(2)).timestamp()),
			exp: Some((Utc::now() - Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(format!("DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 30d, FOR TOKEN 30d")
				.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Prepare the claims object
		let mut claims = claims.clone();
		claims.roles = None;
		// Create the token
		let enc = encode(&HEADER, &claims, &key).unwrap();
		// Signin with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;

		let e = res.unwrap_err();
		match e.downcast().expect("Unexpected error kind") {
			AuthError::ExpiredToken => {}
			e => panic!("Unexpected error, expected ExpiredToken found {e}"),
		}
	}

	#[tokio::test]
	async fn test_token_authenticate_clause() {
		/// How `token` is expected to fail.
		///
		/// The AUTHENTICATE clause can reject a token two ways, and they are
		/// two different error types: user SurrealQL raising `THROW`, or the
		/// clause declining, which is a plain authentication failure.
		#[derive(Debug)]
		enum ExpectedError {
			Thrown(&'static str),
			InvalidAuth,
		}

		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			iss_claim: Option<&'static str>,
			aud_claim: Option<Audience>,
			error_statement: &'static str,
			expected_error: Option<ExpectedError>,
		}

		let test_cases = vec![
			TestCase {
				title: "with correct 'iss' and 'aud' claims",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("surrealdb-test".to_string())),
				error_statement: "THROW",
				expected_error: None,
			},
			TestCase {
				title: "with correct 'iss' and 'aud' claims, multiple audiences",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test".to_string(),
				])),
				error_statement: "THROW",
				expected_error: None,
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("invalid".to_string())),
				error_statement: "THROW",
				expected_error: Some(ExpectedError::Thrown("Invalid token audience string")),
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim, multiple audiences",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test-different".to_string(),
				])),
				error_statement: "THROW",
				expected_error: Some(ExpectedError::Thrown("Invalid token audience array")),
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim, generic error",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("invalid".to_string())),
				error_statement: "RETURN",
				expected_error: Some(ExpectedError::InvalidAuth),
			},
		];

		let test_levels = vec![
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		];

		let secret = "secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("user".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		for level in &test_levels {
			for case in &test_cases {
				println!("Test case: {} level {}", level.level, case.title);

				ds.execute(
					format!(
						r#"
						REMOVE ACCESS IF EXISTS user ON {0};
						DEFINE ACCESS user ON {0} TYPE JWT
							ALGORITHM HS512 KEY '{1}'
							AUTHENTICATE {{
								IF $token.iss != "surrealdb-test" {{ {2} "Invalid token issuer" }};
								IF type::is_array($token.aud) {{
									IF "surrealdb-test" NOT IN $token.aud {{ {2} "Invalid token audience array" }}
								}} ELSE {{
									IF $token.aud IS NOT "surrealdb-test" {{ {2} "Invalid token audience string" }}
								}};
							}}
							DURATION FOR SESSION 2h
						;
					"#,
						level.level, secret, case.error_statement,
					)
					.as_str(),
					&sess,
					None,
				)
				.await
				.unwrap();

				// Prepare the claims object
				let mut claims = claims.clone();
				claims.ns = level.ns.map(|s| s.to_string());
				claims.db = level.db.map(|s| s.to_string());
				claims.iss = case.iss_claim.map(|s| s.to_string());
				claims.aud = case.aud_claim.clone();

				// Create the token
				let enc = encode(&HEADER, &claims, &key).unwrap();

				// Signin with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				if let Some(expected_err) = &case.expected_error {
					assert!(res.is_err(), "Unexpected success for case: {:?}", case);
					let err = res.unwrap_err();
					let matched = match expected_err {
						ExpectedError::Thrown(expected_msg) => {
							matches!(exec_error(&err), Some(ExecError::Thrown(msg)) if msg == expected_msg)
						}
						ExpectedError::InvalidAuth => {
							matches!(err.downcast_ref::<AuthError>(), Some(AuthError::InvalidAuth))
						}
					};
					assert!(matched, "Unexpected error for case: {:?}, got: {:?}", case, err);
				} else {
					assert!(res.is_ok(), "Failed to sign in with token for case: {:?}", case);
					assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
					assert_eq!(sess.db, level.db.map(|s| s.to_string()));
					assert_eq!(sess.ac, Some("user".to_string()));
					// Sub-less token: the identity is the access name, length-prefixed.
					assert_eq!(sess.au.id(), "4:user");

					// Check auth level
					assert_eq!(sess.au.level().ns(), level.ns);
					assert_eq!(sess.au.level().db(), level.db);
					match level.level {
						"ROOT" => assert!(sess.au.is_root()),
						"NS" => assert!(sess.au.is_ns()),
						"DB" => assert!(sess.au.is_db()),
						_ => panic!("Unsupported level"),
					}

					// Check roles
					assert!(
						sess.au.has_role(Role::Viewer),
						"Auth user expected to have Viewer role"
					);
					assert!(
						!sess.au.has_role(Role::Editor),
						"Auth user expected to not have Editor role"
					);
					assert!(
						!sess.au.has_role(Role::Owner),
						"Auth user expected to not have Owner role"
					);

					// Check expiration
					let exp = sess.exp.unwrap();
					let min_exp =
						(Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
					let max_exp =
						(Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
					assert!(
						exp > min_exp && exp < max_exp,
						"Session expiration is expected to match the defined duration in case: {:?}",
						case
					);
				}
			}
		}
	}

	/// Returns the names of the namespaces currently defined at root level.
	async fn root_namespaces(ds: &Datastore) -> Vec<String> {
		let mut info = ds.execute("INFO FOR ROOT", &Session::owner(), None).await.unwrap();
		let info = info.remove(0).result.unwrap();
		let crate::types::PublicValue::Object(info) = info else {
			panic!("INFO FOR ROOT did not return an object: {info:?}");
		};
		let Some(crate::types::PublicValue::Object(namespaces)) = info.get("namespaces") else {
			panic!("INFO FOR ROOT did not list namespaces: {info:?}");
		};
		namespaces.keys().cloned().collect()
	}

	/// Checks the outcome of presenting a token whose AUTHENTICATE clause
	/// attempts a root-level operation. The namespaces in `present` must still
	/// exist, those in `absent` must not, and verification must have been
	/// rejected with `InvalidAuth`. Namespace state is checked before the
	/// verification result so that a clause which ran with excess privileges
	/// is reported as such rather than as an unexpected success.
	async fn assert_root_operation_denied(
		ds: &Datastore,
		res: Result<()>,
		present: &[&str],
		absent: &[&str],
		clause: &str,
	) {
		let namespaces = root_namespaces(ds).await;
		for ns in present {
			assert!(
				namespaces.iter().any(|n| n == ns),
				"Namespace `{ns}` was removed by a {clause} AUTHENTICATE clause: {namespaces:?}"
			);
		}
		for ns in absent {
			assert!(
				!namespaces.iter().any(|n| n == ns),
				"Namespace `{ns}` was created by a {clause} AUTHENTICATE clause: {namespaces:?}"
			);
		}
		assert!(
			res.is_err(),
			"Token was accepted despite a root-level statement in a {clause} AUTHENTICATE clause"
		);
		match res.unwrap_err().downcast().expect("Unexpected error kind") {
			AuthError::InvalidAuth => {}
			e => panic!("Unexpected error, expected InvalidAuth found {e}"),
		}
	}

	#[tokio::test]
	async fn test_token_authenticate_clause_minimum_privileges() {
		// The AUTHENTICATE clause must be evaluated with a session scoped to
		// the access method's own level, never at root level. A database- or
		// namespace-level access definer must not be able to execute
		// root-level statements (e.g. removing another namespace) when a
		// token is presented.
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let root = Session::owner();

		// Database-level JWT access attempting a root-level operation
		{
			let ds = Datastore::new("memory").await.unwrap();
			ds.execute("DEFINE NAMESPACE second", &root, None).await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE JWT
						ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							REMOVE NAMESPACE second;
						}}
						DURATION FOR SESSION 2h
					;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let claims = Claims {
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};
			let enc = encode(&HEADER, &claims, &key).unwrap();
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;
			assert_root_operation_denied(&ds, res, &["second"], &[], "database-level").await;
		}

		// Namespace-level JWT access attempting a root-level operation
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON NAMESPACE TYPE JWT
						ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							DEFINE NAMESPACE pwned;
						}}
						DURATION FOR SESSION 2h
					;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let claims = Claims {
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};
			let enc = encode(&HEADER, &claims, &key).unwrap();
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;
			// The access method's own namespace anchors the listing, so an
			// empty or malformed INFO FOR ROOT cannot satisfy the check
			assert_root_operation_denied(&ds, res, &["test"], &["pwned"], "namespace-level").await;
		}

		// Record access with an `id` claim attempting a root-level operation
		{
			let ds = Datastore::new("memory").await.unwrap();
			ds.execute("DEFINE NAMESPACE second", &root, None).await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE RECORD
						WITH JWT ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							REMOVE NAMESPACE second;
							RETURN $auth;
						}}
						DURATION FOR SESSION 2h
					;

					CREATE user:1;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let claims = Claims {
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:1".to_string()),
				..Claims::default()
			};
			let enc = encode(&HEADER, &claims, &key).unwrap();
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;
			assert_root_operation_denied(&ds, res, &["second"], &[], "record-access").await;
		}

		// Record access without an `id` claim attempting a root-level operation.
		// This takes the path where the AUTHENTICATE clause itself resolves the
		// record, which is only reachable when the token carries no `id`.
		{
			let ds = Datastore::new("memory").await.unwrap();
			ds.execute("DEFINE NAMESPACE second", &root, None).await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE RECORD
						WITH JWT ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							REMOVE NAMESPACE second;
							RETURN user:1;
						}}
						DURATION FOR SESSION 2h
					;

					CREATE user:1;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let claims = Claims {
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};
			let enc = encode(&HEADER, &claims, &key).unwrap();
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;
			assert_root_operation_denied(&ds, res, &["second"], &[], "record-access").await;
		}

		// Root-level JWT access performing the same root-level operation. This
		// is the positive control: the clause is legitimately evaluated at
		// root level, so the token is accepted and the namespace is removed.
		{
			let ds = Datastore::new("memory").await.unwrap();
			ds.execute("DEFINE NAMESPACE second", &root, None).await.unwrap();
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON ROOT TYPE JWT
						ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							REMOVE NAMESPACE second;
						}}
						DURATION FOR SESSION 2h
					;
				"#
				)
				.as_str(),
				&root,
				None,
			)
			.await
			.unwrap();

			let claims = Claims {
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};
			let enc = encode(&HEADER, &claims, &key).unwrap();
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;
			assert!(res.is_ok(), "Root-level AUTHENTICATE clause was rejected: {res:?}");
			let namespaces = root_namespaces(&ds).await;
			assert!(
				!namespaces.iter().any(|n| n == "second"),
				"Root-level AUTHENTICATE clause did not remove the namespace: {namespaces:?}"
			);
		}
	}

	#[tokio::test]
	async fn test_token_record_and_authenticate_clause() {
		// Test with an "id" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:1".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
 				        WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE (
							-- Simple example increasing the record identifier by one
							SELECT * FROM type::record('user', record::id($auth) + 1)
    					)
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1, user:2;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("user".to_string()));
			assert_eq!(sess.au.id(), "user:2");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:2"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
		}

		// Test without an "id" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM type::record('user', $id)
					)
					WITH JWT ALGORITHM HS512 KEY '{secret}'
					AUTHENTICATE (
					    SELECT id FROM user WHERE email = $token.email
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:1 SET email = "info@surrealdb.com";
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let now = Utc::now().timestamp();
			let later = (Utc::now() + Duration::hours(1)).timestamp();
			let claims_json = format!(
				r#"
				{{
					"iss": "surrealdb-test",
					"iat": {now},
					"nbf": {now},
					"exp": {later},
					"ns": "test",
					"db": "test",
					"ac": "user",
					"email": "info@surrealdb.com"
				}}
				"#
			);
			let claims = serde_json::from_str::<Claims>(&claims_json).unwrap();
			// Create the token
			let enc = match encode(&HEADER, &claims, &key) {
				Ok(enc) => enc,
				Err(err) => panic!("Failed to encode token: {:?}", err),
			};
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("user".to_string()));
			assert_eq!(sess.au.id(), "user:1");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:1"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
		}

		// Test being able to fail authentication
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:1".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
                        WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
    					    -- Not just signin, this clause runs across signin, signup and authenticate, which makes it a nice place to centralize logic
    					    IF !$auth.enabled {{
    							THROW "This user is not enabled";
    						}};

    						-- Always need to return the user id back, otherwise auth generically fails
    						RETURN $auth;
    					}}
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1 SET enabled = false;
				"#).as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				ExecError::Thrown(e) => assert_eq!(e, "This user is not enabled"),
				e => panic!("Unexpected error, expected Thrown found {e:?}"),
			}
		}

		// Test AUTHENTICATE clause not returning a value
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:test".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
    				    WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{}}
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				AuthError::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
	}

	/// A token the decoder cannot even parse is an authentication refusal, not
	/// an internal failure, and carries none of the decoder's own account of
	/// what was wrong with it.
	#[rstest]
	#[case::not_a_jwt("garbage")]
	#[case::wrong_segment_count("aaaa.bbbb")]
	#[case::unparseable_segments("aaaa.bbbb.cccc")]
	#[case::empty("")]
	#[tokio::test]
	async fn test_token_malformed(#[case] malformed: &'static str) {
		let ds = Datastore::new("memory").await.unwrap();
		let mut sess = Session::default();

		let e = token(&ds, &mut sess, malformed).await.unwrap_err();
		assert!(
			matches!(e.downcast_ref::<AuthError>(), Some(AuthError::InvalidAuth)),
			"expected InvalidAuth, got: {e}"
		);

		let public = crate::err::anyhow_to_types_error(e);
		assert!(
			public.is_not_allowed(),
			"expected an auth refusal, got {} with message: {}",
			public.kind_str(),
			public.message()
		);
		assert_eq!(
			public.message(),
			AuthError::InvalidAuth.to_string(),
			"the public message must not describe why the token failed to decode"
		);
	}
}

/// `CONTEXT` on the two token-verification paths that settle on a record.
#[cfg(test)]
mod context_clause_tests {
	use chrono::{Duration, Utc};
	use jsonwebtoken::{EncodingKey, encode};
	use surrealdb_types::Value as PV;

	use super::*;
	use crate::iam::token::HEADER;

	fn signed(secret: &str, claims: &Claims) -> String {
		encode(&HEADER, claims, &EncodingKey::from_secret(secret.as_ref())).unwrap()
	}

	fn base_claims() -> Claims {
		Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			..Claims::default()
		}
	}

	/// A token carrying an `id` claim: CONTEXT runs with `$auth` bound to that
	/// record, with no AUTHENTICATE clause in play.
	#[tokio::test]
	async fn token_with_id_claim_computes_the_context() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY 'secret'
				CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
				DURATION FOR SESSION 30d;
			DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
			CREATE user:test;
			CREATE grant:a SET owner = user:test, org = "acme";
			"#,
			&owner,
			None,
		)
		.await
		.unwrap();

		let mut claims = base_claims();
		claims.id = Some("user:test".to_string());
		let enc = signed("secret", &claims);

		let mut sess = Session::default();
		token(&ds, &mut sess, &enc).await.expect("token verification failed");
		assert_eq!(
			sess.data.as_ref().expect("data").public().clone().into_array().unwrap().into_vec(),
			vec![PV::String("acme".to_owned())]
		);

		// A token that fails signature verification never reaches the clause,
		// and leaves the session untouched.
		let bad = signed("wrong-secret", &claims);
		let mut sess = Session::default();
		assert!(token(&ds, &mut sess, &bad).await.is_err());
		assert!(sess.data.is_none());
	}

	/// A token with no `id` claim, where AUTHENTICATE derives the record:
	/// CONTEXT must see the identity AUTHENTICATE settled on.
	#[tokio::test]
	async fn token_authenticated_by_clause_computes_the_context() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY 'secret'
				AUTHENTICATE ( SELECT * FROM ONLY user:test )
				CONTEXT ( RETURN $auth )
				DURATION FOR SESSION 30d;
			DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
			CREATE user:test;
			"#,
			&owner,
			None,
		)
		.await
		.unwrap();

		let enc = signed("secret", &base_claims());
		let mut sess = Session::default();
		token(&ds, &mut sess, &enc).await.expect("token verification failed");
		assert_eq!(
			sess.data.as_ref().expect("data").public(),
			&PV::RecordId(surrealdb_types::RecordId::new("user", "test"))
		);
	}

	/// Re-authenticating a session replaces the principal, so it must not
	/// leave the previous principal's CONTEXT payload behind.
	///
	/// `data` is an authorisation grant — the same grounds on which `clear`
	/// and `reset` drop it. Only the paths that settle a record identity used
	/// to assign it, so a session that authenticated as `user:1` and then
	/// re-authenticated as a database-level principal kept carrying `user:1`'s
	/// grants into every permission it evaluated. An empty `roles` array is
	/// what makes that observable: it leaves the actor without the viewer
	/// role, so table permissions are still enforced for the new principal.
	#[tokio::test]
	async fn re_authentication_drops_the_previous_principals_payload() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY 'secret'
				CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
				DURATION FOR SESSION 30d;
			DEFINE ACCESS tok ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'other'
				DURATION FOR SESSION 30d;
			DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE org IN $session.data;
			CREATE user:1;
			CREATE grant:a SET owner = user:1, org = "acme";
			CREATE doc:1 SET org = "acme";
			CREATE doc:2 SET org = "initech";
			"#,
			&owner,
			None,
		)
		.await
		.unwrap();

		// Authenticate as the record: the payload is populated.
		let mut claims = base_claims();
		claims.ac = Some("user".to_string());
		claims.id = Some("user:1".to_string());
		let mut sess = Session::default();
		token(&ds, &mut sess, &signed("secret", &claims)).await.expect("record token");
		assert!(sess.data.is_some(), "the record path populates the payload");
		assert!(sess.rd.is_some());

		// Re-authenticate the SAME session as a different principal, through
		// an access method that has no record identity and no CONTEXT clause.
		let mut claims = base_claims();
		claims.ac = Some("tok".to_string());
		claims.roles = Some(vec![]);
		token(&ds, &mut sess, &signed("other", &claims)).await.expect("database token");

		assert_eq!(sess.ac.as_deref(), Some("tok"));
		assert!(sess.data.is_none(), "the previous principal's CONTEXT payload must not survive");
		assert!(sess.rd.is_none(), "the previous principal's record identity must not survive");

		let res = ds.execute("SELECT VALUE id FROM doc ORDER BY id", &sess, None).await.unwrap();
		let rows = res.into_iter().next().unwrap().result.unwrap().into_array().unwrap().into_vec();
		assert_eq!(rows, Vec::<PV>::new(), "a stale payload must not keep granting rows");
	}

	/// An access method without the clause is untouched on this path too.
	#[tokio::test]
	async fn token_without_the_clause_leaves_no_data() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY 'secret'
				DURATION FOR SESSION 30d;
			CREATE user:test;
			"#,
			&owner,
			None,
		)
		.await
		.unwrap();

		let mut claims = base_claims();
		claims.id = Some("user:test".to_string());
		let mut sess = Session::default();
		token(&ds, &mut sess, &signed("secret", &claims)).await.expect("token verification");
		assert!(sess.data.is_none());
	}
}
