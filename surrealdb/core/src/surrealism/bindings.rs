//! The values an extension definition's `BINDINGS` give one invocation.
//!
//! ```surql
//! DEFINE EXTENSION ext::mailer FROM silo::acme::mailer<1.2.0> UNSIGNED
//!     BINDINGS { api_key: secret::get("mailgun"), author: "noreply@acme.com" };
//! ```
//!
//! The guest reads a name through its `binding` import. The expression bound
//! to it is evaluated on the first read in the invocation, then served from
//! [`DefinitionBindings::read`] for the rest of it, so an expression no export
//! reads never runs and one it reads runs once. Two names read at the same
//! time are evaluated side by side; two reads of one name share one
//! evaluation. A failed evaluation is not kept, so a later read tries again.
//!
//! An expression runs in the caller's transaction with permissions off and the
//! caller's system level capped at that of the session that wrote the
//! definition (its `auth_limit`), as a `DEFINE API … SECURITY DEFINER` handler
//! runs. Permissions off is what lets a definer hand the extension a value the
//! caller may not read; the cap keeps a binding from reaching past the
//! definer. A caller is never raised above its own level, so a binding that
//! needs a system level, such as `INFO FOR NS`, fails for a record user. So that a caller cannot
//! steer it, it runs in a context isolated from the caller's: the caller's `LET` bindings, request
//! parameters and row are not in scope, while `$auth`, `$session`, `$token` and `$access` are. It
//! may not write or reach an external system with a side effect
//! ([`NoWriteFrame::ExtensionBinding`](crate::dbs::NoWriteFrame)).
//!
//! A failure tells the guest which name failed and nothing else: neither the
//! value nor the evaluation's own error, which could carry data read with
//! permissions off.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use surrealism_runtime::abi::{Abi, Features};
use surrealism_runtime::config::BindingSpec;
use surrealism_runtime::host::Bindings;
use surrealism_runtime::{HostCallError, HostCallResult};

use crate::catalog::ModuleDefinition;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::surrealism::runner::PreparedExtension;
use crate::val::{Value, convert_value_to_public_value};

/// Refuse a definition whose `BINDINGS` do not answer `package`'s
/// `[bindings]` table: a name the table does not declare, or a declared name
/// whose kind is not `option<…>` that the definition leaves out.
///
/// An archive built for an ABI without bindings cannot read any, so it takes
/// no `BINDINGS` and its `[bindings]` table is not consulted.
///
/// Run when a definition is written and its archive is in hand. A definition
/// an import restores before its archive exists is not checked here; a read of
/// a name it leaves out fails when the extension asks for it.
pub(crate) fn check_definition(
	definition: &ModuleDefinition,
	package: &surrealism_runtime::config::SurrealismConfig,
) -> anyhow::Result<()> {
	let abi = Abi::try_from(package.abi)?;
	if !abi.features().contains(Features::BINDINGS) {
		if !definition.bindings.is_empty() {
			anyhow::bail!(
				"The extension is built for ABI {}, which cannot read bindings: rebuild it with a newer SDK to give it `BINDINGS`",
				abi.version().0
			);
		}
		return Ok(());
	}
	for name in definition.bindings.names() {
		if !package.bindings.contains_key(name) {
			anyhow::bail!(
				"`BINDINGS` gives `{name}`, which the extension's package.toml does not declare under [bindings]"
			);
		}
	}
	for (name, spec) in &package.bindings {
		let kind = crate::syn::kind(spec.kind()).map_err(|e| {
			anyhow::anyhow!(
				"The extension's package.toml declares the binding `{name}` with the kind `{}`, which is not a SurrealQL kind: {e}",
				spec.kind()
			)
		})?;
		if !kind.can_be_none() && definition.bindings.get(name).is_none() {
			anyhow::bail!(
				"The extension requires the binding `{name}` of kind `{}`, which `BINDINGS` does not give",
				spec.kind()
			);
		}
	}
	Ok(())
}

/// A name's value in one invocation, once evaluated.
type BindingCell = tokio::sync::OnceCell<Option<surrealdb_types::Value>>;

pub(crate) struct DefinitionBindings {
	ctx: FrozenContext,
	opt: Options,
	definition: Arc<ModuleDefinition>,
	prepared: Arc<dyn PreparedExtension>,
	/// The extension's name, for errors.
	extension: Arc<str>,
	/// One cell per name read so far in this invocation. The lock is held only
	/// to find or add a cell, never across an evaluation; the cell is what
	/// makes concurrent reads of one name share its evaluation.
	read: Mutex<HashMap<String, Arc<BindingCell>>>,
}

impl DefinitionBindings {
	/// The bindings of `definition` for one invocation made from `ctx` and
	/// `opt`, typed by the `[bindings]` table of `prepared`'s package.
	pub(crate) fn new(
		ctx: &FrozenContext,
		opt: &Options,
		definition: Arc<ModuleDefinition>,
		prepared: Arc<dyn PreparedExtension>,
		extension: Arc<str>,
	) -> Self {
		Self {
			ctx: Arc::clone(ctx),
			opt: opt.clone(),
			definition,
			prepared,
			extension,
			read: Default::default(),
		}
	}

	fn failed(&self, name: &str, what: &str) -> HostCallError {
		HostCallError::Datastore(format!(
			"The binding `{name}` of extension `{}` {what}",
			self.extension
		))
	}

	async fn evaluate(
		&self,
		name: &str,
		spec: &BindingSpec,
	) -> HostCallResult<Option<surrealdb_types::Value>> {
		// The kind was checked when the definition was written, and the archive
		// is pinned by its DIGEST, so this fails only for a definition an import
		// restored unchecked.
		let kind = crate::syn::kind(spec.kind())
			.map_err(|_| self.failed(name, "has a kind its package.toml does not spell"))?;
		let Some(expr) = self.definition.bindings.get(name) else {
			if kind.can_be_none() {
				return Ok(None);
			}
			return Err(self.failed(name, "is required and the definition does not bind it"));
		};
		let limit = crate::iam::AuthLimit::try_from(&self.definition.auth_limit)
			.map_err(|_| self.failed(name, "was written by a session whose rights are unknown"))?;
		let opt = self.opt.limited_by(&limit).new_for_extension_binding();
		let ctx = Context::new_isolated(&self.ctx).freeze();
		let exec = crate::exec::entry::frame(&ctx, &opt, None).await;
		let value = crate::exec::entry::scalar(&exec, expr)
			.await
			.map_err(|_| self.failed(name, "failed to evaluate"))?;
		let value = value
			.coerce_to_kind(&kind)
			.map_err(|_| self.failed(name, &format!("is not a value of kind `{}`", spec.kind())))?;
		if matches!(value, Value::None) {
			return Ok(None);
		}
		convert_value_to_public_value(value)
			.map(Some)
			.map_err(|_| self.failed(name, "holds a value an extension cannot receive"))
	}
}

#[async_trait]
impl Bindings for DefinitionBindings {
	async fn get(&self, name: String) -> HostCallResult<Option<surrealdb_types::Value>> {
		// Only a declared name gets a cell, so a guest cannot grow `read` past
		// the package's `[bindings]` by asking for names it made up.
		let Some(spec) = self.prepared.package().bindings.get(&name) else {
			return Err(HostCallError::InvalidRequest(format!(
				"`{name}` is not declared under [bindings] in the package.toml of extension `{}`",
				self.extension
			)));
		};
		let cell = Arc::clone(
			self.read
				.lock()
				.unwrap_or_else(PoisonError::into_inner)
				.entry(name.clone())
				.or_default(),
		);
		cell.get_or_try_init(|| self.evaluate(&name, spec)).await.cloned()
	}
}
