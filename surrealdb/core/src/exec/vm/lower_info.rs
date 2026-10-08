//! Lowering `INFO` statements into programs of store instructions.
//!
//! An `INFO` statement is one plan node; lowering spells out its steps in the
//! order the operator takes them: the permission check, the name it reports
//! on, then for each kind of definition the scan of its prefix and the
//! rendering of what the scan returned, gathered into the object it answers.
//! The program requires the context level the operator does, which the
//! executor resolves — raising for a namespace or database that does not
//! exist — before either runs.

use super::{Lowered, Lowering};
use crate::exec::context::ContextLevel;
use crate::exec::vm::codec::TypeId;
use crate::exec::vm::codec::key::RouteId;
use crate::exec::vm::program::{Definitions, Instr, Operand, ProcessPart, Reg, Source, StoreInstr};
use crate::expr::Base;
use crate::expr::logical::{LogicalExpr, LogicalInfo};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

impl Lowering<'_, '_> {
	/// Lower an `INFO` statement into its program.
	pub(super) async fn info(&mut self, info: &LogicalInfo) -> Lowered<()> {
		match info {
			LogicalInfo::Root {
				structure,
				version,
			} => {
				crate::exec::vm::host::out_of_line(|| self.info_root(*structure, version.as_ref()))
					.await?;
			}
			LogicalInfo::Ns {
				structure,
				version,
			} => {
				crate::exec::vm::host::out_of_line(|| self.info_ns(*structure, version.as_ref()))
					.await?;
			}
			LogicalInfo::Db {
				structure,
				version,
			} => {
				crate::exec::vm::host::out_of_line(|| self.info_db(*structure, version.as_ref()))
					.await?;
			}
			LogicalInfo::Table {
				table,
				structure,
				version,
			} => {
				crate::exec::vm::host::out_of_line(|| {
					self.info_table(table, *structure, version.as_ref())
				})
				.await?;
			}
			LogicalInfo::User {
				user,
				base,
				structure,
			} => {
				crate::exec::vm::host::out_of_line(|| self.info_user(user, *base, *structure))
					.await?
			}
			LogicalInfo::Index {
				index,
				table,
				structure,
			} => {
				crate::exec::vm::host::out_of_line(|| self.info_index(index, table, *structure))
					.await?
			}
		}
		self.push_write_pipeline(Source::Once)?;
		self.scalar = true;
		Ok(())
	}

	/// A register holding the name `expr` gives: a bare name as written, and
	/// anything else evaluated and read as a string (`physical_expr_as_name`).
	async fn info_name(&mut self, expr: &LogicalExpr) -> Lowered<Reg> {
		match expr {
			LogicalExpr::Field(name) => {
				let name = self.info_constant(Value::String(name.clone()));
				self.info_load(name)
			}
			LogicalExpr::Table(name) => {
				let name = self.info_constant(Value::String(name.as_str().into()));
				self.info_load(name)
			}
			expr => {
				let value =
					crate::exec::vm::host::out_of_line(|| self.once_value(expr, false)).await?;
				self.emit(Instr::Store(StoreInstr::CoerceString {
					src: value,
					dst: value,
				}));
				Ok(value)
			}
		}
	}

	fn info_constant(&mut self, value: Value) -> Operand {
		self.consts.push(value);
		Operand::Const((self.consts.len() - 1) as u32)
	}

	fn info_load(&mut self, value: Operand) -> Lowered<Reg> {
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		Ok(dst)
	}

	/// The permission check every `INFO` form but `USER` and `INDEX` makes.
	fn info_allowed(&mut self, base: Base) {
		self.emit(Instr::Store(StoreInstr::Allow {
			action: Action::View,
			resource: ResourceKind::Any,
			base,
		}));
	}

	/// A register holding the version stamp `version` names, evaluated and
	/// read as a datetime; `None` for a statement with no `VERSION`.
	async fn info_version(&mut self, version: Option<&LogicalExpr>) -> Lowered<Option<Reg>> {
		let Some(version) = version else {
			return Ok(None);
		};
		let value = crate::exec::vm::host::out_of_line(|| self.once_value(version, false)).await?;
		self.emit(Instr::Store(StoreInstr::VersionStamp {
			src: value,
			dst: value,
		}));
		Ok(Some(value))
	}

	/// Set field `name` of `out` to the rendering of the definitions of
	/// `kind` stored beneath the `route` bound over `fields`, decoded as `ty`,
	/// as the store stood at the version stamp in `version` when there is one.
	#[allow(clippy::too_many_arguments)]
	fn info_part(
		&mut self,
		out: Reg,
		name: &str,
		(route, ty, kind): (&'static str, &'static str, Definitions),
		fields: &[(&str, Operand)],
		structured: bool,
		scope: Option<(Reg, Reg)>,
		version: Option<Reg>,
	) -> Lowered<()> {
		let key = self.object(fields)?;
		let list = self.register()?;
		match version {
			Some(version) => self.emit(Instr::Store(StoreInstr::ScanAt {
				route: RouteId::named(route),
				key,
				ty: TypeId::named(ty),
				version,
				dst: list,
			})),
			None => self.emit(Instr::Store(StoreInstr::Scan {
				route: RouteId::named(route),
				key,
				ty: TypeId::named(ty),
				dst: list,
				unread: None,
			})),
		};
		self.emit(Instr::Store(StoreInstr::Render {
			definitions: kind,
			structured,
			list,
			dst: list,
			scope,
			version,
		}));
		self.info_set(out, name, Operand::Reg(list));
		self.release(list);
		self.release(key);
		Ok(())
	}

	fn info_set(&mut self, object: Reg, name: &str, value: Operand) {
		self.paths.push(crate::exec::field_path::FieldPath::field(name));
		let path = (self.paths.len() - 1) as u32;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
	}

	/// Answer the object in `out`.
	fn info_answer(&mut self, out: Reg) {
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
	}

	fn info_object(&mut self) -> Lowered<Reg> {
		let out = self.register()?;
		self.emit(Instr::NewObject {
			dst: out,
		});
		Ok(out)
	}

	/// `INFO FOR ROOT`.
	async fn info_root(&mut self, structured: bool, version: Option<&LogicalExpr>) -> Lowered<()> {
		self.info_allowed(Base::Root);
		let version = crate::exec::vm::host::out_of_line(|| self.info_version(version)).await?;
		let out = self.info_object()?;
		let root = [(
			"accesses",
			("RootAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access),
		)];
		for (name, part) in root {
			self.info_part(out, name, part, &[], structured, None, version)?;
		}
		// The root's defaults are its `default` config's, whichever form.
		let ty = self.info_constant(Value::String("default".into()));
		let key = self.object(&[("ty", ty)])?;
		let defaults = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named("RootConfigKey"),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: defaults,
		}));
		self.emit(Instr::Store(StoreInstr::RenderOne {
			definitions: Definitions::Defaults,
			structured: true,
			src: defaults,
			dst: defaults,
		}));
		self.info_set(out, "defaults", Operand::Reg(defaults));
		// The cluster's nodes are not versioned.
		for (name, part, version) in [
			(
				"namespaces",
				("NamespacePrefix", "NamespaceDefinition", Definitions::Namespace),
				version,
			),
			("nodes", ("NodePrefix", "Node", Definitions::Node), None),
		] {
			self.info_part(out, name, part, &[], structured, None, version)?;
		}
		let system = self.register()?;
		self.emit(Instr::Store(StoreInstr::ProcessInfo {
			part: ProcessPart::System,
			dst: system,
		}));
		self.info_set(out, "system", Operand::Reg(system));
		self.info_part(
			out,
			"users",
			("RootUserPrefix", "UserDefinition", Definitions::User),
			&[],
			structured,
			None,
			version,
		)?;
		let config = self.register()?;
		self.emit(Instr::Store(StoreInstr::ProcessInfo {
			part: ProcessPart::Config,
			dst: config,
		}));
		self.info_set(out, "config", Operand::Reg(config));
		self.info_answer(out);
		Ok(())
	}

	/// The session's namespace id, which the executor resolved.
	fn info_ns_id(&mut self) -> Lowered<Reg> {
		let ns_name = self.register()?;
		self.emit(Instr::Store(StoreInstr::SessionNs {
			dst: ns_name,
		}));
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		}));
		let ns = self.register()?;
		self.fields.push("namespace_id".to_owned());
		self.emit(Instr::Store(StoreInstr::GetField {
			src: namespace,
			name: (self.fields.len() - 1) as u32,
			dst: ns,
		}));
		for reg in [ns_name, key, namespace] {
			self.release(reg);
		}
		Ok(ns)
	}

	/// `INFO FOR NS`.
	async fn info_ns(&mut self, structured: bool, version: Option<&LogicalExpr>) -> Lowered<()> {
		self.delegate_context = self.delegate_context.max(ContextLevel::Namespace);
		self.info_allowed(Base::Ns);
		let ns = self.info_ns_id()?;
		let version = crate::exec::vm::host::out_of_line(|| self.info_version(version)).await?;
		let out = self.info_object()?;
		let scope = [("ns", Operand::Reg(ns))];
		for (name, part) in [
			("accesses", ("NsAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access)),
			("databases", ("DatabasePrefix", "DatabaseDefinition", Definitions::Database)),
			("users", ("NsUserPrefix", "UserDefinition", Definitions::User)),
		] {
			self.info_part(out, name, part, &scope, structured, None, version)?;
		}
		self.info_answer(out);
		Ok(())
	}

	/// `INFO FOR DB`.
	async fn info_db(&mut self, structured: bool, version: Option<&LogicalExpr>) -> Lowered<()> {
		self.delegate_context = self.delegate_context.max(ContextLevel::Database);
		self.info_allowed(Base::Db);
		let (ns, db) = self.info_ids()?;
		let version = crate::exec::vm::host::out_of_line(|| self.info_version(version)).await?;
		let out = self.info_object()?;
		let scope = [("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))];
		for (name, part) in [
			("accesses", ("DbAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access)),
			("apis", ("ApiPrefix", "StoredApiDefinition", Definitions::Api)),
			("analyzers", ("AnalyzerPrefix", "AnalyzerDefinition", Definitions::Analyzer)),
			("buckets", ("BucketPrefix", "StoredBucketDefinition", Definitions::Bucket)),
			("functions", ("FunctionPrefix", "StoredFunctionDefinition", Definitions::Function)),
			("modules", ("ModulePrefix", "StoredModuleDefinition", Definitions::Module)),
			("models", ("MlModelPrefix", "StoredMlModelDefinition", Definitions::Model)),
			("params", ("ParamPrefix", "StoredParamDefinition", Definitions::Param)),
			("tables", ("TablePrefix", "StoredTableDefinition", Definitions::Table)),
			("users", ("DbUserPrefix", "UserDefinition", Definitions::User)),
			("configs", ("DbConfigPrefix", "StoredConfigDefinition", Definitions::Config)),
			("sequences", ("SequencePrefix", "SequenceDefinition", Definitions::Sequence)),
		] {
			self.info_part(out, name, part, &scope, structured, Some((ns, db)), version)?;
		}
		self.info_answer(out);
		Ok(())
	}

	/// The session's namespace and database ids, which the executor
	/// resolved.
	fn info_ids(&mut self) -> Lowered<(Reg, Reg)> {
		let ns = self.info_ns_id()?;
		let db_name = self.register()?;
		self.emit(Instr::Store(StoreInstr::SessionDb {
			dst: db_name,
		}));
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		}));
		let db = self.register()?;
		self.fields.push("database_id".to_owned());
		self.emit(Instr::Store(StoreInstr::GetField {
			src: database,
			name: (self.fields.len() - 1) as u32,
			dst: db,
		}));
		for reg in [db_name, key, database] {
			self.release(reg);
		}
		Ok((ns, db))
	}

	/// The id of the session's namespace, read by name, for a statement the
	/// executor resolved no namespace for. One that does not exist raises
	/// what `user` names when it is set — the user's not-found error, naming
	/// the session's namespace and database — and otherwise the namespace's
	/// not-found error.
	fn info_found_ns_id(&mut self, user: Option<Reg>) -> Lowered<Reg> {
		use crate::exec::vm::program::StatementError;
		let ns_name = self.register()?;
		self.emit(Instr::Store(StoreInstr::SessionNs {
			dst: ns_name,
		}));
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		}));
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: true,
			target: 0,
		});
		match user {
			Some(user) => self.info_missing_user(Base::Db, user, StatementError::UserDbNotFound)?,
			None => {
				self.emit(Instr::Store(StoreInstr::Raise {
					error: StatementError::NsNotFound,
					arg: ns_name,
				}));
			}
		}
		let at = self.here();
		self.patch(present, at);
		let ns = self.register()?;
		self.fields.push("namespace_id".to_owned());
		self.emit(Instr::Store(StoreInstr::GetField {
			src: namespace,
			name: (self.fields.len() - 1) as u32,
			dst: ns,
		}));
		for reg in [ns_name, key, namespace] {
			self.release(reg);
		}
		Ok(ns)
	}

	/// The ids of the session's namespace and database, read by name, for a
	/// statement the executor resolved neither for. Either not existing
	/// raises the not-found error of user `user`, naming the session's
	/// namespace and database.
	fn info_found_ids(&mut self, user: Reg) -> Lowered<(Reg, Reg)> {
		use crate::exec::vm::program::StatementError;
		let ns = self.info_found_ns_id(Some(user))?;
		let db_name = self.register()?;
		self.emit(Instr::Store(StoreInstr::SessionDb {
			dst: db_name,
		}));
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		}));
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: 0,
		});
		self.info_missing_user(Base::Db, user, StatementError::UserDbNotFound)?;
		let at = self.here();
		self.patch(present, at);
		let db = self.register()?;
		self.fields.push("database_id".to_owned());
		self.emit(Instr::Store(StoreInstr::GetField {
			src: database,
			name: (self.fields.len() - 1) as u32,
			dst: db,
		}));
		for reg in [db_name, key, database] {
			self.release(reg);
		}
		Ok((ns, db))
	}

	/// `INFO FOR TABLE`.
	async fn info_table(
		&mut self,
		table: &LogicalExpr,
		structured: bool,
		version: Option<&LogicalExpr>,
	) -> Lowered<()> {
		self.delegate_context = self.delegate_context.max(ContextLevel::Database);
		self.info_allowed(Base::Db);
		let (ns, db) = self.info_ids()?;
		let tb = crate::exec::vm::host::out_of_line(|| self.info_name(table)).await?;
		let version = crate::exec::vm::host::out_of_line(|| self.info_version(version)).await?;
		let out = self.info_object()?;
		let scope = [("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", Operand::Reg(tb))];
		for (name, part) in [
			("events", ("EventPrefix", "StoredEventDefinition", Definitions::Event)),
			("fields", ("FieldPrefix", "StoredFieldDefinition", Definitions::Field)),
			("indexes", ("IndexDefPrefix", "StoredIndexDefinition", Definitions::Index)),
			("lives", ("SubscriptionPrefix", "StoredSubscriptionDefinition", Definitions::Live)),
			("tables", ("ForeignTablePrefix", "StoredTableDefinition", Definitions::Table)),
		] {
			self.info_part(out, name, part, &scope, structured, None, version)?;
		}
		self.info_answer(out);
		Ok(())
	}

	/// `INFO FOR USER`: at the base written,
	/// or at the base the session selects when it runs, each base its own
	/// branch.
	async fn info_user(
		&mut self,
		user: &LogicalExpr,
		base: Option<Base>,
		structured: bool,
	) -> Lowered<()> {
		if let Some(base) = base {
			self.delegate_context = self.delegate_context.max(match base {
				Base::Root => ContextLevel::Root,
				Base::Ns => ContextLevel::Namespace,
				Base::Db => ContextLevel::Database,
			});
			return crate::exec::vm::host::out_of_line(|| {
				self.info_user_at(user, base, structured, true)
			})
			.await;
		}
		let to_db = self.emit(Instr::Store(StoreInstr::JumpIfSessionBase {
			base: Base::Db,
			target: 0,
		}));
		let to_ns = self.emit(Instr::Store(StoreInstr::JumpIfSessionBase {
			base: Base::Ns,
			target: 0,
		}));
		crate::exec::vm::host::out_of_line(|| {
			self.info_user_at(user, Base::Root, structured, false)
		})
		.await?;
		let from_root = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(to_ns, at);
		crate::exec::vm::host::out_of_line(|| self.info_user_at(user, Base::Ns, structured, false))
			.await?;
		let from_ns = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(to_db, at);
		crate::exec::vm::host::out_of_line(|| self.info_user_at(user, Base::Db, structured, false))
			.await?;
		let end = self.here();
		self.patch(from_root, end);
		self.patch(from_ns, end);
		Ok(())
	}

	/// `INFO FOR USER` at `base`. Where the base is `written`, the executor
	/// resolved the session's namespace and database down to it; at the
	/// session's own base the statement resolves them itself, raising for a
	/// namespace that does not exist, and for a database that does not as for
	/// a user it does not find.
	async fn info_user_at(
		&mut self,
		user: &LogicalExpr,
		base: Base,
		structured: bool,
		written: bool,
	) -> Lowered<()> {
		use crate::exec::vm::program::StatementError;
		self.emit(Instr::Store(StoreInstr::Allow {
			action: Action::View,
			resource: ResourceKind::Actor,
			base,
		}));
		let name = crate::exec::vm::host::out_of_line(|| self.info_name(user)).await?;
		let (route, fields, missing) = match base {
			Base::Root => (
				"RootUserKey",
				vec![("user", Operand::Reg(name))],
				StatementError::UserRootNotFound,
			),
			Base::Ns => {
				let ns = if written {
					self.info_ns_id()?
				} else {
					self.info_found_ns_id(None)?
				};
				(
					"NsUserKey",
					vec![("ns", Operand::Reg(ns)), ("user", Operand::Reg(name))],
					StatementError::UserNsNotFound,
				)
			}
			Base::Db => {
				let (ns, db) = if written {
					self.info_ids()?
				} else {
					self.info_found_ids(name)?
				};
				(
					"DbUserKey",
					vec![
						("ns", Operand::Reg(ns)),
						("db", Operand::Reg(db)),
						("user", Operand::Reg(name)),
					],
					StatementError::UserDbNotFound,
				)
			}
		};
		let key = self.object(&fields)?;
		let definition = self.register()?;
		self.emit(Instr::Store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		}));
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: 0,
		});
		self.info_missing_user(base, name, missing)?;
		let at = self.here();
		self.patch(present, at);
		self.emit(Instr::Store(StoreInstr::RenderOne {
			definitions: Definitions::User,
			structured,
			src: definition,
			dst: definition,
		}));
		self.info_answer(definition);
		Ok(())
	}

	/// `INFO FOR INDEX index ON TABLE table`.
	async fn info_index(
		&mut self,
		index: &LogicalExpr,
		table: &LogicalExpr,
		_structure: bool,
	) -> Lowered<()> {
		self.delegate_context = self.delegate_context.max(ContextLevel::Database);
		self.emit(Instr::Store(StoreInstr::Allow {
			action: Action::View,
			resource: ResourceKind::Actor,
			base: Base::Db,
		}));
		let index = crate::exec::vm::host::out_of_line(|| self.info_name(index)).await?;
		let table = crate::exec::vm::host::out_of_line(|| self.info_name(table)).await?;
		let out = self.register()?;
		self.emit(Instr::Store(StoreInstr::InfoIndex {
			table,
			index,
			dst: out,
		}));
		self.info_answer(out);
		self.release(index);
		self.release(table);
		self.release(out);
		Ok(())
	}

	/// Raise the missing user's error, naming the user and the session's
	/// namespace and database names down to `base`.
	fn info_missing_user(
		&mut self,
		base: Base,
		name: Reg,
		missing: crate::exec::vm::program::StatementError,
	) -> Lowered<()> {
		let mut detail = vec![("name", Operand::Reg(name))];
		if !matches!(base, Base::Root) {
			let ns = self.register()?;
			self.emit(Instr::Store(StoreInstr::SessionNs {
				dst: ns,
			}));
			detail.push(("ns", Operand::Reg(ns)));
		}
		if matches!(base, Base::Db) {
			let db = self.register()?;
			self.emit(Instr::Store(StoreInstr::SessionDb {
				dst: db,
			}));
			detail.push(("db", Operand::Reg(db)));
		}
		let detail = self.object(&detail)?;
		self.emit(Instr::Store(StoreInstr::Raise {
			error: missing,
			arg: detail,
		}));
		Ok(())
	}
}
