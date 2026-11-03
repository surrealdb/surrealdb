//! `DEFINE CONFIG`, `ALTER CONFIG`, `DEFINE API` and `ALTER API`.
//!
//! Each statement stores a definition whose middleware arguments are values. An argument that is
//! written is held by the definition's template. An argument that is evaluated is held there as
//! `NONE`, and the whole middleware list of the configuration is then replaced with the evaluated
//! one. An argument is written only when its operand is a literal equal to the value the template
//! holds at its position. Any other argument is evaluated in the statement's frame and refused
//! when it nests deeper than a value may be stored.

use super::{
	AlterAttrs, Ddl, DefineAttrs, IrError, NameClause, Operand, OperandSlot, Reg, RouteId,
	StatementError, StoreInstr, TypeId, Value,
};
use crate::exec::ir::iface::statements::{
	AlterApiClauseShape, ApiConfigShape, ConfigShape, DefineApi,
};
use crate::exec::vm::program::Instr;
use crate::expr::ApiMethod;
use crate::expr::statements::alter::AlterKind;
use crate::iam::ConfigKind;

/// The field `key` of the object `value`.
fn child<'v>(value: &'v Value, key: &str) -> Option<&'v Value> {
	if let Value::Object(object) = value {
		object.get(key)
	} else {
		None
	}
}

/// The element `index` of the array `value`.
fn element(value: &Value, index: usize) -> Option<&Value> {
	if let Value::Array(array) = value {
		array.get(index)
	} else {
		None
	}
}

/// The kind of `config`, and the name its definition is stored under.
fn config_kind(config: &ConfigShape) -> (ConfigKind, &'static str) {
	match config {
		ConfigShape::GraphQL(_) => (ConfigKind::GraphQL, "graphql"),
		ConfigShape::Api(_) => (ConfigKind::Api, "api"),
		ConfigShape::Default(_) => (ConfigKind::Default, "default"),
	}
}

impl Ddl<'_, '_> {
	/// The name an optional clause gives, or `NONE`: a written name and `NONE` are held by the
	/// definition's template and read at no instruction. Anything else is evaluated here and
	/// read as an optional name, in the register answered.
	fn optional_name(
		&mut self,
		slot: OperandSlot,
		into: NameClause,
	) -> Result<Option<Reg>, IrError> {
		if let Some(written @ (Value::None | Value::String(_))) = self.literal(slot)? {
			let written = written.clone();
			let operand = self.constant(written)?;
			self.read_as(slot, operand)?;
			return Ok(None);
		}
		let value = self.evaluated(slot)?;
		self.store(StoreInstr::CoerceName {
			src: value,
			dst: value,
			into,
			optional: true,
		});
		Ok(Some(value))
	}

	/// A register holding the middleware of `config` with its arguments evaluated, or none when
	/// every argument is written. `stored` is the stored form of the configuration, whose
	/// `middleware` holds the arguments the statement writes.
	fn evaluated_middleware(
		&mut self,
		config: &ApiConfigShape,
		stored: Option<&Value>,
	) -> Result<Option<Reg>, IrError> {
		let entries = stored.and_then(|stored| child(stored, "middleware"));
		let mut written: Vec<Vec<Option<Value>>> = Vec::with_capacity(config.middleware.len());
		let mut all_written = true;
		for (at, entry) in config.middleware.iter().enumerate() {
			let args = entries.and_then(|entries| element(entries, at));
			let args = args.and_then(|entry| child(entry, "args"));
			let mut held = Vec::with_capacity(entry.args.len());
			for (position, slot) in entry.args.iter().enumerate() {
				let stored_arg = args.and_then(|args| element(args, position));
				let arg = match (self.literal(*slot)?, stored_arg) {
					(Some(literal), Some(stored)) if literal == stored => Some(literal.clone()),
					(Some(_) | None, Some(_) | None) => None,
				};
				all_written &= arg.is_some();
				held.push(arg);
			}
			written.push(held);
		}
		if all_written {
			for (entry, held) in config.middleware.iter().zip(written) {
				for (slot, value) in entry.args.iter().zip(held) {
					let value =
						value.ok_or(IrError::Stale("ir_lower::ddl: a middleware argument"))?;
					let operand = self.constant(value)?;
					self.read_as(*slot, operand)?;
				}
			}
			return Ok(None);
		}
		let list = self.reg()?;
		self.emit(Instr::NewArray {
			dst: list,
		});
		for entry in &config.middleware {
			let args = self.reg()?;
			self.emit(Instr::NewArray {
				dst: args,
			});
			for slot in &entry.args {
				let value = self.evaluated(*slot)?;
				self.store(StoreInstr::CheckDepth {
					value,
				});
				self.emit(Instr::ArrayPush {
					array: args,
					value: Operand::Reg(value),
				});
			}
			let name = self.constant(Value::String(entry.name.as_str().into()))?;
			let entry = self.object(&[("name", name), ("args", Operand::Reg(args))])?;
			self.emit(Instr::ArrayPush {
				array: list,
				value: Operand::Reg(entry),
			});
		}
		Ok(Some(list))
	}

	/// Sets the namespace and database of a `DEFAULT` config, or the middleware of an API
	/// config, in `definition`. Each value is evaluated where the body computes the definition.
	fn fill_config(
		&mut self,
		definition: Reg,
		config: &ConfigShape,
		stored: &Value,
	) -> Result<(), IrError> {
		match config {
			ConfigShape::GraphQL(_) => {}
			ConfigShape::Default(config) => {
				let mut defaults = Vec::new();
				for (field, slot, into) in [
					("namespace", config.namespace, NameClause::ConfigNamespace),
					("database", config.database, NameClause::ConfigDatabase),
				] {
					if let Some(value) = self.optional_name(slot, into)? {
						defaults.push((field, value));
					}
				}
				for (field, value) in defaults {
					self.set_path(definition, &["Default", field], Operand::Reg(value))?;
				}
			}
			ConfigShape::Api(config) => {
				let middleware = self.evaluated_middleware(config, child(stored, "Api"))?;
				if let Some(list) = middleware {
					self.set_path(definition, &["Api", "middleware"], Operand::Reg(list))?;
				}
			}
		}
		Ok(())
	}

	/// The key a config of `kind` is stored under, and the route of it: the root's default config,
	/// or one of the session's database.
	fn config_key(
		&mut self,
		kind: &ConfigKind,
		cg: Operand,
		creating: bool,
	) -> Result<(&'static str, Reg), IrError> {
		match crate::ddl::define::config::config_kind_base(kind) {
			crate::catalog::base::Base::Db => {
				let (ns, db) = if creating {
					self.ids_creating()?
				} else {
					self.session_ids()?
				};
				let key =
					self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", cg)])?;
				Ok(("DbConfigKey", key))
			}
			crate::catalog::base::Base::Root => Ok(("RootConfigKey", self.object(&[("ty", cg)])?)),
			crate::catalog::base::Base::Ns => Err(IrError::Invariant {
				what: "ir_lower::ddl: a config administered at a namespace",
			}),
		}
	}

	/// `DEFINE CONFIG`: the definition is written through the store's replace.
	pub(super) fn define_config(
		&mut self,
		attrs: &DefineAttrs,
		config: &ConfigShape,
	) -> Result<(), IrError> {
		let (kind, cg) = config_kind(config);
		self.guard()?;
		let cg = self.constant(Value::String(cg.into()))?;
		let (route, key) = self.config_key(&kind, cg, true)?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: existing,
		});
		let name = self.load(cg)?;
		let missing = self.exists(&attrs.mode, existing, StatementError::CgAlreadyExists, name)?;
		self.bind(missing)?;
		let written: crate::catalog::ConfigDefinition =
			super::definition_of(&attrs.template.value, "StoredConfigDefinition")?;
		let names = crate::exec::vm::schema_host::config_named_types(&written);
		// A named type is a type of a database, so only a database's config can name one.
		if !names.is_empty() && route == "DbConfigKey" {
			let (ns, db) = (self.reg()?, self.reg()?);
			self.get_field(key, "ns", ns)?;
			self.get_field(key, "db", db)?;
			self.check_types_defined((ns, db), names, || "The API config".to_owned());
		}
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.fill_config(definition, config, &attrs.template.value)?;
		self.store(StoreInstr::Replace {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER CONFIG`: the session's database's config of the kind is written whether or not it
	/// existed, unless the statement asks `IF EXISTS`. The statement's comment is not stored.
	pub(super) fn alter_config(
		&mut self,
		attrs: &AlterAttrs,
		config: &ConfigShape,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		let (_, cg) = config_kind(config);
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let cg = self.constant(Value::String(cg.into()))?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", cg)])?;
		if attrs.if_exists {
			let existing = self.reg()?;
			self.store(StoreInstr::Get {
				route: RouteId::named("DbConfigKey"),
				key,
				ty: TypeId::named("StoredConfigDefinition"),
				dst: existing,
			});
			let present = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(existing),
				truthy: true,
				target: present,
			});
			self.answer_none()?;
			self.bind(present)?;
		}
		self.check_altered_types(
			template,
			(ns, db),
			crate::exec::vm::schema_host::config_named_types,
			|| "The API config".to_owned(),
		)?;
		let constant = self.constant(template.value.clone())?;
		let definition = self.load(constant)?;
		self.fill_config(definition, config, &template.value)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("DbConfigKey"),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE API`: the path is parsed after the existence check, and a method in more than one
	/// `FOR` clause is refused once it is parsed. The path's middleware, then each action's, is
	/// evaluated before the comment.
	pub(super) fn define_api(
		&mut self,
		attrs: &DefineAttrs,
		api: &DefineApi,
	) -> Result<(), IrError> {
		let parsed_path: Option<crate::api::path::Path> = match self.literal(api.path)? {
			Some(Value::String(text)) => text.as_str().parse().ok(),
			Some(_) | None => None,
		};
		let mut seen: Vec<ApiMethod> = Vec::new();
		let mut duplicate = None;
		for method in api.actions.iter().flat_map(|action| action.methods.iter()) {
			if seen.contains(method) {
				duplicate = Some(method.to_string());
				break;
			}
			seen.push(*method);
		}
		self.guard()?;
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let path = self.name(api.path, NameClause::ApiPath)?;
		let given = path.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("ap", given)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: existing,
		});
		let name = self.load(given)?;
		let missing = self.exists(&attrs.mode, existing, StatementError::ApAlreadyExists, name)?;
		self.bind(missing)?;
		let written: crate::catalog::ApiDefinition =
			super::definition_of(&attrs.template.value, "StoredApiDefinition")?;
		let names = crate::exec::vm::schema_host::api_named_types(&written);
		if !names.is_empty() {
			let subject = self.shown_subject("The API", api.path)?;
			self.check_types_defined((ns, db), names, || subject);
		}
		let parsed = if parsed_path.is_none() {
			let given = self.load(given)?;
			let (text, rendered) = (self.reg()?, self.reg()?);
			self.store(StoreInstr::ParseApiPath {
				src: given,
				text,
				rendered,
			});
			Some((text, rendered))
		} else {
			None
		};
		if let Some(method) = duplicate {
			let method = self.constant(Value::String(method.into()))?;
			let detail = self.object(&[("value", given), ("method", method)])?;
			self.store(StoreInstr::Raise {
				error: StatementError::ApMethodDuplicate,
				arg: detail,
			});
		}
		let stored = &attrs.template.value;
		let middleware = self.evaluated_middleware(&api.config, child(stored, "config"))?;
		let mut action_middleware = Vec::with_capacity(api.actions.len());
		for (at, action) in api.actions.iter().enumerate() {
			let action_config = child(stored, "actions")
				.and_then(|actions| element(actions, at))
				.and_then(|action| child(action, "config"));
			action_middleware.push(self.evaluated_middleware(&action.config, action_config)?);
		}
		let comment = self.comment(api.comment)?;
		let template = self.constant(stored.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		if let Some(list) = middleware {
			self.set_path(definition, &["config", "middleware"], Operand::Reg(list))?;
		}
		if action_middleware.iter().any(Option::is_some) {
			let Some(Value::Array(actions)) = child(stored, "actions") else {
				return Err(IrError::Stale(
					"ir_lower::ddl: stored API actions that are not a list",
				));
			};
			let list = self.reg()?;
			self.emit(Instr::NewArray {
				dst: list,
			});
			for (action, evaluated) in actions.iter().zip(&action_middleware) {
				let action = self.constant(action.clone())?;
				let action = self.load(action)?;
				if let Some(evaluated) = evaluated {
					self.set_path(action, &["config", "middleware"], Operand::Reg(*evaluated))?;
				}
				self.emit(Instr::ArrayPush {
					array: list,
					value: Operand::Reg(action),
				});
			}
			self.set_field(definition, "actions", Operand::Reg(list))?;
		}
		self.session_auth_limit(definition)?;
		let rendered = match (parsed, parsed_path) {
			(Some((text, rendered)), _) => {
				self.set_field(definition, "path", Operand::Reg(text))?;
				Operand::Reg(rendered)
			}
			(None, Some(path)) => self.constant(Value::String(path.to_string().into()))?,
			(None, None) => return Err(IrError::Stale("ir_lower::ddl: an API path")),
		};
		let stored_at = self.object(&[("ns", ns_op), ("db", db_op), ("ap", rendered)])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ApiKey"),
			key: stored_at,
			ty: TypeId::named("StoredApiDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER API`: each clause applies in order to the definition read, then the security mode and
	/// the comment are replaced and the definition is stamped with the session's authority. The
	/// template holds, per clause, the stored values the clause sets: the configuration and the
	/// fallback of a `FOR ANY`, the methods and the action of a `FOR`, or the methods a `DROP`
	/// removes; and the types the clauses name, which the database must define.
	pub(super) fn alter_api(
		&mut self,
		attrs: &AlterAttrs,
		path: OperandSlot,
		clauses: &[AlterApiClauseShape],
		security: bool,
		comment: &AlterKind<String>,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		let Some(Value::Array(values)) = child(&template.value, "clauses") else {
			return Err(IrError::Stale(
				"ir_lower::ddl: an API alteration whose values are no list",
			));
		};
		if values.len() != clauses.len() {
			return Err(IrError::Stale(
				"ir_lower::ddl: an API alteration whose values do not match",
			));
		}
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let ap = self.name(path, NameClause::ApiPath)?.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ap", ap)])?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: definition,
		});
		let detail = self.object(&[("value", ap)])?;
		self.must_exist(attrs.if_exists, definition, StatementError::ApNotFound, detail)?;
		// The types the clauses name, which the template lists under `types`.
		let mut names = Vec::new();
		if let Some(Value::Array(listed)) = child(&template.value, "types") {
			for name in listed.iter() {
				let Value::String(name) = name else {
					return Err(IrError::Stale("ir_lower::ddl: an API alteration's type name"));
				};
				names.push(name.as_str().to_owned());
			}
		}
		if !names.is_empty() {
			let subject = self.shown_subject("The API", path)?;
			self.check_types_defined((ns, db), names, || subject);
		}
		let actions = self.reg()?;
		for (clause, set) in clauses.iter().zip(values.iter()) {
			match clause {
				AlterApiClauseShape::ForAny {
					config,
					fallback,
				} => {
					if let Some(config) = config {
						let stored = child(set, "config")
							.ok_or(IrError::Stale("ir_lower::ddl: an API configuration"))?;
						let value = self.constant(stored.clone())?;
						self.set_field(definition, "config", value)?;
						if let Some(list) = self.evaluated_middleware(config, Some(stored))? {
							self.set_path(
								definition,
								&["config", "middleware"],
								Operand::Reg(list),
							)?;
						}
					}
					match fallback {
						AlterKind::Set(_) | AlterKind::Drop => {
							let stored = child(set, "fallback")
								.ok_or(IrError::Stale("ir_lower::ddl: an API fallback"))?;
							let value = self.constant(stored.clone())?;
							self.set_field(definition, "fallback", value)?;
						}
						AlterKind::None => {}
					}
				}
				AlterApiClauseShape::SetAction(action) => {
					let methods = child(set, "methods")
						.ok_or(IrError::Stale("ir_lower::ddl: an API action's methods"))?;
					self.drop_methods(definition, actions, methods)?;
					let stored = child(set, "action")
						.ok_or(IrError::Stale("ir_lower::ddl: an API action"))?;
					self.get_field(definition, "actions", actions)?;
					let evaluated =
						self.evaluated_middleware(&action.config, child(stored, "config"))?;
					let value = self.constant(stored.clone())?;
					let value = self.load(value)?;
					if let Some(list) = evaluated {
						self.set_path(value, &["config", "middleware"], Operand::Reg(list))?;
					}
					self.store(StoreInstr::Push {
						list: actions,
						value,
					});
					self.set_field(definition, "actions", Operand::Reg(actions))?;
				}
				AlterApiClauseShape::DropAction {
					..
				} => {
					let methods = child(set, "methods")
						.ok_or(IrError::Stale("ir_lower::ddl: an API action's methods"))?;
					self.drop_methods(definition, actions, methods)?;
				}
			}
		}
		if security {
			let stored = child(&template.value, "security")
				.ok_or(IrError::Stale("ir_lower::ddl: an API security mode"))?;
			let value = self.constant(stored.clone())?;
			self.set_field(definition, "security", value)?;
		}
		match comment {
			AlterKind::Set(comment) => {
				let comment = self.constant(Value::String(comment.as_str().into()))?;
				self.set_field(definition, "comment", comment)?;
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None)?;
				self.set_field(definition, "comment", none)?;
			}
			AlterKind::None => {}
		}
		self.session_auth_limit(definition)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// Removes the methods in `methods` from the actions of the definition in `definition`, the
	/// actions being read into `actions`.
	fn drop_methods(
		&mut self,
		definition: Reg,
		actions: Reg,
		methods: &Value,
	) -> Result<(), IrError> {
		self.get_field(definition, "actions", actions)?;
		let methods = self.constant(methods.clone())?;
		let methods = self.load(methods)?;
		self.store(StoreInstr::DropMethods {
			actions,
			methods,
		});
		self.set_field(definition, "actions", Operand::Reg(actions))
	}
}
