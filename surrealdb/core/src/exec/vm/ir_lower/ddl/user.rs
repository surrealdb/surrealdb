//! `DEFINE USER` and `ALTER USER`.
//!
//! A user's credentials reach the program only as constants of the definition it writes. The
//! derived hash, the session code and the SCRAM verifier are already computed when the executor
//! admits the statement, and they are encoded into the constant here, never into an attribute
//! that prints. A statement that still holds a plaintext password is refused, so no definition is
//! written with an empty hash.

use surrealdb_strand::Strand;

use super::{
	AlterAttrs, AlterTemplate, Base, Ddl, DefineAttrs, IrError, NameClause, Operand, Reg, RouteId,
	StatementError, StoreInstr, TypeId, Value, stored,
};
use crate::exec::ir::iface::statements::{AlterUser, DefineUser};
use crate::exec::vm::program::Instr;
use crate::expr::statements::alter::AlterKind;

/// The route the users of `base` are stored under.
const fn user_route(base: Base) -> &'static str {
	match base {
		Base::Root => "RootUserKey",
		Base::Ns => "NsUserKey",
		Base::Db => "DbUserKey",
	}
}

impl Ddl<'_, '_> {
	/// `DEFINE USER`. Reads the durations and the comment before the name, and creates the scope
	/// ids before the existence check.
	pub(super) fn define_user(
		&mut self,
		attrs: &DefineAttrs,
		user: &DefineUser,
	) -> Result<(), IrError> {
		if user.password.is_some() {
			return Err(IrError::Stale("ir_lower::ddl: a user whose password is not yet hashed"));
		}
		self.guard()?;
		let (token, token_evaluated) = self.duration(user.duration.token, true)?;
		let (session, session_evaluated) = self.duration(user.duration.session, true)?;
		let comment = self.comment(user.comment)?;
		let name = self.name(user.name, NameClause::User)?;
		let (route, mut fields, exists) = match user.base {
			Base::Root => {
				(user_route(user.base), Vec::new(), StatementError::UserRootAlreadyExists)
			}
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					user_route(user.base),
					vec![("ns", Operand::Reg(ns))],
					StatementError::UserNsAlreadyExists,
				)
			}
			Base::Db => {
				let (ns, db) = self.ids_creating()?;
				(
					user_route(user.base),
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::UserDbAlreadyExists,
				)
			}
		};
		fields.push(("user", name.operand));
		let key = self.object(&fields)?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: existing,
		});
		let base = user.base;
		let missing = self.exists_with(
			&attrs.mode,
			existing,
			|l| {
				let shown = l.reg()?;
				l.get_field(existing, "name", shown)?;
				l.missing_at_base(false, base, ("name", Operand::Reg(shown)), exists)
			},
			|_| Ok(()),
		)?;
		self.bind(missing)?;
		let template = self.user_template(attrs, user)?;
		let template = self.constant(template)?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		for (field, value, evaluated) in [
			("token_duration", token, token_evaluated),
			("session_duration", session, session_evaluated),
		] {
			if evaluated {
				self.set_field(definition, field, value)?;
			}
		}
		self.set_name(definition, "name", &name)?;
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// The definition `DEFINE USER` writes: the statement's template with the hash, the code and
	/// the SCRAM verifier the statement carries.
	fn user_template(&self, attrs: &DefineAttrs, user: &DefineUser) -> Result<Value, IrError> {
		let secrets = crate::catalog::UserDefinition {
			name: Default::default(),
			hash: user.secrets.hash.clone(),
			code: user.secrets.code.clone(),
			roles: Vec::new(),
			token_duration: None,
			session_duration: None,
			comment: None,
			base: user.base.into(),
			scram: user.secrets.scram.clone(),
		};
		let (Value::Object(mut template), Value::Object(secrets)) =
			(attrs.template.value.clone(), stored(&secrets, "UserDefinition")?)
		else {
			return Err(IrError::Stale("ir_lower::ddl: a user template that is not an object"));
		};
		for field in ["hash", "code", "scram"] {
			template.insert(field.to_owned(), secrets.get(field).cloned().unwrap_or(Value::None));
		}
		Ok(Value::Object(template))
	}

	/// `ALTER USER`: the user is read before any clause replaces its fields. On a namespace the
	/// session's namespace is created when it does not exist; on a database it must exist.
	pub(super) fn alter_user(
		&mut self,
		attrs: &AlterAttrs,
		user: &AlterUser,
	) -> Result<(), IrError> {
		if user.password.is_some() {
			return Err(IrError::Stale("ir_lower::ddl: a user whose password is not yet hashed"));
		}
		let template = alter_user_template(user)?;
		self.guard()?;
		let name = self.name(user.name, NameClause::User)?.operand;
		let (route, mut fields, missing) = match user.base {
			Base::Root => (user_route(user.base), Vec::new(), StatementError::UserRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					user_route(user.base),
					vec![("ns", Operand::Reg(ns))],
					StatementError::UserNsNotFound,
				)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					user_route(user.base),
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::UserDbNotFound,
				)
			}
		};
		fields.push(("user", name));
		let key = self.object(&fields)?;
		let definition: Reg = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: present,
		});
		self.missing_at_base(attrs.if_exists, user.base, ("name", name), missing)?;
		self.bind(present)?;
		self.replace_fields(definition, &template)?;
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

/// What `ALTER USER` replaces in the stored user: the definition its clauses form, and the
/// fields they set or drop.
pub(super) fn alter_user_template(user: &AlterUser) -> Result<AlterTemplate, IrError> {
	let mut altered = crate::catalog::UserDefinition {
		name: Default::default(),
		hash: String::new(),
		code: String::new(),
		roles: Vec::new(),
		token_duration: None,
		session_duration: None,
		comment: None,
		base: user.base.into(),
		scram: None,
	};
	let mut fields: Vec<&'static str> = Vec::new();
	if let Some(hash) = &user.hash {
		altered.hash.clone_from(hash);
		fields.push("hash");
	}
	if let Some(scram) = &user.scram {
		altered.scram.clone_from(scram);
		fields.push("scram");
	}
	match &user.roles {
		AlterKind::Set(roles) => {
			altered.roles = roles.to_vec();
			fields.push("roles");
		}
		AlterKind::Drop => fields.push("roles"),
		AlterKind::None => {}
	}
	for (clause, slot, field) in [
		(&user.token_duration, &mut altered.token_duration, "token_duration"),
		(&user.session_duration, &mut altered.session_duration, "session_duration"),
	] {
		match clause {
			AlterKind::Set(duration) => *slot = duration.map(crate::val::BoundedDuration),
			AlterKind::Drop => *slot = None,
			AlterKind::None => continue,
		}
		fields.push(field);
	}
	match &user.comment {
		AlterKind::Set(comment) => {
			altered.comment = Some(comment.clone());
			fields.push("comment");
		}
		AlterKind::Drop => fields.push("comment"),
		AlterKind::None => {}
	}
	Ok(AlterTemplate {
		ty: "UserDefinition",
		value: stored(&altered, "UserDefinition")?,
		fields: fields.into_iter().map(Strand::new).collect(),
	})
}
