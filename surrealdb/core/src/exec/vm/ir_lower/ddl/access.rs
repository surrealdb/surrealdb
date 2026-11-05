//! `DEFINE ACCESS` and `ALTER ACCESS`.
//!
//! The definition an access method is stored as is the statement's template, whose keys and URLs
//! are empty. A key the statement writes is set from the text its `AccessSecrets` hold, at the
//! step the body reads it; one it evaluates is converted into a register and set from there, and
//! an evaluated key is refused afterwards when it is the redaction placeholder.

use super::{
	AlterAttrs, Base, ClauseConversion, Ddl, DefineAttrs, DefineKind, IrError, NameClause, Operand,
	OperandSlot, Reg, RouteId, StatementError, StoreInstr, TypeId, Value,
};
use crate::exec::ir::iface::statements::{
	AccessSecrets, AccessTypeShape, AlterAccess, DefineAccess, JwtShape, JwtVerifyShape,
};
use crate::exec::vm::program::{AccessCheck, Instr};
use crate::expr::Algorithm;

/// What the keys and the evaluated clauses of a definition add to its template.
#[derive(Default)]
struct Fills {
	/// The stored field each value is set at, in the order the body computes them.
	set: Vec<(Vec<&'static str>, Operand)>,
	/// The registers that hold an evaluated key, which the statement refuses when one is the
	/// redaction placeholder.
	keys: Vec<Reg>,
}

/// The stored form of an access algorithm.
fn catalog_algorithm(alg: Algorithm) -> crate::catalog::Algorithm {
	use crate::catalog::Algorithm as A;
	match alg {
		Algorithm::EdDSA => A::EdDSA,
		Algorithm::Es256 => A::Es256,
		Algorithm::Es384 => A::Es384,
		Algorithm::Es512 => A::Es512,
		Algorithm::Hs256 => A::Hs256,
		Algorithm::Hs384 => A::Hs384,
		Algorithm::Hs512 => A::Hs512,
		Algorithm::Ps256 => A::Ps256,
		Algorithm::Ps384 => A::Ps384,
		Algorithm::Ps512 => A::Ps512,
		Algorithm::Rs256 => A::Rs256,
		Algorithm::Rs384 => A::Rs384,
		Algorithm::Rs512 => A::Rs512,
	}
}

/// Whether a JWT configuration of the method uses ES512.
fn uses_es512(jwt: &JwtShape) -> bool {
	let verify = matches!(
		jwt.verify,
		JwtVerifyShape::Key {
			alg: Algorithm::Es512,
			..
		}
	);
	verify || jwt.issue.as_ref().is_some_and(|issue| issue.alg == Algorithm::Es512)
}

/// The path of the JWT configuration of a record method, of its bearer half, of a JWT method and
/// of a bearer method in the stored definition.
const RECORD_JWT: &[&str] = &["access_type", "Record", "jwt"];
const RECORD_BEARER_JWT: &[&str] = &["access_type", "Record", "bearer", "jwt"];
const JWT: &[&str] = &["access_type", "Jwt"];
const BEARER_JWT: &[&str] = &["access_type", "Bearer", "jwt"];

/// The JWT configurations of an access method, in the order the body reads them, each with the
/// path of it in the stored definition.
fn jwts(access_type: &AccessTypeShape) -> Vec<(&JwtShape, &'static [&'static str])> {
	match access_type {
		AccessTypeShape::Record(record) => {
			let mut jwts = vec![(&record.jwt, RECORD_JWT)];
			if let Some(bearer) = &record.bearer {
				jwts.push((&bearer.jwt, RECORD_BEARER_JWT));
			}
			jwts
		}
		AccessTypeShape::Jwt(jwt) => vec![(jwt, JWT)],
		AccessTypeShape::Bearer(bearer) => vec![(&bearer.jwt, BEARER_JWT)],
	}
}

impl Ddl<'_, '_> {
	/// `ALTER ACCESS`: the method is read before any clause replaces its fields, and the altered
	/// method is checked against what its type admits before it is written back. On a namespace
	/// the session's namespace is created when it does not exist; on a database it must exist.
	pub(super) fn alter_access(
		&mut self,
		attrs: &AlterAttrs,
		access: &AlterAccess,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let ac = self.name(access.name, NameClause::Access)?.operand;
		// A named type is a type of a database, so only a database access method can name one.
		let mut database = None;
		let (route, mut fields, missing) = match access.base {
			Base::Root => ("RootAccessMethodKey", Vec::new(), StatementError::AccessRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					"NsAccessMethodKey",
					vec![("ns", Operand::Reg(ns))],
					StatementError::AccessNsNotFound,
				)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				database = Some((ns, db));
				(
					"DbAccessMethodKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbNotFound,
				)
			}
		};
		fields.push(("ac", ac));
		let key = self.object(&fields)?;
		let method = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: method,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(method),
			truthy: true,
			target: present,
		});
		self.missing_at_base(attrs.if_exists, access.base, ("ac", ac), missing)?;
		self.bind(present)?;
		if let Some(database) = database {
			let subject = self.shown_subject("The access method", access.name)?;
			self.check_altered_types(
				template,
				database,
				crate::exec::vm::schema_host::access_named_types,
				|| subject,
			)?;
		}
		self.replace_fields(method, template)?;
		self.store(StoreInstr::AccessCheck {
			check: AccessCheck::Altered {
				access: method,
			},
		});
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			value: method,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Ddl<'_, '_> {
	/// `DEFINE ACCESS`: the durations, the comment and the keys are read before the name, as the
	/// body reads them, and the scope's ids are created ahead of the existence check. A record
	/// method's token duration is checked as it is read; ES512 is refused once the statement goes
	/// on to write.
	pub(super) fn define_access(
		&mut self,
		attrs: &DefineAttrs,
		access: &DefineAccess,
	) -> Result<(), IrError> {
		self.guard()?;
		let mut fills = Fills::default();
		let record = matches!(access.access_type, AccessTypeShape::Record(_));
		let (grant, evaluated) = self.duration(access.duration.grant, true)?;
		if evaluated {
			fills.set.push((vec!["grant_duration"], grant));
		}
		let written_none = match self.literal(access.duration.token)? {
			Some(value) => {
				matches!(value.clone().cast_to::<Option<crate::val::Duration>>(), Ok(None))
			}
			None => false,
		};
		let (token, evaluated) = self.duration(access.duration.token, true)?;
		if evaluated {
			fills.set.push((vec!["token_duration"], token));
		}
		if record {
			// A written `NONE` is converted and checked where the body checks the duration, so the
			// statement fails with the body's error there.
			let token = match token {
				Operand::Reg(token) if evaluated => Some(token),
				Operand::Reg(_) | Operand::Const(_) | Operand::Field(_) if written_none => {
					let none = self.constant(Value::None)?;
					let token = self.load(none)?;
					self.store(StoreInstr::Convert {
						src: token,
						dst: token,
						clause: ClauseConversion::Duration,
					});
					Some(token)
				}
				Operand::Reg(_) | Operand::Const(_) | Operand::Field(_) => None,
			};
			if let Some(token) = token {
				self.store(StoreInstr::AccessCheck {
					check: AccessCheck::RecordToken {
						token,
					},
				});
			}
		}
		let (session, evaluated) = self.duration(access.duration.session, true)?;
		if evaluated {
			fills.set.push((vec!["session_duration"], session));
		}
		let comment = self.comment(access.comment)?;
		if comment.evaluated {
			fills.set.push((vec!["comment"], comment.operand));
		}
		let jwts = jwts(&access.access_type);
		for (jwt, at) in &jwts {
			self.access_jwt(jwt, at, &access.secrets, &mut fills)?;
		}
		let name = self.name(access.name, NameClause::Access)?;
		if !fills.keys.is_empty() {
			let shown = self.load(name.operand)?;
			for key in std::mem::take(&mut fills.keys) {
				self.store(StoreInstr::AccessCheck {
					check: AccessCheck::Unredacted {
						key,
						name: shown,
					},
				});
			}
		}
		// The database a database access method is defined in, whose types its clauses may name.
		let mut database = None;
		let (route, mut fields, exists) = match access.base {
			Base::Root => {
				("RootAccessMethodKey", Vec::new(), StatementError::AccessRootAlreadyExists)
			}
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					"NsAccessMethodKey",
					vec![("ns", Operand::Reg(ns))],
					StatementError::AccessNsAlreadyExists,
				)
			}
			Base::Db => {
				let (ns, db) = self.ids_creating()?;
				database = Some((ns, db));
				(
					"DbAccessMethodKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbAlreadyExists,
				)
			}
		};
		fields.push(("ac", name.operand));
		let key = self.object(&fields)?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: existing,
		});
		let base = access.base;
		let missing = self.exists_with(
			&attrs.mode,
			existing,
			|l| {
				let shown = l.reg()?;
				l.get_field(existing, "name", shown)?;
				l.missing_at_base(false, base, ("ac", Operand::Reg(shown)), exists)
			},
			|_| Ok(()),
		)?;
		self.bind(missing)?;
		if let Some(database) = database {
			let written: crate::catalog::AccessDefinition =
				super::definition_of(&attrs.template.value, "StoredAccessDefinition")?;
			let names = crate::exec::vm::schema_host::access_named_types(&written);
			if !names.is_empty() {
				let subject = self.shown_subject("The access method", access.name)?;
				self.check_types_defined(database, names, || subject);
			}
		}
		if jwts.iter().any(|(jwt, _)| uses_es512(jwt)) {
			self.store(StoreInstr::AccessCheck {
				check: AccessCheck::RejectEs512 {
					existing,
					overwrite: attrs.mode == DefineKind::Overwrite,
				},
			});
		}
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_name(definition, "name", &name)?;
		for (path, value) in &fills.set {
			self.set_path(definition, path, *value)?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// A JWT configuration at `at` in the stored definition: its verification key or URL, its
	/// issuer key, the check that a symmetric algorithm's two keys are the same, then its
	/// audiences.
	fn access_jwt(
		&mut self,
		jwt: &JwtShape,
		at: &[&'static str],
		secrets: &AccessSecrets,
		fills: &mut Fills,
	) -> Result<(), IrError> {
		let verify = match &jwt.verify {
			JwtVerifyShape::Key {
				alg,
				key,
			} => {
				let (text, reg) = self.access_string(
					*key,
					ClauseConversion::Coerced,
					(at, &["verify", "Key", "key"]),
					secrets,
					fills,
				)?;
				fills.keys.extend(reg);
				Some((*alg, text, reg))
			}
			JwtVerifyShape::Jwks {
				url,
			} => {
				self.access_string(
					*url,
					ClauseConversion::Cast,
					(at, &["verify", "Jwks", "url"]),
					secrets,
					fills,
				)?;
				None
			}
		};
		let issue = match &jwt.issue {
			Some(issue) => {
				let (text, reg) = self.access_string(
					issue.key,
					ClauseConversion::Cast,
					(at, &["issue", "key"]),
					secrets,
					fills,
				)?;
				fills.keys.extend(reg);
				Some((text, reg))
			}
			None => None,
		};
		if let (Some((alg, verify_text, verify_reg)), Some((issue_text, issue_reg))) =
			(verify, issue)
			&& alg.is_symmetric()
		{
			match (verify_reg, issue_reg) {
				(None, None) if verify_text == issue_text => {}
				(verify_reg, issue_reg) => {
					let verify = self.written_or(verify_reg, &verify_text)?;
					let issue = self.written_or(issue_reg, &issue_text)?;
					self.store(StoreInstr::AccessCheck {
						check: AccessCheck::SymmetricKeys {
							alg: catalog_algorithm(alg),
							verify,
							issue,
						},
					});
				}
			}
		}
		if let Some(slots) = &jwt.audience {
			self.access_audience(slots, at, fills)?;
		}
		Ok(())
	}

	/// The register that holds `reg`, or the text `written` when there is none.
	fn written_or(&mut self, reg: Option<Reg>, written: &str) -> Result<Reg, IrError> {
		match reg {
			Some(reg) => Ok(reg),
			None => {
				let text = self.constant(Value::String(written.into()))?;
				self.load(text)
			}
		}
	}

	/// A string clause of a JWT configuration, at `path` under `at`: the text it is written as, set
	/// in the definition, or the register the evaluated clause is converted into, set from there.
	/// Answers the text, empty when the clause is evaluated, and the register.
	fn access_string(
		&mut self,
		slot: OperandSlot,
		clause: ClauseConversion,
		(at, path): (&[&'static str], &[&'static str]),
		secrets: &AccessSecrets,
		fills: &mut Fills,
	) -> Result<(String, Option<Reg>), IrError> {
		let at: Vec<&'static str> = at.iter().chain(path).copied().collect();
		if let Some(text) = secrets.written(slot) {
			let text = text.to_owned();
			let operand = self.constant(Value::String(text.as_str().into()))?;
			self.read_as(slot, operand)?;
			fills.set.push((at, operand));
			return Ok((text, None));
		}
		let reg = self.converted(slot, clause)?;
		fills.set.push((at, Operand::Reg(reg)));
		Ok((String::new(), Some(reg)))
	}

	/// The audiences of a JWT configuration at `at`: held by the template when each is written as
	/// a value that coerces to text, and otherwise the list each is read into, set in the
	/// definition.
	fn access_audience(
		&mut self,
		slots: &[OperandSlot],
		at: &[&'static str],
		fills: &mut Fills,
	) -> Result<(), IrError> {
		let mut written = Vec::with_capacity(slots.len());
		for slot in slots {
			written.push(match self.literal(*slot)? {
				Some(value) => value.clone().coerce_to::<String>().ok(),
				None => None,
			});
		}
		let all_written = written.iter().all(Option::is_some);
		if all_written {
			for (slot, text) in slots.iter().zip(written) {
				let text = text.ok_or(IrError::Stale("ir_lower::ddl: an audience"))?;
				let operand = self.constant(Value::String(text.into()))?;
				self.read_as(*slot, operand)?;
			}
			return Ok(());
		}
		let list = self.reg()?;
		self.emit(Instr::NewArray {
			dst: list,
		});
		for (slot, text) in slots.iter().zip(written) {
			let entry = match text {
				Some(text) => {
					let operand = self.constant(Value::String(text.into()))?;
					self.read_as(*slot, operand)?;
					operand
				}
				None => Operand::Reg(self.converted(*slot, ClauseConversion::Coerced)?),
			};
			self.emit(Instr::ArrayPush {
				array: list,
				value: entry,
			});
		}
		fills.set.push((at.iter().chain(&["audience"]).copied().collect(), Operand::Reg(list)));
		Ok(())
	}
}
