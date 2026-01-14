use crate::cnf::MIGRATION_TABLE_PROBE_COUNT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::key::thing;
use crate::kvs::version::v3::{IssueKind, MigrationIssue, MigratorPass, PassState, Severity};
use crate::kvs::{KeyDecode, KeyEncode as _};
use crate::sql::visit::Visitor;
use crate::sql::{Array, Base, Id, Number, Object, Value};
use hashbrown::{Equivalent, HashMap};
use std::fmt::Write;
use std::hash;

/// The number of records we load per batch for checking the migration.
const RECORD_CHECK_BATCH_SIZE: u32 = 1024;

#[derive(Eq, PartialEq)]
pub enum TypeKey {
	Integer(usize),
	String(String),
}

impl hash::Hash for TypeKey {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			TypeKey::Integer(i) => {
				0u8.hash(state);
				i.hash(state);
			}
			TypeKey::String(s) => {
				1u8.hash(state);
				s.hash(state);
			}
		}
	}
}

#[derive(Eq, PartialEq)]
pub struct TypeKeyRef<'a>(&'a str);

impl hash::Hash for TypeKeyRef<'_> {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		1u8.hash(state);
		self.0.hash(state);
	}
}

impl<'a> Equivalent<TypeKey> for TypeKeyRef<'a> {
	fn equivalent(&self, key: &TypeKey) -> bool {
		match key {
			TypeKey::Integer(_) => false,
			TypeKey::String(b) => self.0 == b,
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum IdType {
	Integer,
	Float,
	Decimal,
}

const MAX_SCHEMA_TYPES: usize = 1024;

/// Struct to check for number values, within the same place in a key, which have a different type.
pub struct KeyConflictChecker {
	types: Vec<(HashMap<TypeKey, usize>, Option<IdType>)>,
}

impl KeyConflictChecker {
	pub fn new() -> Self {
		KeyConflictChecker {
			types: vec![(HashMap::new(), None)],
		}
	}

	/// Checks if the new id value has a number conflict with already existing schema.
	///
	/// Will return None if the schema has grown too large to keep track off.
	pub fn check_conflict(&mut self, id: &Id) -> Option<bool> {
		match id {
			Id::Array(array) => return self.visit_array(array, 0),
			Id::Object(object) => return self.visit_object(object, 0),
			_ => {}
		}
		Some(false)
	}

	fn visit_array(&mut self, array: &Array, type_idx: usize) -> Option<bool> {
		for (k, v) in array.iter().enumerate() {
			if let Some(x) = self.types[type_idx].0.get(&TypeKey::Integer(k)) {
				if self.visit_value(v, *x)? {
					return Some(true);
				}
			} else {
				let idx = self.build_value(v)?;
				self.types[type_idx].0.insert(TypeKey::Integer(k), idx);
			}
		}
		Some(false)
	}

	fn visit_object(&mut self, object: &Object, type_idx: usize) -> Option<bool> {
		for (k, v) in object.iter() {
			if let Some(x) = self.types[type_idx].0.get(&TypeKeyRef(k)) {
				if self.visit_value(v, *x)? {
					return Some(true);
				}
			} else {
				let idx = self.build_value(v)?;
				self.types[type_idx].0.insert(TypeKey::String(k.clone()), idx);
			}
		}
		Some(false)
	}

	fn visit_value(&mut self, value: &Value, type_idx: usize) -> Option<bool> {
		match value {
			Value::Number(number) => {
				let kind = match number {
					Number::Int(_) => IdType::Integer,
					Number::Float(_) => IdType::Float,
					Number::Decimal(_) => IdType::Decimal,
				};
				if let Some(x) = self.types[type_idx].1 {
					if x != kind {
						// conflict
						return Some(true);
					}
				} else {
					self.types[type_idx].1 = Some(kind);
				}
				Some(false)
			}
			Value::Array(array) => self.visit_array(array, type_idx),
			Value::Object(object) => self.visit_object(object, type_idx),
			_ => Some(false),
		}
	}

	fn build_value(&mut self, value: &Value) -> Option<usize> {
		match value {
			Value::Number(number) => {
				let kind = match number {
					Number::Int(_) => IdType::Integer,
					Number::Float(_) => IdType::Float,
					Number::Decimal(_) => IdType::Decimal,
				};
				let res = self.types.len();
				if res >= MAX_SCHEMA_TYPES {
					return None;
				}
				self.types.push((HashMap::new(), Some(kind)));
				Some(res)
			}
			Value::Array(array) => {
				let mut object_schema = HashMap::new();
				for (k, v) in array.iter().enumerate() {
					let idx = self.build_value(v)?;
					object_schema.insert(TypeKey::Integer(k), idx);
				}
				let res = self.types.len();
				if res >= MAX_SCHEMA_TYPES {
					return None;
				}
				self.types.push((object_schema, None));
				Some(res)
			}
			Value::Object(object) => {
				let mut object_schema = HashMap::new();
				for (k, v) in object.iter() {
					let idx = self.build_value(v)?;
					object_schema.insert(TypeKey::String(k.clone()), idx);
				}
				let res = self.types.len();
				if res >= MAX_SCHEMA_TYPES {
					return None;
				}
				self.types.push((object_schema, None));
				Some(res)
			}
			_ => {
				let res = self.types.len();
				if res >= MAX_SCHEMA_TYPES {
					return None;
				}
				self.types.push((HashMap::new(), None));
				Some(res)
			}
		}
	}
}

/// The number of records we load per batch for checking the migration.
const RECORD_CHECK_BATCH_SIZE: u32 = 1024;

pub async fn diagnose(
	(ctx, opts): (&Context, &Options),
	(probe,): (Option<bool>,),
) -> Result<Value, Error> {
	let probe = probe.unwrap_or(true);

	let mut issues = Vec::new();
	let mut export = String::new();
	let mut path = String::new();

	if let Ok(x) = opts.ns() {
		diagnose_ns(ctx, opts, probe, x, &mut issues, &mut path, &mut export).await?;
	} else {
		opts.is_allowed(Action::View, ResourceKind::Namespace, &Base::Root)?;

		let tx = ctx.tx();

		for ns in tx.all_ns().await?.iter() {
			diagnose_ns(ctx, opts, probe, &ns.name.0, &mut issues, &mut path, &mut export).await?;
		}

		for access in tx.all_root_accesses().await?.iter() {
			let mut pass =
				MigratorPass::new(&mut issues, &mut export, &mut path, PassState::default());
			let _ = pass.visit_define_access(access);
		}
	}

	let res = issues.iter().map(|x| Value::from(x.to_object())).collect::<Value>();

	Ok(res)
}

async fn diagnose_ns(
	ctx: &Context,
	opts: &Options,
	probe: bool,
	ns: &str,
	issues: &mut Vec<MigrationIssue>,
	path: &mut String,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_ns(Some(ns.into()));

	let len = path.len();
	write!(path, "/ns/{ns}").expect("Writing into a string cannot fail");

	if let Ok(db) = opts.db() {
		diagnose_ns_db(ctx, &opts, probe, ns, db, issues, path, export).await?
	} else {
		opts.is_allowed(Action::View, ResourceKind::Database, &Base::Ns)?;

		let tx = ctx.tx();

		for db in tx.all_db(ns).await?.iter() {
			diagnose_ns_db(ctx, &opts, probe, ns, &db.name.0, issues, path, export).await?
		}

		for access in tx.all_ns_accesses(ns).await?.iter() {
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			let _ = pass.visit_define_access(access);
		}
	}

	path.truncate(len);
	Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn diagnose_ns_db(
	ctx: &Context,
	opts: &Options,
	probe: bool,
	ns: &str,
	db: &str,
	issues: &mut Vec<MigrationIssue>,
	path: &mut String,
	export: &mut String,
) -> Result<(), Error> {
	let opts = opts.clone().with_db(Some(db.into()));

	opts.is_allowed(Action::View, ResourceKind::Database, &Base::Db)?;

	let len = path.len();
	write!(path, "/db/{db}").expect("Writing into a string cannot fail");

	let tx = ctx.tx();

	for f in tx.all_db_functions(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_function(f);
	}

	// TODO: No versioning at the moment,
	// Possibly add?
	for t in tx.all_tb(ns, db, None).await?.iter() {
		{
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			let _ = pass.visit_define_table(t);
			for f in tx.all_tb_fields(ns, db, &t.name.0, None).await?.iter() {
				let _ = pass.visit_define_field(f);
			}
		}

		let len = path.len();
		write!(path, "/table/{}", &t.name.0).expect("Writing into a string cannot fail");

		{
			let mut pass = MigratorPass::new(issues, export, path, PassState::default());
			for e in tx.all_tb_events(ns, db, &t.name).await?.iter() {
				let _ = pass.visit_define_event(e);
			}
		}

		let mut begin = thing::prefix(ns, db, &t.name)?;
		let end = thing::suffix(ns, db, &t.name)?;
		let mut count = if probe {
			*MIGRATION_TABLE_PROBE_COUNT
		} else {
			usize::MAX
		};

		let mut schema_checker = KeyConflictChecker::new();
		let mut found_key_issue = false;

		while count != 0 {
			let limit = (RECORD_CHECK_BATCH_SIZE as usize).min(count) as u32;

			let r = tx.scan(begin.as_slice()..end.as_slice(), limit, None).await?;

			if r.is_empty() {
				break;
			}

			let last = r.len() - 1;
			for (idx, (k, v)) in r.into_iter().enumerate() {
				let k = thing::Thing::decode(&k)?;

				let len = path.len();
				write!(path, "/record/{}", k.id).expect("Writing into a string cannot fail");

				if !found_key_issue {
					match schema_checker.check_conflict(&k.id) {
						Some(true) => {
							found_key_issue = true;
							issues.push(MigrationIssue{
								severity: Severity::CanBreak,
								error: "Found number keys with different types in the same position within a record-id key which will have a different order in 3.0".to_owned(),
								details: String::new(),
								kind: IssueKind::NumberKeyOrdering,
								origin: path.clone(),
								error_location: None,
								resolution: None,
							});
						} // no issue
						Some(false) => {} // no issue
						None => {
							// Schema too chaotic to check without blowing up memory
							// usage.
							found_key_issue = true;
							issues.push(MigrationIssue{
								severity: Severity::CanBreak,
								error: "Found table key schema with a very poly-morphic type, table could contain keys which might have a different ordering in 3.0".to_owned(),
								details: String::new(),
								kind: IssueKind::NumberKeyOrdering,
								origin: path.clone(),
								error_location: None,
								resolution: None,
							});
						}
					}
				}

				let v = revision::from_slice::<Value>(&v)?;

				{
					let mut pass = MigratorPass::new(
						issues,
						export,
						path,
						PassState {
							breaking_futures: true,
							breaking_closures: true,
							..PassState::default()
						},
					);
					let _ = pass.visit_value(&v);
					for f in tx.all_tb_fields(ns, db, &t.name.0, None).await?.iter() {
						let _ = pass.visit_define_field(f);
					}
				}

				if idx == last {
					begin.clear();
					k.encode_into(&mut begin)?;
					begin.push(0xff);
				}

				path.truncate(len);
			}

			count -= limit as usize;
		}

		path.truncate(len);
	}

	for access in tx.all_db_accesses(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_access(access);
	}

	for api in tx.all_db_apis(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_api_definition(api);
	}

	for api in tx.all_db_params(ns, db).await?.iter() {
		let mut pass = MigratorPass::new(issues, export, path, PassState::default());
		let _ = pass.visit_define_param(api);
	}

	path.truncate(len);

	Ok(())
}
