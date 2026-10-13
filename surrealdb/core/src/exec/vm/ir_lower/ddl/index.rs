//! `DEFINE INDEX`.
//!
//! An index names fields of its table. A statement whose columns are all plain paths with literal
//! subscripts writes them, and its template holds them; any other statement resolves its columns
//! when it runs: a path whose subscripts are evaluated, `type::fields(x)`, `type::field(x)` or an
//! expression that evaluates to a path. Every column is then checked against the catalog the
//! statement finds when it runs, so a column that names a computed field is refused there.

use super::{
	Ddl, DefineAttrs, Instr, IrError, NameClause, Operand, OperandSlot, Reg, RouteId,
	StatementError, StoreInstr, StoreRefusal, TypeId, Value,
};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::statements::{IndexColumn, IndexSpec, Place, PlacePart};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::ir_lower::expr::Lowerer;
use crate::exec::vm::program::{ColumnSource, Label, PathSource};
use crate::expr::{Expr, Function, Idiom, Part};

/// How a stored column is resolved when the statement runs.
enum StoredColumn {
	/// `type::fields(x)`: the value of the argument, which holds the paths.
	TypeFields(crate::exec::ir::iface::ids::ValueId),
	/// `type::field(x)`: the value of the argument, which holds the path.
	TypeField(crate::exec::ir::iface::ids::ValueId),
	/// One of the two calls written with no argument.
	Refused(bool),
	/// Any other expression: its value, read as the text of a path.
	Evaluated(crate::exec::ir::iface::ids::ValueId),
}

/// The single path step that `text` spells, kept as written. `text` is the step's canonical
/// SurrealQL and parses as one step after a field.
fn kept_part(text: &str) -> Result<Part, IrError> {
	use std::str::FromStr as _;
	let idiom = Idiom::from_str(&format!("_{text}"))
		.map_err(|_| IrError::Stale("ir_lower::ddl: a kept index path step that does not parse"))?;
	match <[Part; 2]>::try_from(idiom.0) {
		Ok([_, step]) => Ok(step),
		Err(_) => Err(IrError::Stale("ir_lower::ddl: a kept index path step that is not one step")),
	}
}

impl Ddl<'_, '_> {
	/// `DEFINE INDEX`: the table is created when it does not exist and refused when it is a
	/// lightweight relation. Each column is checked against the catalog before the definition is
	/// written, and a replaced index is retired. An import that replays an unchanged definition
	/// keeps the index's data and refreshes the definition alone.
	pub(super) fn define_index(
		&mut self,
		attrs: &DefineAttrs,
		(name, name_text): (OperandSlot, &str),
		table: OperandSlot,
		(columns, index): (&[IndexColumn], &IndexSpec),
		(comment, concurrently): (OperandSlot, bool),
	) -> Result<(), IrError> {
		let is_count = matches!(index, IndexSpec::Count(_));
		self.guard()?;
		let name = self.name(name, NameClause::Index)?;
		let table = self.name(table, NameClause::IndexTable)?;
		let tb = table.operand;
		let (ns, db, database) = self.selected_ids(StatementError::DbNotFound)?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.table_creating((ns, db, database), tb, tb_key)?;
		self.store(StoreInstr::RefuseLightweight {
			table: table_def,
			refusal: StoreRefusal::Index,
		});
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name.operand)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: existing,
		});
		// The refusal names the index as the statement writes it.
		let shown = self.constant(Value::String(name_text.into()))?;
		let shown = self.load(shown)?;
		let missing = self.exists(&attrs.mode, existing, StatementError::IxAlreadyExists, shown)?;
		self.bind(missing)?;

		// Each column names a field the table defines, or a path beneath one whose type admits
		// sub-fields, and no computed one, or a component of the table's record id; on a
		// schemaless table a column may name no field at all.
		let schemafull = self.reg()?;
		self.get_field(table_def, "schemafull", schemafull)?;
		let written = self.written_columns(columns)?;
		let evaluated_cols = match &written {
			Some(_) => None,
			None => {
				let list = self.reg()?;
				self.emit(Instr::NewArray {
					dst: list,
				});
				for column in columns {
					self.resolve_column(column, list)?;
				}
				self.check_columns((ns, db), (tb, name.operand), list, schemafull)?;
				Some(list)
			}
		};
		for idiom in written.iter().flatten() {
			let fd = idiom.to_raw_string();
			let mut next = Vec::new();
			let checked =
				self.index_column_field((ns_op, db_op, tb), &fd, &fd, name.operand, false)?;
			next.push(checked);
			if let Some(Part::Field(first)) = idiom.0.first() {
				let admitted = self.index_column_field(
					(ns_op, db_op, tb),
					first.as_str(),
					first.as_str(),
					name.operand,
					true,
				)?;
				next.push(admitted);
			}
			// When neither the field nor its parent settles the column, the catalog checks it. A
			// component of the table's record id is accepted as the whole column and refused
			// beneath it. On a schemafull table, any other path that no field covers is missing.
			let named = self.constant(Value::String(fd.as_str().into()))?;
			let named = self.load(named)?;
			let list = self.reg()?;
			self.emit(Instr::NewArray {
				dst: list,
			});
			self.emit(Instr::ArrayPush {
				array: list,
				value: Operand::Reg(named),
			});
			self.check_columns((ns, db), (tb, name.operand), list, schemafull)?;
			for label in next {
				self.bind(label)?;
			}
		}

		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_name(definition, "name", &name)?;
		self.set_name(definition, "table_name", &table)?;
		self.set_comment(definition, &comment)?;
		if let Some(list) = evaluated_cols {
			self.set_field(definition, "cols", Operand::Reg(list))?;
		}

		// An import replaying a definition whose index is unchanged keeps the index's data and
		// its on-disk format, refreshing only the definition.
		if matches!(attrs.mode, crate::expr::statements::define::DefineKind::Default) {
			let fresh = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(existing),
				truthy: false,
				target: fresh,
			});
			let reuses = self.reg()?;
			self.store(StoreInstr::IndexReplayReuses {
				existing,
				template: definition,
				dst: reuses,
			});
			let changed = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(reuses),
				truthy: false,
				target: changed,
			});
			for field in ["index_id", "format_version"] {
				let kept = self.reg()?;
				self.get_field(existing, field, kept)?;
				self.set_field(definition, field, Operand::Reg(kept))?;
			}
			self.put_index_definition((ns, db, tb), key, definition, name.operand)?;
			self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
			self.store(StoreInstr::ClearCache);
			self.answer_none()?;
			self.bind(fresh)?;
			self.bind(changed)?;
		}

		// A definition that consumes the table's shared doc-id space adopts its mappings, so a
		// queued reclaim of the space is cancelled first.
		if !is_count {
			let tb_reg = self.load(tb)?;
			self.store(StoreInstr::ClaimDocIdsReclaim {
				ns,
				db,
				tb: tb_reg,
				upgrading: None,
			});
		}
		// A replaced index is retired; its doc-id space goes too when it was the last consumer
		// and the replacement consumes none.
		let fresh = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: fresh,
		});
		self.retire_index((ns, db, tb), key, existing, table_def, is_count)?;
		self.bind(fresh)?;

		// A (re)defined index takes a fresh id.
		let tb_reg = self.load(tb)?;
		let id = self.reg()?;
		self.store(StoreInstr::NextIndexId {
			ns,
			db,
			tb: tb_reg,
			dst: id,
			draw: 0,
			reserved: None,
		});
		self.set_field(definition, "index_id", Operand::Reg(id))?;
		self.put_index_definition((ns, db, tb), key, definition, name.operand)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::BuildIndex {
			table: table_def,
			index: definition,
			blocking: !concurrently,
			fresh: true,
		});
		self.answer_none()
	}

	/// Checks the columns in `list` against the catalog.
	fn check_columns(
		&mut self,
		(ns, db): (Reg, Reg),
		(tb, index): (Operand, Operand),
		list: Reg,
		schemafull: Reg,
	) -> Result<(), IrError> {
		let table = self.load(tb)?;
		let index = self.load(index)?;
		self.store(StoreInstr::CheckIndexColumns {
			ns,
			db,
			table,
			index,
			columns: list,
			schemafull,
		});
		Ok(())
	}

	/// The subscript the operand `slot` writes, when it writes a literal other than a string.
	fn literal_subscript(&self, slot: OperandSlot) -> Result<Option<Expr>, IrError> {
		Ok(self
			.literal(slot)?
			.map(|value| value.clone().into_literal())
			.filter(|written| matches!(written, Expr::Literal(_))))
	}

	/// The path `place` writes, with a placeholder at each subscript that is evaluated, and the
	/// slots of those subscripts in the order the path holds them. A written subscript is held by
	/// the path.
	fn place_path(&self, place: &Place) -> Result<(Idiom, Vec<OperandSlot>), IrError> {
		let mut parts = Vec::with_capacity(place.parts.len());
		let mut holes = Vec::new();
		for part in place.parts.iter() {
			parts.push(match part {
				PlacePart::Field(name) => Part::Field(name.clone()),
				PlacePart::All => Part::All,
				PlacePart::First => Part::First,
				PlacePart::Last => Part::Last,
				PlacePart::Flatten => Part::Flatten,
				PlacePart::Dynamic(slot) => match self.literal_subscript(*slot)? {
					Some(written) => Part::Value(written),
					None => {
						holes.push(*slot);
						Part::Value(Expr::Param(crate::expr::Param::from("hole".to_owned())))
					}
				},
				PlacePart::Optional => Part::Optional,
				PlacePart::Doc => Part::Doc,
				PlacePart::RepeatRecurse => Part::RepeatRecurse,
				PlacePart::Unaddressed(_, text) => kept_part(text)?,
				PlacePart::Filter(_) | PlacePart::Index(_) => {
					return Err(IrError::Stale("ir_lower::ddl: an index path step"));
				}
			});
		}
		Ok((Idiom(parts), holes))
	}

	/// The columns as the statement writes them, when each is a path whose subscripts are all
	/// written: they are then held by the definition's template, and the operands that hold a
	/// subscript are read as written.
	fn written_columns(&mut self, columns: &[IndexColumn]) -> Result<Option<Vec<Idiom>>, IrError> {
		let mut paths = Vec::with_capacity(columns.len());
		for column in columns {
			let IndexColumn::Path(place) = column else {
				return Ok(None);
			};
			let (path, holes) = self.place_path(place)?;
			if !holes.is_empty() {
				return Ok(None);
			}
			paths.push((place, path));
		}
		for (place, _) in &paths {
			for part in place.parts.iter() {
				if let PlacePart::Dynamic(slot) = part
					&& let Some(written) = self.literal(*slot)?
				{
					let operand = self.constant(written.clone())?;
					self.read_as(*slot, operand)?;
				}
			}
		}
		Ok(Some(paths.into_iter().map(|(_, path)| path).collect()))
	}

	/// Appends to the array in `list` the paths `column` names, evaluating what it holds in the
	/// statement's frame.
	fn resolve_column(&mut self, column: &IndexColumn, list: Reg) -> Result<(), IrError> {
		let source = match column {
			IndexColumn::Path(place) => {
				let (idiom, slots) = self.place_path(place)?;
				let mut holes = Vec::with_capacity(slots.len());
				for slot in &slots {
					holes.push(self.evaluated(*slot)?);
				}
				for part in place.parts.iter() {
					if let PlacePart::Dynamic(slot) = part
						&& !slots.contains(slot)
						&& let Some(written) = self.literal(*slot)?
					{
						let operand = self.constant(written.clone())?;
						self.read_as(*slot, operand)?;
					}
				}
				ColumnSource::Path(PathSource::Written {
					idiom,
					holes,
				})
			}
			IndexColumn::Stored(slot) => {
				let region = *self
					.regions
					.get(usize::from(*slot))
					.ok_or(IrError::Stale("ir_lower::ddl: region slot"))?;
				match self.stored_column(region)? {
					StoredColumn::TypeFields(value) => {
						ColumnSource::TypeFields(self.evaluated_in(region, value)?)
					}
					StoredColumn::TypeField(value) => {
						ColumnSource::TypeField(self.evaluated_in(region, value)?)
					}
					StoredColumn::Refused(many) => ColumnSource::Refused(
						if many {
							"Expected an argument for type::fields function call"
						} else {
							"Expected an argument for type::field function call"
						}
						.to_owned(),
					),
					StoredColumn::Evaluated(value) => {
						ColumnSource::Path(PathSource::Evaluated(self.evaluated_in(region, value)?))
					}
				}
			}
		};
		self.store(StoreInstr::ResolveColumns {
			source,
			list,
		});
		Ok(())
	}

	/// How the column held by `region` is resolved: by the argument of `type::fields` or
	/// `type::field`, or by the value of the whole expression.
	fn stored_column(&self, region: RegionId) -> Result<StoredColumn, IrError> {
		let ir = self.ir;
		let last = ops_in(ir, region)
			.last()
			.ok_or(IrError::Stale("ir_lower::ddl: an empty index column"))?;
		let terminator = ir.op(last).ok_or(IrError::Stale("ir_lower::ddl: op"))?;
		let [yielded] = terminator.operands.as_slice() else {
			return Err(IrError::Stale("ir_lower::ddl: an index column that yields no value"));
		};
		let defining = match ir.value(*yielded).map(|value| value.def) {
			Some(crate::exec::ir::core::value::ValueDef::OpResult {
				op,
				index: 0,
			}) => ir.op(op),
			Some(_) | None => None,
		};
		if let Some(call) = defining
			&& let OpKind::Expr(ExprOp::Call(function)) = &call.kind
			&& let Function::Normal(name) = &function.function
			&& (name == "type::fields" || name == "type::field")
		{
			let many = name == "type::fields";
			return Ok(match (call.operands.first(), many) {
				(Some(arg), true) => StoredColumn::TypeFields(*arg),
				(Some(arg), false) => StoredColumn::TypeField(*arg),
				(None, many) => StoredColumn::Refused(many),
			});
		}
		Ok(StoredColumn::Evaluated(*yielded))
	}

	/// A register holding `value`, evaluated in `region`'s ops in the statement's frame. A value
	/// a second evaluation could change is evaluated by the statement's run alone. The statement
	/// has no record in scope, so the row the region receives is `NONE`.
	fn evaluated_in(
		&mut self,
		region: RegionId,
		value: crate::exec::ir::iface::ids::ValueId,
	) -> Result<Reg, IrError> {
		if self.value_real_run_only(value)? {
			self.store(StoreInstr::RealRunOnly);
		}
		let none = self.constant(Value::None)?;
		let mut lowerer = Lowerer::in_context(&mut *self.em, self.ir, &mut *self.cx);
		for arg in lowerer.region_args(region)? {
			lowerer.bind_arg(arg, none);
		}
		let operand = lowerer.region_operand(region, value)?;
		let needs = lowerer.needs();
		self.needs = self.needs.merged(&needs);
		match operand {
			Operand::Reg(reg) => Ok(reg),
			other @ (Operand::Const(_) | Operand::Field(_)) => self.load(other),
		}
	}

	/// Checks that the field `fd` of the table, when it exists, is not computed: the emitted code
	/// jumps to the label it answers when the field exists and is not computed, and falls through
	/// when no field answers the check. With `parent`, a field that does not admit sub-fields
	/// settles nothing either.
	fn index_column_field(
		&mut self,
		(ns, db, tb): (Operand, Operand, Operand),
		fd: &str,
		shown: &str,
		index: Operand,
		parent: bool,
	) -> Result<Label, IrError> {
		let fd = self.constant(Value::String(fd.into()))?;
		let key = self.object(&[("ns", ns), ("db", db), ("tb", tb), ("fd", fd)])?;
		let field = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: field,
		});
		let past = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(field),
			truthy: false,
			target: past,
		});
		if parent {
			let admits = self.reg()?;
			self.store(StoreInstr::FieldAdmitsSubFields {
				field,
				dst: admits,
			});
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(admits),
				truthy: false,
				target: past,
			});
		}
		let computed = self.reg()?;
		self.get_field(field, "computed", computed)?;
		let plain = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(computed),
			truthy: false,
			target: plain,
		});
		let shown = self.constant(Value::String(shown.into()))?;
		let detail = self.object(&[("field", shown), ("index", index)])?;
		self.store(StoreInstr::Raise {
			error: StatementError::ComputedFieldCannotBeIndexed,
			arg: detail,
		});
		self.bind(plain)?;
		let passed = self.label()?;
		self.emit(Instr::Jump {
			target: passed,
		});
		self.bind(past)?;
		Ok(passed)
	}
}
