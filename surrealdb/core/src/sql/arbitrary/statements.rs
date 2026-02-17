use arbitrary::Arbitrary;

use crate::sql::access_type::{BearerAccess, BearerAccessSubject};
use crate::sql::arbitrary::{
	self, arb_group, arb_opt, arb_order, arb_splits, arb_vec1, atleast_one, insert_data,
};
use crate::sql::kind::KindLiteral;
use crate::sql::statements::SetStatement;
use crate::sql::statements::alter::{
	AlterDatabaseStatement, AlterIndexStatement, AlterKind, AlterNamespaceStatement,
	AlterSystemStatement,
};
use crate::sql::statements::define::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineUserStatement,
};
use crate::sql::{
	AccessType, Ast, Base, BinaryOperator, Data, DefineFieldStatement, DefineIndexStatement, Expr,
	Index, InsertStatement, KillStatement, Kind, Literal, Permission, Permissions, SelectStatement,
	TopLevelExpr, View,
};

impl<'a> Arbitrary<'a> for KillStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let id = match u.int_in_range(0u8..=1)? {
			0 => Expr::Param(u.arbitrary()?),
			1 => Expr::Literal(Literal::Uuid(u.arbitrary()?)),
			_ => unreachable!(),
		};

		Ok(KillStatement {
			id,
		})
	}
}

impl<'a> Arbitrary<'a> for DefineAccessStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let kind = u.arbitrary()?;
		let name = u.arbitrary()?;
		let access_type = u.arbitrary()?;
		let authenticate = u.arbitrary()?;
		let duration = crate::sql::access::AccessDuration {
			grant: u.arbitrary()?,
			token: u.arbitrary()?,
			session: u.arbitrary()?,
		};
		let comment = u.arbitrary()?;

		let base = if matches!(
			access_type,
			AccessType::Record(_)
				| AccessType::Bearer(BearerAccess {
					subject: BearerAccessSubject::Record,
					..
				})
		) {
			Base::Db
		} else {
			u.arbitrary()?
		};

		Ok(DefineAccessStatement {
			kind,
			name,
			base,
			access_type,
			authenticate,
			duration,
			comment,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineUserStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let kind = u.arbitrary()?;
		let name = u.arbitrary()?;
		let base = u.arbitrary()?;
		let pass_type = u.arbitrary()?;
		let comment = u.arbitrary()?;

		let mut roles = vec![match u.int_in_range(0u8..=2)? {
			0 => "viewer".to_string(),
			1 => "editor".to_string(),
			2 => "owner".to_string(),
			_ => unreachable!(),
		}];
		roles.reserve_exact(u.arbitrary_len::<u8>()?);
		for _ in 1..roles.capacity() {
			roles.push(match u.int_in_range(0u8..=2)? {
				0 => "viewer".to_string(),
				1 => "editor".to_string(),
				2 => "owner".to_string(),
				_ => unreachable!(),
			});
		}

		Ok(DefineUserStatement {
			kind,
			name,
			base,
			pass_type,
			token_duration: u.arbitrary()?,
			session_duration: u.arbitrary()?,
			roles,
			comment,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineIndexStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let kind = u.arbitrary()?;
		let name = u.arbitrary()?;
		let what = u.arbitrary()?;
		let index = u.arbitrary()?;
		let comment = u.arbitrary()?;
		let concurrently = u.arbitrary()?;

		let cols = match index {
			Index::Uniq | Index::Idx => {
				let mut cols = vec![u.arbitrary()?];
				cols.reserve_exact(u.arbitrary_len::<String>()?);
				for _ in 1..cols.capacity() {
					cols.push(u.arbitrary()?);
				}
				cols
			}
			Index::Hnsw(_) | Index::FullText(_) => vec![u.arbitrary()?],
			Index::Count(_) => Vec::new(),
		};

		Ok(DefineIndexStatement {
			kind,
			name,
			what,
			cols,
			index,
			comment,
			concurrently,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for InsertStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let into = match u.int_in_range(0u8..=2)? {
			0 => None,
			1 => Some(Expr::Param(u.arbitrary()?)),
			2 => Some(Expr::Table(u.arbitrary()?)),
			_ => unreachable!(),
		};

		let update = if u.arbitrary()? {
			Some(Data::UpdateExpression(atleast_one(u)?))
		} else {
			None
		};

		Ok(InsertStatement {
			into,
			data: insert_data(u)?,
			ignore: u.arbitrary()?,
			update,
			output: u.arbitrary()?,
			timeout: u.arbitrary()?,
			relation: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for SelectStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let mut fields = u.arbitrary()?;

		let group = if u.arbitrary()? {
			Some(arb_group(u, &mut fields)?)
		} else {
			None
		};

		let split = if u.arbitrary()? {
			Some(arb_splits(u, &mut fields)?)
		} else {
			None
		};

		let order = if u.arbitrary()? {
			Some(arb_order(u, &mut fields)?)
		} else {
			None
		};

		Ok(SelectStatement {
			fields,
			omit: u.arbitrary()?,
			only: u.arbitrary()?,
			what: arb_vec1(u, Expr::arbitrary)?,
			with: u.arbitrary()?,
			cond: u.arbitrary()?,
			split,
			group,
			order,
			limit: u.arbitrary()?,
			start: u.arbitrary()?,
			fetch: u.arbitrary()?,
			version: u.arbitrary()?,
			timeout: u.arbitrary()?,
			explain: u.arbitrary()?,
			tempfiles: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for View {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let mut expr = u.arbitrary()?;

		let group = if u.arbitrary()? {
			Some(arb_group(u, &mut expr)?)
		} else {
			None
		};

		Ok(View {
			expr,
			what: arb_vec1(u, |u| u.arbitrary())?,
			cond: u.arbitrary()?,
			group,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineAnalyzerStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(DefineAnalyzerStatement {
			kind: u.arbitrary()?,
			name: u.arbitrary()?,
			function: u.arbitrary()?,
			tokenizers: arb_opt(u, |u| arb_vec1(u, Arbitrary::arbitrary))?,
			filters: arb_opt(u, |u| arb_vec1(u, Arbitrary::arbitrary))?,
			comment: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineFieldStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let field_kind = u.arbitrary()?;

		fn contains_object(kind: &Kind) -> bool {
			match kind {
				Kind::Object => true,
				Kind::Either(kinds) => kinds.iter().any(contains_object),
				Kind::Array(inner, _) | Kind::Set(inner, _) => contains_object(inner),
				Kind::Literal(KindLiteral::Object(_)) => true,
				Kind::Literal(KindLiteral::Array(x)) => x.iter().any(contains_object),
				_ => false,
			}
		}

		let flexible = if let Some(kind) = &field_kind
			&& contains_object(kind)
		{
			u.arbitrary()?
		} else {
			false
		};

		let mut permissions: Permissions = u.arbitrary()?;
		permissions.delete = Permission::Full;

		Ok(DefineFieldStatement {
			kind: u.arbitrary()?,
			name: u.arbitrary()?,
			what: u.arbitrary()?,
			field_kind,
			flexible,
			readonly: u.arbitrary()?,
			value: u.arbitrary()?,
			assert: u.arbitrary()?,
			computed: u.arbitrary()?,
			default: u.arbitrary()?,
			permissions,
			comment: u.arbitrary()?,
			reference: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for AlterIndexStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		// Make sure there is atleast one modification.
		let comment = u.arbitrary()?;
		let prepare_remove = if let AlterKind::None = comment {
			true
		} else {
			u.arbitrary()?
		};
		Ok(AlterIndexStatement {
			name: u.arbitrary()?,
			table: u.arbitrary()?,
			if_exists: u.arbitrary()?,
			comment,
			prepare_remove,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for AlterSystemStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let query_timeout = match u.int_in_range(0u8..=1)? {
			0 => AlterKind::Drop,
			1 => AlterKind::Set(u.arbitrary()?),
			_ => unreachable!(),
		};

		Ok(AlterSystemStatement {
			query_timeout,
			compact: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for AlterDatabaseStatement {
	fn arbitrary(_: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(AlterDatabaseStatement {
			compact: true,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for AlterNamespaceStatement {
	fn arbitrary(_: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(AlterNamespaceStatement {
			compact: true,
		})
	}
}

impl<'a> Arbitrary<'a> for Ast {
	fn size_hint(depth: usize) -> (usize, Option<usize>) {
		Vec::<TopLevelExpr>::size_hint(depth)
	}

	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let mut expressions = Vec::<TopLevelExpr>::arbitrary(u)?;
		for e in expressions.iter_mut() {
			if let TopLevelExpr::Expr(Expr::Binary {
				left,
				op: BinaryOperator::Equal,
				right,
				..
			}) = e && let Expr::Param(ref left) = **left
			{
				*e = TopLevelExpr::Expr(Expr::Let(Box::new(SetStatement {
					name: left.clone().into_string(),
					kind: None,
					what: (**right).clone(),
				})))
			}
		}

		Ok(Ast {
			expressions,
		})
	}
}
