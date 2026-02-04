use arbitrary::Arbitrary;

use crate::sql::access_type::{BearerAccess, BearerAccessSubject};
use crate::sql::arbitrary::{
	self, arb_group, arb_opt, arb_order, arb_splits, arb_vec1, atleast_one, insert_data,
	local_idiom, plain_idiom,
};
use crate::sql::statements::access::AccessStatementPurge;
use crate::sql::statements::define::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineUserStatement,
};
use crate::sql::statements::{
	DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
	DefineIndexStatement, DefineNamespaceStatement, DefineParamStatement, DefineTableStatement,
	InsertStatement, KillStatement, SelectStatement, UseStatement,
};
use crate::sql::{
	AccessType, Base, Data, Ident, Idioms, Index, Kind, Literal, Operator, Permission, Permissions,
	Value, View,
};

impl<'a> Arbitrary<'a> for UseStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		match u.int_in_range(0u8..=2)? {
			0 => Ok(UseStatement {
				ns: Some(u.arbitrary()?),
				db: None,
			}),
			1 => Ok(UseStatement {
				ns: None,
				db: Some(u.arbitrary()?),
			}),
			2 => Ok(UseStatement {
				ns: Some(u.arbitrary()?),
				db: Some(u.arbitrary()?),
			}),
			_ => unreachable!(),
		}
	}
}

impl<'a> Arbitrary<'a> for KillStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let id = match u.int_in_range(0u8..=1)? {
			0 => Value::Param(u.arbitrary()?),
			1 => Value::Uuid(u.arbitrary()?),
			_ => unreachable!(),
		};

		Ok(KillStatement {
			id,
		})
	}
}

impl<'a> Arbitrary<'a> for DefineAccessStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
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
			if_not_exists,
			overwrite,
			kind: u.arbitrary()?,
			name,
			base,
			authenticate,
			duration,
			comment,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineUserStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let name = u.arbitrary()?;
		let base = u.arbitrary()?;
		let comment = u.arbitrary()?;
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;

		let mut roles = vec![match u.int_in_range(0u8..=2)? {
			0 => Ident("viewer".to_string()),
			1 => Ident("editor".to_string()),
			2 => Ident("owner".to_string()),
			_ => unreachable!(),
		}];
		roles.reserve_exact(u.arbitrary_len::<u8>()?);
		for _ in 1..roles.capacity() {
			roles.push(match u.int_in_range(0u8..=2)? {
				0 => Ident("viewer".to_string()),
				1 => Ident("editor".to_string()),
				2 => Ident("owner".to_string()),
				_ => unreachable!(),
			});
		}

		Ok(DefineUserStatement {
			if_not_exists,
			overwrite,
			name,
			base,
			roles,
			hash: u.arbitrary()?,
			code: u.arbitrary()?,
			duration: u.arbitrary()?,
			comment,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineIndexStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
		let name = u.arbitrary()?;
		let what = u.arbitrary()?;
		let index = u.arbitrary()?;
		let comment = u.arbitrary()?;
		let concurrently = u.arbitrary()?;

		let cols = match index {
			Index::Uniq | Index::Idx => arb_vec1(u, local_idiom)?,
			Index::Hnsw(_) | Index::Search(_) | Index::MTree(_) => vec![local_idiom(u)?],
			Index::Count => Vec::new(),
		};
		Ok(DefineIndexStatement {
			if_not_exists,
			overwrite,
			name,
			what,
			defer: u.arbitrary()?,
			cols: Idioms(cols),
			index,
			comment,
			concurrently,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineNamespaceStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;

		Ok(DefineNamespaceStatement {
			id: u.arbitrary()?,
			name: u.arbitrary()?,
			comment: u.arbitrary()?,
			if_not_exists,
			overwrite,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineDatabaseStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;

		Ok(DefineDatabaseStatement {
			id: u.arbitrary()?,
			name: u.arbitrary()?,
			comment: u.arbitrary()?,
			changefeed: u.arbitrary()?,
			if_not_exists,
			overwrite,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineTableStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
		Ok(DefineTableStatement {
			id: u.arbitrary()?,
			name: u.arbitrary()?,
			drop: u.arbitrary()?,
			full: u.arbitrary()?,
			view: u.arbitrary()?,
			permissions: u.arbitrary()?,
			changefeed: u.arbitrary()?,
			comment: u.arbitrary()?,
			if_not_exists,
			kind: u.arbitrary()?,
			overwrite,
			cache_fields_ts: u.arbitrary()?,
			cache_events_ts: u.arbitrary()?,
			cache_tables_ts: u.arbitrary()?,
			cache_indexes_ts: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineFunctionStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
		Ok(DefineFunctionStatement {
			name: u.arbitrary()?,
			args: u.arbitrary()?,
			block: u.arbitrary()?,
			comment: u.arbitrary()?,
			permissions: u.arbitrary()?,
			if_not_exists,
			overwrite,
			returns: u.arbitrary()?,
			auth_limit: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineParamStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
		Ok(DefineParamStatement {
			name: u.arbitrary()?,
			value: u.arbitrary()?,
			comment: u.arbitrary()?,
			permissions: u.arbitrary()?,
			if_not_exists,
			overwrite,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineEventStatement {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;
		Ok(DefineEventStatement {
			name: u.arbitrary()?,
			what: u.arbitrary()?,
			when: u.arbitrary()?,
			then: u.arbitrary()?,
			comment: u.arbitrary()?,
			if_not_exists,
			overwrite,
			auth_limit: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for InsertStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let into = match u.int_in_range(0u8..=2)? {
			0 => None,
			1 => Some(Value::Param(u.arbitrary()?)),
			2 => Some(Value::Table(u.arbitrary()?)),
			_ => unreachable!(),
		};

		let update = if u.arbitrary()? {
			let data = arb_vec1(u, |u| {
				let op = match u.int_in_range(0u8..=3)? {
					0 => Operator::Equal,
					1 => Operator::Inc,
					2 => Operator::Dec,
					3 => Operator::Ext,
					_ => unreachable!(),
				};

				Ok((plain_idiom(u)?, op, u.arbitrary()?))
			})?;

			Some(Data::UpdateExpression(data))
		} else {
			None
		};

		Ok(InsertStatement {
			into,
			data: insert_data(u)?,
			ignore: u.arbitrary()?,
			parallel: u.arbitrary()?,
			update,
			output: u.arbitrary()?,
			timeout: u.arbitrary()?,
			relation: u.arbitrary()?,
			version: u.arbitrary()?,
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
			expr: fields,
			omit: u.arbitrary()?,
			only: u.arbitrary()?,
			what: u.arbitrary()?,
			with: u.arbitrary()?,
			cond: u.arbitrary()?,
			parallel: u.arbitrary()?,
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
			what: u.arbitrary()?,
			cond: u.arbitrary()?,
			group,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for DefineAnalyzerStatement {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;

		Ok(DefineAnalyzerStatement {
			if_not_exists,
			overwrite,
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
		let kind = u.arbitrary()?;
		let if_not_exists: bool = u.arbitrary()?;
		let overwrite = !if_not_exists && u.arbitrary()?;

		let mut permissions: Permissions = u.arbitrary()?;
		permissions.delete = Permission::Full;

		Ok(DefineFieldStatement {
			if_not_exists,
			overwrite,
			name: local_idiom(u)?,
			what: u.arbitrary()?,
			kind,
			flex: u.arbitrary()?,
			default_always: u.arbitrary()?,
			auth_limit: u.arbitrary()?,
			readonly: u.arbitrary()?,
			value: u.arbitrary()?,
			assert: u.arbitrary()?,
			default: u.arbitrary()?,
			permissions,
			comment: u.arbitrary()?,
			reference: u.arbitrary()?,
		})
	}
}

impl<'a> arbitrary::Arbitrary<'a> for AccessStatementPurge {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let (expired, revoked) = *u.choose(&[(true, false), (false, true), (true, true)])?;

		Ok(AccessStatementPurge {
			ac: u.arbitrary()?,
			base: u.arbitrary()?,
			expired,
			revoked,
			grace: u.arbitrary()?,
		})
	}
}
