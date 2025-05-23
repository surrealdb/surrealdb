//! Tests to check that the SQL and Expr structs are equivalent.

#![allow(non_snake_case)]

use crate::{expr, sql};
use revision::Revisioned;
use std::ops::Bound;

macro_rules! test_revisioned_equality {
	($test:ident, $sql_ty:ty, $expr_ty:ty, $sql_example:expr) => {
		#[test]
		fn $test() {
			{
				let size_of_sql = std::mem::size_of::<$sql_ty>();
				let size_of_expr = std::mem::size_of::<$expr_ty>();
				assert_eq!(size_of_sql, size_of_expr, "Size of SQL and Expr structs do not match");

				let align_of_sql = std::mem::align_of::<$sql_ty>();
				let align_of_expr = std::mem::align_of::<$expr_ty>();
				assert_eq!(
					align_of_sql, align_of_expr,
					"Alignment of SQL and Expr structs do not match"
				);
			}

			{
				let sql_instance = $sql_example;
				let expr_instance: $expr_ty = sql_instance.clone().into();

				assert_eq!(<$sql_ty>::revision(), <$expr_ty>::revision(), "Revisions do not match");

				let sql_revisioned_bytes = revision::to_vec(&sql_instance).unwrap();
				let expr_revisioned_bytes = revision::to_vec(&expr_instance).unwrap();

				assert_eq!(
					sql_revisioned_bytes, expr_revisioned_bytes,
					"Serialized bytes do not match"
				);
				let sql_from_sql_bytes: $sql_ty =
					revision::from_slice(&sql_revisioned_bytes).unwrap();
				let sql_from_expr_bytes: $sql_ty =
					revision::from_slice(&expr_revisioned_bytes).unwrap();
				let expr_from_sql_bytes: $expr_ty =
					revision::from_slice(&sql_revisioned_bytes).unwrap();
				let expr_from_expr_bytes: $expr_ty =
					revision::from_slice(&expr_revisioned_bytes).unwrap();

				assert_eq!(
					sql_instance, sql_from_sql_bytes,
					"Deserialized SQL instance from SQL bytes does not match original"
				);
				assert_eq!(
					sql_instance, sql_from_expr_bytes,
					"Deserialized SQL instance from Expr bytes does not match original"
				);
				assert_eq!(
					expr_instance, expr_from_sql_bytes,
					"Deserialized Expr instance from SQL bytes does not match original"
				);
				assert_eq!(
					expr_instance, expr_from_expr_bytes,
					"Deserialized Expr instance from Expr bytes does not match original"
				);
			}
		}
	};
}

test_revisioned_equality!(sql_Id_Number, sql::Id, expr::Id, sql::Id::Number(5));
test_revisioned_equality!(sql_Id_String, sql::Id, expr::Id, sql::Id::String("test".to_string()));
test_revisioned_equality!(sql_Id_Uuid, sql::Id, expr::Id, sql::Id::Uuid(sql::Uuid::new_v4()));
test_revisioned_equality!(
	sql_Id_Array,
	sql::Id,
	expr::Id,
	sql::Id::Array(sql::Array(vec![1.into(), 2.into(), 3.into()]))
);
test_revisioned_equality!(
	sql_Id_Object,
	sql::Id,
	expr::Id,
	sql::Id::Object([("key".to_string(), sql::SqlValue::from(1))].into_iter().collect())
);
test_revisioned_equality!(
	sql_Id_Generate,
	sql::Id,
	expr::Id,
	sql::Id::Generate(sql::id::Gen::Ulid)
);
test_revisioned_equality!(
	sql_Id_Range,
	sql::Id,
	expr::Id,
	sql::Id::Range(Box::new(sql::id::range::IdRange {
		beg: Bound::Included(1.into()),
		end: Bound::Excluded(2.into())
	}))
);

test_revisioned_equality!(
	sql_IdRange,
	sql::IdRange,
	expr::IdRange,
	sql::id::range::IdRange {
		beg: Bound::Included(1.into()),
		end: Bound::Excluded(2.into())
	}
);

test_revisioned_equality!(
	sql_statements_alter_AlterFieldStatement,
	sql::statements::alter::AlterFieldStatement,
	expr::statements::alter::AlterFieldStatement,
	sql::statements::alter::AlterFieldStatement::default()
);
test_revisioned_equality!(
	sql_statements_alter_AlterSequenceStatement,
	sql::statements::alter::AlterSequenceStatement,
	expr::statements::alter::AlterSequenceStatement,
	sql::statements::alter::AlterSequenceStatement::default()
);
test_revisioned_equality!(
	sql_statements_alter_AlterTableStatement,
	sql::statements::alter::AlterTableStatement,
	expr::statements::alter::AlterTableStatement,
	sql::statements::alter::AlterTableStatement::default()
);
test_revisioned_equality!(
	sql_statements_AlterStatement,
	sql::statements::AlterStatement,
	expr::statements::AlterStatement,
	sql::statements::AlterStatement::Field(sql::statements::alter::AlterFieldStatement::default())
);

test_revisioned_equality!(
	sql_statements_define_config_api_ApiConfig,
	sql::statements::define::config::api::ApiConfig,
	expr::statements::define::config::api::ApiConfig,
	sql::statements::define::config::api::ApiConfig::default()
);
test_revisioned_equality!(
	sql_statements_define_config_graphql_GraphQLConfig,
	sql::statements::define::config::graphql::GraphQLConfig,
	expr::statements::define::config::graphql::GraphQLConfig,
	sql::statements::define::config::graphql::GraphQLConfig::default()
);
test_revisioned_equality!(
	sql_statements_define_config_graphql_TablesConfig,
	sql::statements::define::config::graphql::TablesConfig,
	expr::statements::define::config::graphql::TablesConfig,
	sql::statements::define::config::graphql::TablesConfig::default()
);
test_revisioned_equality!(
	sql_statements_define_config_graphql_TableConfig,
	sql::statements::define::config::graphql::TableConfig,
	expr::statements::define::config::graphql::TableConfig,
	sql::statements::define::config::graphql::TableConfig::default()
);
test_revisioned_equality!(
	sql_statements_define_config_graphql_FunctionsConfig,
	sql::statements::define::config::graphql::FunctionsConfig,
	expr::statements::define::config::graphql::FunctionsConfig,
	sql::statements::define::config::graphql::FunctionsConfig::default()
);
test_revisioned_equality!(
	sql_statements_define_DefineConfigStatement,
	sql::statements::define::DefineConfigStatement,
	expr::statements::define::DefineConfigStatement,
	sql::statements::define::DefineConfigStatement {
		inner: sql::statements::define::config::ConfigInner::Api(
			sql::statements::define::config::api::ApiConfig::default()
		),
		if_not_exists: false,
		overwrite: false,
	}
);
test_revisioned_equality!(
	sql_statements_define_config_ConfigInner,
	sql::statements::define::config::ConfigInner,
	expr::statements::define::config::ConfigInner,
	sql::statements::define::config::ConfigInner::Api(
		sql::statements::define::config::api::ApiConfig::default()
	)
);
test_revisioned_equality!(
	sql_statements_DefineAccessStatement,
	sql::statements::DefineAccessStatement,
	expr::statements::DefineAccessStatement,
	sql::statements::DefineAccessStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineAnalyzerStatement,
	sql::statements::DefineAnalyzerStatement,
	expr::statements::DefineAnalyzerStatement,
	sql::statements::DefineAnalyzerStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineApiStatement,
	sql::statements::DefineApiStatement,
	expr::statements::DefineApiStatement,
	sql::statements::DefineApiStatement::default()
);
test_revisioned_equality!(
	sql_statements_define_ApiAction,
	sql::statements::define::ApiAction,
	expr::statements::define::ApiAction,
	sql::statements::define::ApiAction::default()
);
test_revisioned_equality!(
	sql_statements_define_DefineBucketStatement,
	sql::statements::define::DefineBucketStatement,
	expr::statements::define::DefineBucketStatement,
	sql::statements::define::DefineBucketStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineDatabaseStatement,
	sql::statements::DefineDatabaseStatement,
	expr::statements::DefineDatabaseStatement,
	sql::statements::DefineDatabaseStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineEventStatement,
	sql::statements::DefineEventStatement,
	expr::statements::DefineEventStatement,
	sql::statements::DefineEventStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineFieldStatement,
	sql::statements::DefineFieldStatement,
	expr::statements::DefineFieldStatement,
	sql::statements::DefineFieldStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineFunctionStatement,
	sql::statements::DefineFunctionStatement,
	expr::statements::DefineFunctionStatement,
	sql::statements::DefineFunctionStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineIndexStatement,
	sql::statements::DefineIndexStatement,
	expr::statements::DefineIndexStatement,
	sql::statements::DefineIndexStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineStatement,
	sql::statements::DefineStatement,
	expr::statements::DefineStatement,
	sql::statements::DefineStatement::Namespace(
		sql::statements::DefineNamespaceStatement::default()
	)
);
test_revisioned_equality!(
	sql_statements_DefineModelStatement,
	sql::statements::DefineModelStatement,
	expr::statements::DefineModelStatement,
	sql::statements::DefineModelStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineNamespaceStatement,
	sql::statements::DefineNamespaceStatement,
	expr::statements::DefineNamespaceStatement,
	sql::statements::DefineNamespaceStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineParamStatement,
	sql::statements::DefineParamStatement,
	expr::statements::DefineParamStatement,
	sql::statements::DefineParamStatement::default()
);
test_revisioned_equality!(
	sql_statements_define_DefineSequenceStatement,
	sql::statements::define::DefineSequenceStatement,
	expr::statements::define::DefineSequenceStatement,
	sql::statements::define::DefineSequenceStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineTableStatement,
	sql::statements::DefineTableStatement,
	expr::statements::DefineTableStatement,
	sql::statements::DefineTableStatement::default()
);
test_revisioned_equality!(
	sql_statements_DefineUserStatement,
	sql::statements::DefineUserStatement,
	expr::statements::DefineUserStatement,
	sql::statements::DefineUserStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveAccessStatement,
	sql::statements::RemoveAccessStatement,
	expr::statements::RemoveAccessStatement,
	sql::statements::RemoveAccessStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveAnalyzerStatement,
	sql::statements::RemoveAnalyzerStatement,
	expr::statements::RemoveAnalyzerStatement,
	sql::statements::RemoveAnalyzerStatement::default()
);
test_revisioned_equality!(
	sql_statements_remove_RemoveBucketStatement,
	sql::statements::remove::RemoveBucketStatement,
	expr::statements::remove::RemoveBucketStatement,
	sql::statements::remove::RemoveBucketStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveDatabaseStatement,
	sql::statements::RemoveDatabaseStatement,
	expr::statements::RemoveDatabaseStatement,
	sql::statements::RemoveDatabaseStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveEventStatement,
	sql::statements::RemoveEventStatement,
	expr::statements::RemoveEventStatement,
	sql::statements::RemoveEventStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveFieldStatement,
	sql::statements::RemoveFieldStatement,
	expr::statements::RemoveFieldStatement,
	sql::statements::RemoveFieldStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveFunctionStatement,
	sql::statements::RemoveFunctionStatement,
	expr::statements::RemoveFunctionStatement,
	sql::statements::RemoveFunctionStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveIndexStatement,
	sql::statements::RemoveIndexStatement,
	expr::statements::RemoveIndexStatement,
	sql::statements::RemoveIndexStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveStatement,
	sql::statements::RemoveStatement,
	expr::statements::RemoveStatement,
	sql::statements::RemoveStatement::Namespace(
		sql::statements::RemoveNamespaceStatement::default()
	)
);
test_revisioned_equality!(
	sql_statements_RemoveModelStatement,
	sql::statements::RemoveModelStatement,
	expr::statements::RemoveModelStatement,
	sql::statements::RemoveModelStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveNamespaceStatement,
	sql::statements::RemoveNamespaceStatement,
	expr::statements::RemoveNamespaceStatement,
	sql::statements::RemoveNamespaceStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveParamStatement,
	sql::statements::RemoveParamStatement,
	expr::statements::RemoveParamStatement,
	sql::statements::RemoveParamStatement::default()
);
test_revisioned_equality!(
	sql_statements_remove_RemoveSequenceStatement,
	sql::statements::remove::RemoveSequenceStatement,
	expr::statements::remove::RemoveSequenceStatement,
	sql::statements::remove::RemoveSequenceStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveTableStatement,
	sql::statements::RemoveTableStatement,
	expr::statements::RemoveTableStatement,
	sql::statements::RemoveTableStatement::default()
);
test_revisioned_equality!(
	sql_statements_RemoveUserStatement,
	sql::statements::RemoveUserStatement,
	expr::statements::RemoveUserStatement,
	sql::statements::RemoveUserStatement::default()
);
test_revisioned_equality!(
	sql_statements_access_AccessStatement,
	sql::statements::access::AccessStatement,
	expr::statements::access::AccessStatement,
	sql::statements::access::AccessStatement::Show(
		sql::statements::access::AccessStatementShow::default()
	)
);
test_revisioned_equality!(
	sql_statements_access_AccessStatementGrant,
	sql::statements::access::AccessStatementGrant,
	expr::statements::access::AccessStatementGrant,
	sql::statements::access::AccessStatementGrant {
		ac: sql::Ident::default(),
		base: Some(sql::Base::default()),
		subject: sql::statements::access::Subject::Record(sql::Thing {
			tb: "table".to_string(),
			id: sql::Id::Number(1),
		}),
	}
);
test_revisioned_equality!(
	sql_statements_access_AccessStatementShow,
	sql::statements::access::AccessStatementShow,
	expr::statements::access::AccessStatementShow,
	sql::statements::access::AccessStatementShow::default()
);
test_revisioned_equality!(
	sql_statements_access_AccessStatementRevoke,
	sql::statements::access::AccessStatementRevoke,
	expr::statements::access::AccessStatementRevoke,
	sql::statements::access::AccessStatementRevoke::default()
);
test_revisioned_equality!(
	sql_statements_access_AccessStatementPurge,
	sql::statements::access::AccessStatementPurge,
	expr::statements::access::AccessStatementPurge,
	sql::statements::access::AccessStatementPurge::default()
);
test_revisioned_equality!(
	sql_statements_access_Subject,
	sql::statements::access::Subject,
	expr::statements::access::Subject,
	sql::statements::access::Subject::Record(sql::Thing {
		tb: "table".to_string(),
		id: sql::Id::Number(1),
	})
);
test_revisioned_equality!(
	sql_statements_analyze_AnalyzeStatement,
	sql::statements::analyze::AnalyzeStatement,
	expr::statements::analyze::AnalyzeStatement,
	sql::statements::analyze::AnalyzeStatement::Idx(sql::Ident::default(), sql::Ident::default())
);
test_revisioned_equality!(
	sql_statements_begin_BeginStatement,
	sql::statements::begin::BeginStatement,
	expr::statements::begin::BeginStatement,
	sql::statements::begin::BeginStatement::default()
);
test_revisioned_equality!(
	sql_statements_BreakStatement,
	sql::statements::BreakStatement,
	expr::statements::BreakStatement,
	sql::statements::BreakStatement::default()
);
test_revisioned_equality!(
	sql_statements_cancel_CancelStatement,
	sql::statements::cancel::CancelStatement,
	expr::statements::cancel::CancelStatement,
	sql::statements::cancel::CancelStatement::default()
);
test_revisioned_equality!(
	sql_statements_commit_CommitStatement,
	sql::statements::commit::CommitStatement,
	expr::statements::commit::CommitStatement,
	sql::statements::commit::CommitStatement::default()
);
test_revisioned_equality!(
	sql_statements_ContinueStatement,
	sql::statements::ContinueStatement,
	expr::statements::ContinueStatement,
	sql::statements::ContinueStatement::default()
);
test_revisioned_equality!(
	sql_statements_CreateStatement,
	sql::statements::CreateStatement,
	expr::statements::CreateStatement,
	sql::statements::CreateStatement::default()
);
test_revisioned_equality!(
	sql_statements_DeleteStatement,
	sql::statements::DeleteStatement,
	expr::statements::DeleteStatement,
	sql::statements::DeleteStatement::default()
);
test_revisioned_equality!(
	sql_statements_ForeachStatement,
	sql::statements::ForeachStatement,
	expr::statements::ForeachStatement,
	sql::statements::ForeachStatement::default()
);
test_revisioned_equality!(
	sql_statements_IfelseStatement,
	sql::statements::IfelseStatement,
	expr::statements::IfelseStatement,
	sql::statements::IfelseStatement::default()
);
test_revisioned_equality!(
	sql_statements_InfoStatement,
	sql::statements::InfoStatement,
	expr::statements::InfoStatement,
	sql::statements::InfoStatement::Root(true)
);
test_revisioned_equality!(
	sql_statements_InsertStatement,
	sql::statements::InsertStatement,
	expr::statements::InsertStatement,
	sql::statements::InsertStatement::default()
);
test_revisioned_equality!(
	sql_statements_KillStatement,
	sql::statements::KillStatement,
	expr::statements::KillStatement,
	sql::statements::KillStatement::default()
);
test_revisioned_equality!(
	sql_statements_LiveStatement,
	sql::statements::LiveStatement,
	expr::statements::LiveStatement,
	sql::statements::LiveStatement::default()
);
test_revisioned_equality!(
	sql_statements_OptionStatement,
	sql::statements::OptionStatement,
	expr::statements::OptionStatement,
	sql::statements::OptionStatement::default()
);
test_revisioned_equality!(
	sql_statements_OutputStatement,
	sql::statements::OutputStatement,
	expr::statements::OutputStatement,
	sql::statements::OutputStatement::default()
);
test_revisioned_equality!(
	sql_statements_rebuild_RebuildStatement,
	sql::statements::rebuild::RebuildStatement,
	expr::statements::rebuild::RebuildStatement,
	sql::statements::rebuild::RebuildStatement::Index(
		sql::statements::rebuild::RebuildIndexStatement::default()
	)
);
test_revisioned_equality!(
	sql_statements_rebuild_RebuildIndexStatement,
	sql::statements::rebuild::RebuildIndexStatement,
	expr::statements::rebuild::RebuildIndexStatement,
	sql::statements::rebuild::RebuildIndexStatement::default()
);
test_revisioned_equality!(
	sql_statements_RelateStatement,
	sql::statements::RelateStatement,
	expr::statements::RelateStatement,
	sql::statements::RelateStatement::default()
);
test_revisioned_equality!(
	sql_statements_SelectStatement,
	sql::statements::SelectStatement,
	expr::statements::SelectStatement,
	sql::statements::SelectStatement::default()
);
test_revisioned_equality!(
	sql_statements_SetStatement,
	sql::statements::SetStatement,
	expr::statements::SetStatement,
	sql::statements::SetStatement::default()
);
test_revisioned_equality!(
	sql_statements_show_ShowSince,
	sql::statements::show::ShowSince,
	expr::statements::show::ShowSince,
	sql::statements::show::ShowSince::Timestamp(sql::Datetime::default())
);
test_revisioned_equality!(
	sql_statements_ShowStatement,
	sql::statements::ShowStatement,
	expr::statements::ShowStatement,
	sql::statements::ShowStatement {
		table: Some(sql::Table::default()),
		since: sql::statements::show::ShowSince::Timestamp(sql::Datetime::default()),
		limit: Some(1),
	}
);
test_revisioned_equality!(
	sql_statements_SleepStatement,
	sql::statements::SleepStatement,
	expr::statements::SleepStatement,
	sql::statements::SleepStatement::default()
);
test_revisioned_equality!(
	sql_statements_ThrowStatement,
	sql::statements::ThrowStatement,
	expr::statements::ThrowStatement,
	sql::statements::ThrowStatement::default()
);
test_revisioned_equality!(
	sql_statements_UpdateStatement,
	sql::statements::UpdateStatement,
	expr::statements::UpdateStatement,
	sql::statements::UpdateStatement::default()
);
test_revisioned_equality!(
	sql_statements_UpsertStatement,
	sql::statements::UpsertStatement,
	expr::statements::UpsertStatement,
	sql::statements::UpsertStatement::default()
);
test_revisioned_equality!(
	sql_statements_UseStatement,
	sql::statements::UseStatement,
	expr::statements::UseStatement,
	sql::statements::UseStatement::default()
);
test_revisioned_equality!(sql_SqlValues, sql::SqlValues, expr::Values, sql::SqlValues::default());
test_revisioned_equality!(sql_SqlValue, sql::SqlValue, expr::Value, sql::SqlValue::default());
test_revisioned_equality!(
	sql_AccessType,
	sql::AccessType,
	expr::AccessType,
	sql::AccessType::default()
);
test_revisioned_equality!(
	sql_JwtAccess,
	sql::JwtAccess,
	expr::JwtAccess,
	sql::JwtAccess::default()
);
test_revisioned_equality!(
	sql_access_type_JwtAccessIssue,
	sql::access_type::JwtAccessIssue,
	expr::access_type::JwtAccessIssue,
	sql::access_type::JwtAccessIssue::default()
);
test_revisioned_equality!(
	sql_access_type_JwtAccessVerify,
	sql::access_type::JwtAccessVerify,
	expr::access_type::JwtAccessVerify,
	sql::access_type::JwtAccessVerify::default()
);
test_revisioned_equality!(
	sql_access_type_JwtAccessVerifyKey,
	sql::access_type::JwtAccessVerifyKey,
	expr::access_type::JwtAccessVerifyKey,
	sql::access_type::JwtAccessVerifyKey::default()
);
test_revisioned_equality!(
	sql_access_type_JwtAccessVerifyJwks,
	sql::access_type::JwtAccessVerifyJwks,
	expr::access_type::JwtAccessVerifyJwks,
	sql::access_type::JwtAccessVerifyJwks::default()
);
test_revisioned_equality!(
	sql_RecordAccess,
	sql::RecordAccess,
	expr::RecordAccess,
	sql::RecordAccess::default()
);
test_revisioned_equality!(
	sql_access_type_BearerAccess,
	sql::access_type::BearerAccess,
	expr::access_type::BearerAccess,
	sql::access_type::BearerAccess::default()
);
test_revisioned_equality!(
	sql_access_type_BearerAccessType,
	sql::access_type::BearerAccessType,
	expr::access_type::BearerAccessType,
	sql::access_type::BearerAccessType::Bearer
);
test_revisioned_equality!(
	sql_access_type_BearerAccessSubject,
	sql::access_type::BearerAccessSubject,
	expr::access_type::BearerAccessSubject,
	sql::access_type::BearerAccessSubject::Record
);
test_revisioned_equality!(
	sql_access_AccessDuration,
	sql::access::AccessDuration,
	expr::access::AccessDuration,
	sql::access::AccessDuration::default()
);
test_revisioned_equality!(
	sql_Algorithm,
	sql::Algorithm,
	expr::Algorithm,
	sql::Algorithm::default()
);
test_revisioned_equality!(sql_Array, sql::Array, expr::Array, sql::Array::default());
test_revisioned_equality!(sql_Base, sql::Base, expr::Base, sql::Base::default());
test_revisioned_equality!(sql_Block, sql::Block, expr::Block, sql::Block::default());
test_revisioned_equality!(
	sql_Entry,
	sql::Entry,
	expr::Entry,
	sql::Entry::Value(sql::SqlValue::default())
);
test_revisioned_equality!(sql_Bytes, sql::Bytes, expr::Bytes, sql::Bytes::default());
test_revisioned_equality!(
	sql_Cast,
	sql::Cast,
	expr::Cast,
	sql::Cast(sql::Kind::default(), sql::SqlValue::default())
);
test_revisioned_equality!(
	sql_ChangeFeed,
	sql::ChangeFeed,
	expr::ChangeFeed,
	sql::ChangeFeed::default()
);
test_revisioned_equality!(
	sql_Closure,
	sql::Closure,
	expr::Closure,
	sql::Closure {
		args: vec![(sql::Ident::default(), sql::Kind::default())],
		returns: Some(sql::Kind::default()),
		body: sql::SqlValue::default(),
	}
);
test_revisioned_equality!(sql_Cond, sql::Cond, expr::Cond, sql::Cond::default());
test_revisioned_equality!(sql_Constant, sql::Constant, expr::Constant, sql::Constant::MathE);
test_revisioned_equality!(sql_Data, sql::Data, expr::Data, sql::Data::default());
test_revisioned_equality!(sql_Datetime, sql::Datetime, expr::Datetime, sql::Datetime::default());
test_revisioned_equality!(sql_Dir, sql::Dir, expr::Dir, sql::Dir::default());
test_revisioned_equality!(sql_Duration, sql::Duration, expr::Duration, sql::Duration::default());
test_revisioned_equality!(
	sql_Edges,
	sql::Edges,
	expr::Edges,
	sql::Edges {
		dir: sql::Dir::default(),
		from: sql::Thing {
			tb: "table".to_string(),
			id: sql::Id::Number(1),
		},
		what: sql::graph::GraphSubjects::default(),
	}
);
test_revisioned_equality!(sql_Explain, sql::Explain, expr::Explain, sql::Explain::default());
test_revisioned_equality!(
	sql_Expression,
	sql::Expression,
	expr::Expression,
	sql::Expression::default()
);
test_revisioned_equality!(sql_Fetchs, sql::Fetchs, expr::Fetchs, sql::Fetchs::default());
test_revisioned_equality!(sql_Fetch, sql::Fetch, expr::Fetch, sql::Fetch::default());
test_revisioned_equality!(
	sql_field_Fields,
	sql::field::Fields,
	expr::field::Fields,
	sql::field::Fields::default()
);
test_revisioned_equality!(
	sql_field_Field,
	sql::field::Field,
	expr::field::Field,
	sql::field::Field::default()
);
test_revisioned_equality!(
	sql_File,
	sql::File,
	expr::File,
	sql::File {
		bucket: "bucket".to_string(),
		key: "key".to_string(),
	}
);
test_revisioned_equality!(sql_Filter, sql::Filter, expr::Filter, sql::Filter::Ascii);
test_revisioned_equality!(
	sql_Function,
	sql::Function,
	expr::Function,
	sql::Function::Normal("test".to_string(), vec![sql::SqlValue::default()])
);
test_revisioned_equality!(
	sql_Future,
	sql::Future,
	expr::Future,
	sql::Future(sql::Block::default())
);
test_revisioned_equality!(
	sql_Geometry,
	sql::Geometry,
	expr::Geometry,
	sql::Geometry::Point(geo::Point::default())
);
test_revisioned_equality!(sql_Graph, sql::Graph, expr::Graph, sql::Graph::default());
test_revisioned_equality!(
	sql_graph_GraphSubjects,
	sql::graph::GraphSubjects,
	expr::graph::GraphSubjects,
	sql::graph::GraphSubjects::default()
);
test_revisioned_equality!(
	sql_graph_GraphSubject,
	sql::graph::GraphSubject,
	expr::graph::GraphSubject,
	sql::graph::GraphSubject::Table(sql::Table::default())
);
test_revisioned_equality!(sql_Groups, sql::Groups, expr::Groups, sql::Groups::default());
test_revisioned_equality!(sql_Group, sql::Group, expr::Group, sql::Group::default());
test_revisioned_equality!(sql_Ident, sql::Ident, expr::Ident, sql::Ident::default());
test_revisioned_equality!(sql_Idioms, sql::Idioms, expr::Idioms, sql::Idioms::default());
test_revisioned_equality!(sql_Idiom, sql::Idiom, expr::Idiom, sql::Idiom::default());
test_revisioned_equality!(
	sql_index_Index,
	sql::index::Index,
	expr::index::Index,
	sql::index::Index::default()
);
test_revisioned_equality!(
	sql_index_SearchParams,
	sql::index::SearchParams,
	expr::index::SearchParams,
	sql::index::SearchParams {
		az: sql::Ident::default(),
		hl: true,
		sc: sql::scoring::Scoring::default(),
		doc_ids_order: 0,
		doc_lengths_order: 1,
		postings_order: 2,
		terms_order: 3,
		doc_ids_cache: 4,
		doc_lengths_cache: 5,
		postings_cache: 6,
		terms_cache: 7,
	}
);
test_revisioned_equality!(
	sql_index_MTreeParams,
	sql::index::MTreeParams,
	expr::index::MTreeParams,
	sql::index::MTreeParams {
		dimension: 0,
		distance: sql::index::Distance::default(),
		vector_type: sql::index::VectorType::default(),
		capacity: 1,
		doc_ids_order: 2,
		doc_ids_cache: 3,
		mtree_cache: 4,
	}
);
test_revisioned_equality!(
	sql_index_HnswParams,
	sql::index::HnswParams,
	expr::index::HnswParams,
	sql::index::HnswParams {
		dimension: 0,
		distance: sql::index::Distance::default(),
		vector_type: sql::index::VectorType::default(),
		m: 1,
		m0: 2,
		ef_construction: 3,
		extend_candidates: true,
		keep_pruned_connections: false,
		ml: sql::Number::default(),
	}
);
test_revisioned_equality!(
	sql_index_Distance,
	sql::index::Distance,
	expr::index::Distance,
	sql::index::Distance::default()
);
test_revisioned_equality!(
	sql_index_VectorType,
	sql::index::VectorType,
	expr::index::VectorType,
	sql::index::VectorType::default()
);
test_revisioned_equality!(sql_Kind, sql::Kind, expr::Kind, sql::Kind::default());
test_revisioned_equality!(
	sql_Literal,
	sql::Literal,
	expr::Literal,
	sql::Literal::Number(sql::Number::default())
);
test_revisioned_equality!(
	sql_language_Language,
	sql::language::Language,
	expr::language::Language,
	sql::language::Language::English
);
test_revisioned_equality!(sql_Limit, sql::Limit, expr::Limit, sql::Limit::default());
test_revisioned_equality!(sql_Mock, sql::Mock, expr::Mock, sql::Mock::Count("test".to_string(), 1));
test_revisioned_equality!(sql_Model, sql::Model, expr::Model, sql::Model::default());
test_revisioned_equality!(sql_Number, sql::Number, expr::Number, sql::Number::default());
test_revisioned_equality!(sql_Object, sql::Object, expr::Object, sql::Object::default());
test_revisioned_equality!(sql_Operator, sql::Operator, expr::Operator, sql::Operator::default());
test_revisioned_equality!(
	sql_order_Ordering,
	sql::order::Ordering,
	expr::order::Ordering,
	sql::order::Ordering::Random
);
test_revisioned_equality!(
	sql_order_OrderList,
	sql::order::OrderList,
	expr::order::OrderList,
	sql::order::OrderList::default()
);
test_revisioned_equality!(
	sql_order_Order,
	sql::order::Order,
	expr::order::Order,
	sql::order::Order::default()
);
test_revisioned_equality!(sql_Output, sql::Output, expr::Output, sql::Output::default());
test_revisioned_equality!(sql_Param, sql::Param, expr::Param, sql::Param::default());
test_revisioned_equality!(sql_Part, sql::Part, expr::Part, sql::Part::All);
test_revisioned_equality!(
	sql_part_DestructurePart,
	sql::part::DestructurePart,
	expr::part::DestructurePart,
	sql::part::DestructurePart::All(sql::Ident::default())
);
test_revisioned_equality!(
	sql_part_Recurse,
	sql::part::Recurse,
	expr::part::Recurse,
	sql::part::Recurse::Fixed(5)
);
test_revisioned_equality!(
	sql_part_RecurseInstruction,
	sql::part::RecurseInstruction,
	expr::part::RecurseInstruction,
	sql::part::RecurseInstruction::Path {
		inclusive: true
	}
);
test_revisioned_equality!(
	sql_Permissions,
	sql::Permissions,
	expr::Permissions,
	sql::Permissions::default()
);
test_revisioned_equality!(
	sql_Permission,
	sql::Permission,
	expr::Permission,
	sql::Permission::default()
);
test_revisioned_equality!(sql_Query, sql::Query, expr::Query, sql::Query::default());
test_revisioned_equality!(
	sql_Range,
	sql::Range,
	expr::Range,
	sql::Range {
		beg: Bound::Included(1.into()),
		end: Bound::Excluded(2.into())
	}
);
test_revisioned_equality!(
	sql_reference_Reference,
	sql::reference::Reference,
	expr::reference::Reference,
	sql::reference::Reference {
		on_delete: sql::reference::ReferenceDeleteStrategy::Reject,
	}
);
test_revisioned_equality!(
	sql_reference_ReferenceDeleteStrategy,
	sql::reference::ReferenceDeleteStrategy,
	expr::reference::ReferenceDeleteStrategy,
	sql::reference::ReferenceDeleteStrategy::Unset
);
test_revisioned_equality!(
	sql_reference_Refs,
	sql::reference::Refs,
	expr::reference::Refs,
	sql::reference::Refs(vec![(Some(sql::Table::default()), Some(sql::Idiom::default()))])
);
test_revisioned_equality!(
	sql_Regex,
	sql::Regex,
	expr::Regex,
	sql::Regex(regex::Regex::new("test").unwrap())
);
test_revisioned_equality!(sql_Scoring, sql::Scoring, expr::Scoring, sql::Scoring::default());
test_revisioned_equality!(sql_Script, sql::Script, expr::Script, sql::Script::default());
test_revisioned_equality!(sql_Splits, sql::Splits, expr::Splits, sql::Splits::default());
test_revisioned_equality!(sql_Split, sql::Split, expr::Split, sql::Split::default());
test_revisioned_equality!(sql_Start, sql::Start, expr::Start, sql::Start::default());
test_revisioned_equality!(
	sql_Statements,
	sql::Statements,
	expr::LogicalPlans,
	sql::Statements::default()
);
test_revisioned_equality!(
	sql_Statement,
	sql::Statement,
	expr::LogicalPlan,
	sql::Statement::Value(sql::SqlValue::default())
);
test_revisioned_equality!(sql_Strand, sql::Strand, expr::Strand, sql::Strand::default());
test_revisioned_equality!(
	sql_Subquery,
	sql::Subquery,
	expr::Subquery,
	sql::Subquery::Value(sql::SqlValue::default())
);
test_revisioned_equality!(
	sql_TableType,
	sql::TableType,
	expr::TableType,
	sql::TableType::default()
);
test_revisioned_equality!(sql_Relation, sql::Relation, expr::Relation, sql::Relation::default());
test_revisioned_equality!(sql_Tables, sql::Tables, expr::Tables, sql::Tables::default());
test_revisioned_equality!(sql_Table, sql::Table, expr::Table, sql::Table::default());
test_revisioned_equality!(
	sql_Thing,
	sql::Thing,
	expr::Thing,
	sql::Thing {
		tb: "table".to_string(),
		id: sql::Id::Number(1),
	}
);
test_revisioned_equality!(sql_Timeout, sql::Timeout, expr::Timeout, sql::Timeout::default());
test_revisioned_equality!(sql_Tokenizer, sql::Tokenizer, expr::Tokenizer, sql::Tokenizer::Class);
test_revisioned_equality!(
	sql_user_UserDuration,
	sql::user::UserDuration,
	expr::user::UserDuration,
	sql::user::UserDuration::default()
);
test_revisioned_equality!(sql_Uuid, sql::Uuid, expr::Uuid, sql::Uuid::default());
test_revisioned_equality!(sql_Version, sql::Version, expr::Version, sql::Version::default());
test_revisioned_equality!(sql_View, sql::View, expr::View, sql::View::default());
test_revisioned_equality!(sql_With, sql::With, expr::With, sql::With::NoIndex);
