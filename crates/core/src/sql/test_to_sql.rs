use geo::Point;
use rstest::rstest;
use surrealdb_types::ToSql;

use crate::sql::literal::ObjectEntry;
use crate::sql::statements::access::{AccessStatementGrant, Subject};
use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::rebuild::RebuildIndexStatement;
use crate::sql::statements::show::ShowSince;
use crate::sql::statements::{
	AccessStatement, AlterStatement, AlterTableStatement, CreateStatement, DefineStatement,
	DefineTableStatement, DeleteStatement, ForeachStatement, IfelseStatement, InfoStatement,
	InsertStatement, OptionStatement, OutputStatement, RebuildStatement, RelateStatement,
	RemoveStatement, RemoveTableStatement, SelectStatement, SetStatement, ShowStatement,
	SleepStatement, UpdateStatement, UpsertStatement, UseStatement,
};
use crate::sql::{
	BinaryOperator, Block, Closure, Constant, Data, Expr, Fields, Function, FunctionCall, Idiom,
	KillStatement, Literal, LiveStatement, Mock, Param, PostfixOperator, PrefixOperator,
	RecordIdKeyLit, RecordIdLit, TopLevelExpr,
};
use crate::types::{PublicBytes, PublicDuration, PublicFile, PublicGeometry};
use crate::val::range::TypedRange;
use crate::val::{Bytes, Duration, File, Geometry, Number, Object, RecordId, Set, Value};

#[rstest]
// Values
#[case::value_none(Value::None, "NONE", "NONE")]
#[case::value_null(Value::Null, "NULL", "NULL")]
#[case::value_bool_true(Value::Bool(true), "true", "true")]
#[case::value_bool_false(Value::Bool(false), "false", "false")]
#[case::value_number_int(Value::Number(Number::Int(1)), "1", "1")]
#[case::value_number_float(Value::Number(Number::Float(1.0)), "1f", "1f")]
#[case::value_number_decimal(Value::Number(Number::Decimal(1.into())), "1dec", "1dec")]
#[case::value_string(Value::String("hello".to_string()), "'hello'", "'hello'")]
#[case::value_array(Value::Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))].into()), "[1, 2]", "[\n\t1,\n\t2\n]")]
#[case::value_set(Value::Set(Set::new()), "{,}", "{,}")]
#[case::value_set_one(Value::Set(Set::from(vec![Value::Number(Number::Int(1))])), "{1,}", "{1,}")]
#[case::value_set_two(Value::Set(Set::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))])), "{1, 2}", "{1, 2}")]
#[case::value_object(Value::Object(Object::from_iter(vec![(String::from("key"), Value::Number(Number::Int(1)))].into_iter())), "{ key: 1 }", "{\n\tkey: 1\n}")]
#[case::value_geometry(Value::Geometry(Geometry::Point(Point::new(1.0, 2.0))), "(1, 2)", "(1, 2)")]
#[case::value_bytes(Value::Bytes(Bytes(b"hello".to_vec())), "b\"68656C6C6F\"", "b\"68656C6C6F\"")]
#[case::value_datetime(Value::Datetime("1970-01-01T00:00:00Z".parse().unwrap()), "d'1970-01-01T00:00:00Z'", "d'1970-01-01T00:00:00Z'")]
#[case::value_duration(Value::Duration(Duration::from_secs(1)), "1s", "1s")]
#[case::value_file(Value::File(File::new("bucket".to_string(), "path/to/file.txt".to_string())), "f\"bucket:/path/to/file.txt\"", "f\"bucket:/path/to/file.txt\"")]
#[case::value_record_id(Value::RecordId(RecordId::new("table".to_string(), "123".to_string())), "table:⟨123⟩", "table:⟨123⟩")]
#[case::value_regex(Value::Regex("hello".parse().unwrap()), "/hello/", "/hello/")]
// Expression: Literals
#[case::expr_lit_none(Expr::Literal(Literal::None), "NONE", "NONE")]
#[case::expr_lit_null(Expr::Literal(Literal::Null), "NULL", "NULL")]
#[case::expr_lit_bool_true(Expr::Literal(Literal::Bool(true)), "true", "true")]
#[case::expr_lit_bool_false(Expr::Literal(Literal::Bool(false)), "false", "false")]
#[case::expr_lit_number_int(Expr::Literal(Literal::Integer(1)), "1", "1")]
#[case::expr_lit_number_float(Expr::Literal(Literal::Float(1.0)), "1f", "1f")]
#[case::expr_lit_number_decimal(Expr::Literal(Literal::Decimal(1.into())), "1dec", "1dec")]
#[case::expr_lit_string(Expr::Literal(Literal::String("hello".to_string())), "'hello'", "'hello'")]
#[case::expr_lit_array(Expr::Literal(Literal::Array(vec![
    Expr::Literal(Literal::Integer(1)),
    Expr::Literal(Literal::Integer(2))
])), "[1, 2]", "[\n\t1,\n\t2\n]")]
#[case::expr_lit_object(Expr::Literal(Literal::Object(vec![
    ObjectEntry {
        key: "key".to_string(),
        value: Expr::Literal(Literal::Integer(1))
    }
])), "{ key: 1 }", "{\n\tkey: 1\n}")]
#[case::expr_lit_geometry(
	Expr::Literal(Literal::Geometry(PublicGeometry::Point(Point::new(1.0, 2.0)))),
	"(1, 2)",
	"(1, 2)"
)]
#[case::expr_lit_bytes(Expr::Literal(Literal::Bytes(PublicBytes::from(Bytes(b"hello".to_vec())))), "b\"68656C6C6F\"", "b\"68656C6C6F\"")]
#[case::expr_lit_datetime(Expr::Literal(Literal::Datetime("1970-01-01T00:00:00Z".parse().unwrap())), "d'1970-01-01T00:00:00Z'", "d'1970-01-01T00:00:00Z'")]
#[case::expr_lit_duration(
	Expr::Literal(Literal::Duration(PublicDuration::from(Duration::from_secs(1)))),
	"1s",
	"1s"
)]
#[case::expr_lit_file(Expr::Literal(Literal::File(PublicFile::from(File::new("bucket".to_string(), "path/to/file.txt".to_string())))), "f\"bucket:/path/to/file.txt\"", "f\"bucket:/path/to/file.txt\"")]
#[case::expr_lit_record_id(Expr::Literal(Literal::RecordId(RecordIdLit {
    table: "table".to_string(),
    key: RecordIdKeyLit::Number(123)
})), "table:123", "table:123")]
#[case::expr_lit_regex(Expr::Literal(Literal::Regex("hello".parse().unwrap())), "/hello/", "/hello/")]
// Expression: Params
#[case::expr_param(Expr::Param(Param::new("x".to_string())), "$x", "$x")]
// Expression: Idioms
#[case::expr_idiom_field(Expr::Idiom(Idiom::field("x".to_string())), "x", "x")]
// Expression: Tables
#[case::expr_table(Expr::Table("table".to_string()), "`table`", "`table`")]
// Expression: Mocks
#[case::expr_mock_count(Expr::Mock(Mock::Count("table".to_string(), 1)), "|`table`:1|", "|`table`:1|")]
#[case::expr_mock_range(Expr::Mock(Mock::Range("table".to_string(), TypedRange::from_range(1..10))), "|`table`:1..10|", "|`table`:1..10|")]
// Expression: Block
#[case::expr_block_empty(Expr::Block(Box::new(Block(vec![]))), "{;}", "{;}")]
#[case::expr_block(Expr::Block(Box::new(Block(vec![
    Expr::Literal(Literal::Integer(1)),
    Expr::Literal(Literal::Integer(2))
]))), "{\n1;\n2;\n}", "{\n\n\t1;\n\n\t2;\n\n}")]
// Expression: Constants
#[case::expr_constant_math_e(Expr::Constant(Constant::MathE), "math::E", "math::E")]
// Expression: Prefix
#[case::expr_prefix_not(Expr::Prefix { op: PrefixOperator::Not, expr: Box::new(Expr::Literal(Literal::Bool(true))) }, "!true", "!true")]
#[case::expr_prefix_negate(Expr::Prefix { op: PrefixOperator::Negate, expr: Box::new(Expr::Literal(Literal::Integer(5))) }, "-5", "-5")]
// Expression: Postfix
#[case::expr_postfix_range(Expr::Postfix { expr: Box::new(Expr::Literal(Literal::Integer(1))), op: PostfixOperator::Range }, "1..", "1..")]
// Expression: Binary
#[case::expr_binary_add(Expr::Binary { left: Box::new(Expr::Literal(Literal::Integer(1))), op: BinaryOperator::Add, right: Box::new(Expr::Literal(Literal::Integer(2))) }, "1 + 2", "1 + 2")]
#[case::expr_binary_equal(Expr::Binary { left: Box::new(Expr::Param(Param::new("x".to_string()))), op: BinaryOperator::Equal, right: Box::new(Expr::Literal(Literal::Integer(5))) }, "$x = 5", "$x = 5")]
// Expression: FunctionCall
#[case::expr_function_call(Expr::FunctionCall(Box::new(FunctionCall { receiver: Function::Normal("count".to_string()), arguments: vec![] })), "count()", "count()")]
#[case::expr_function_call_args(Expr::FunctionCall(Box::new(FunctionCall { receiver: Function::Normal("array::len".to_string()), arguments: vec![Expr::Param(Param::new("arr".to_string()))] })), "array::len($arr)", "array::len($arr)")]
// Expression: Closure
#[case::expr_closure(Expr::Closure(Box::new(Closure { args: vec![], returns: None, body: Expr::Literal(Literal::Integer(1)) })), "|| 1", "|| 1")]
// Expression: Break
#[case::expr_break(Expr::Break, "BREAK", "BREAK")]
// Expression: Continue
#[case::expr_continue(Expr::Continue, "CONTINUE", "CONTINUE")]
// Expression: Throw
#[case::expr_throw(Expr::Throw(Box::new(Expr::Literal(Literal::String("error".to_string())))), "THROW 'error'", "THROW 'error'")]
// Expression: Return
#[case::expr_return(Expr::Return(Box::new(OutputStatement { what: Expr::Literal(Literal::Integer(1)), fetch: None })), "RETURN 1", "RETURN 1")]
// Expression: If
#[case::expr_if(Expr::If(Box::new(IfelseStatement { exprs: vec![(Expr::Literal(Literal::Bool(true)), Expr::Block(Box::new(Block(vec![Expr::Literal(Literal::Integer(1))]))))], close: None })), "IF true { 1 }", "IF true\n\t{ 1 }")]
// Expression: Select
#[case::expr_select(Expr::Select(Box::new(SelectStatement { expr: Fields::all(), omit: vec![], only: false, what: vec![Expr::Table("user".to_string())], with: None, cond: None, split: None, group: None, order: None, limit: None, start: None, fetch: None, version: None, timeout: None, parallel: false, explain: None, tempfiles: false })), "SELECT * FROM user", "SELECT * FROM user")]
// Expression: Create
#[case::expr_create(Expr::Create(Box::new(CreateStatement { only: false, what: vec![Expr::Table("user".to_string())], data: None, output: None, timeout: None, parallel: false, version: None })), "CREATE user", "CREATE user")]
// Expression: Update
#[case::expr_update(Expr::Update(Box::new(UpdateStatement { only: false, what: vec![Expr::Table("user".to_string())], with: None, data: None, cond: None, output: None, timeout: None, parallel: false, explain: None })), "UPDATE user", "UPDATE user")]
// Expression: Delete
#[case::expr_delete(Expr::Delete(Box::new(DeleteStatement { only: false, what: vec![Expr::Table("user".to_string())], with: None, cond: None, output: None, timeout: None, parallel: false, explain: None })), "DELETE user", "DELETE user")]
// Expression: Relate
#[case::expr_relate(Expr::Relate(Box::new(RelateStatement { only: false, through: Expr::Table("likes".to_string()), from: Expr::Param(Param::new("from".to_string())), to: Expr::Param(Param::new("to".to_string())), uniq: false, data: None, output: None, timeout: None, parallel: false })), "RELATE $from -> likes -> $to", "RELATE $from -> likes -> $to")]
// Expression: Insert
#[case::expr_insert(Expr::Insert(Box::new(InsertStatement { into: Some(Expr::Table("user".to_string())), data: Data::SingleExpression(Expr::Literal(Literal::Object(vec![ObjectEntry { key: "name".to_string(), value: Expr::Literal(Literal::String("test".to_string())) }]))), ignore: false, update: None, output: None, timeout: None, parallel: false, relation: false, version: None })), "INSERT INTO user { name: 'test' }", "INSERT INTO user {\n\tname: 'test'\n}")]
// Expression: Define
#[case::expr_define(
	Expr::Define(Box::new(DefineStatement::Table(DefineTableStatement::default()))),
	"DEFINE TABLE NONE TYPE ANY SCHEMALESS PERMISSIONS NONE",
	"DEFINE TABLE NONE TYPE ANY SCHEMALESS\n\tPERMISSIONS NONE"
)]
// Expression: Remove
#[case::expr_remove(
	Expr::Remove(Box::new(RemoveStatement::Table(RemoveTableStatement::default()))),
	"REMOVE TABLE NONE",
	"REMOVE TABLE NONE"
)]
// Expression: Rebuild
#[case::expr_rebuild(Expr::Rebuild(Box::new(RebuildStatement::Index(RebuildIndexStatement { name: "idx".to_string(), what: "user".to_string(), if_exists: false, concurrently: false }))), "REBUILD INDEX idx ON user", "REBUILD INDEX idx ON user")]
// Expression: Upsert
#[case::expr_upsert(Expr::Upsert(Box::new(UpsertStatement { only: false, what: vec![Expr::Table("user".to_string())], with: None, data: None, cond: None, output: None, timeout: None, parallel: false, explain: None })), "UPSERT user", "UPSERT user")]
// Expression: Alter
#[case::expr_alter(Expr::Alter(Box::new(AlterStatement::Table(AlterTableStatement { name: "user".to_string(), if_exists: false, schemafull: AlterKind::None, permissions: None, changefeed: AlterKind::None, comment: AlterKind::None, kind: None }))), "ALTER TABLE user", "ALTER TABLE user")]
// Expression: Info
#[case::expr_info(
	Expr::Info(Box::new(InfoStatement::Root(false))),
	"INFO FOR ROOT",
	"INFO FOR ROOT"
)]
// Expression: Foreach
#[case::expr_foreach(Expr::Foreach(Box::new(ForeachStatement { param: Param::new("item".to_string()), range: Expr::Literal(Literal::Array(vec![Expr::Literal(Literal::Integer(1)), Expr::Literal(Literal::Integer(2))])), block: Block(vec![Expr::Literal(Literal::Integer(1))]) })), "FOR $item IN [1, 2] { 1 }", "FOR $item IN [\n\t1,\n\t2\n] { 1 }")]
// Expression: Let
#[case::expr_let(Expr::Let(Box::new(SetStatement { name: "x".to_string(), what: Expr::Literal(Literal::Integer(5)), kind: None })), "LET $x = 5", "LET $x = 5")]
// Expression: Sleep
#[case::expr_sleep(Expr::Sleep(Box::new(SleepStatement { duration: PublicDuration::from(Duration::from_secs(1)) })), "SLEEP 1s", "SLEEP 1s")]
// Complex nested expressions
#[case::nested_if_else(
    Expr::If(Box::new(IfelseStatement {
        exprs: vec![
            (
                Expr::Binary {
                    left: Box::new(Expr::Param(Param::new("x".to_string()))),
                    op: BinaryOperator::MoreThan,
                    right: Box::new(Expr::Literal(Literal::Integer(10)))
                },
                Expr::Block(Box::new(Block(vec![
                    Expr::Let(Box::new(SetStatement {
                        name: "result".to_string(),
                        what: Expr::Literal(Literal::String("high".to_string())),
                        kind: None
                    })),
                    Expr::Return(Box::new(OutputStatement {
                        what: Expr::Param(Param::new("result".to_string())),
                        fetch: None
                    }))
                ])))
            ),
            (
                Expr::Binary {
                    left: Box::new(Expr::Param(Param::new("x".to_string()))),
                    op: BinaryOperator::MoreThan,
                    right: Box::new(Expr::Literal(Literal::Integer(5)))
                },
                Expr::Block(Box::new(Block(vec![
                    Expr::Return(Box::new(OutputStatement {
                        what: Expr::Literal(Literal::String("medium".to_string())),
                        fetch: None
                    }))
                ])))
            )
        ],
        close: Some(Expr::Block(Box::new(Block(vec![
            Expr::Return(Box::new(OutputStatement {
                what: Expr::Literal(Literal::String("low".to_string())),
                fetch: None
            }))
        ]))))
    })),
    "IF $x > 10 {\nLET $result = 'high';\nRETURN $result;\n} ELSE IF $x > 5 { RETURN 'medium' } ELSE { RETURN 'low' }",
    "IF $x > 10\n\t{\n\n\t\tLET $result = 'high';\n\n\t\tRETURN $result;\n\t\n}\nELSE IF $x > 5\n\t{ RETURN 'medium' }\nELSE\n\t{ RETURN 'low' }"
)]
#[case::nested_foreach_with_select(
    Expr::Foreach(Box::new(ForeachStatement {
        param: Param::new("user".to_string()),
        range: Expr::Select(Box::new(SelectStatement {
            expr: Fields::all(),
            omit: vec![],
            only: false,
            what: vec![Expr::Table("users".to_string())],
            with: None,
            cond: None,
            split: None,
            group: None,
            order: None,
            limit: None,
            start: None,
            fetch: None,
            version: None,
            timeout: None,
            parallel: false,
            explain: None,
            tempfiles: false
        })),
        block: Block(vec![
            Expr::If(Box::new(IfelseStatement {
                exprs: vec![(
                    Expr::Binary {
                        left: Box::new(Expr::Idiom(Idiom(vec![
                            crate::sql::Part::Field("user".to_string()),
                            crate::sql::Part::Field("active".to_string())
                        ]))),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expr::Literal(Literal::Bool(true)))
                    },
                    Expr::Block(Box::new(Block(vec![
                        Expr::Create(Box::new(CreateStatement {
                            only: false,
                            what: vec![Expr::Table("active_users".to_string())],
                            data: Some(Data::ContentExpression(Expr::Param(Param::new("user".to_string())))),
                            output: None,
                            timeout: None,
                            parallel: false,
                            version: None
                        }))
                    ])))
                )],
                close: None
            }))
        ])
    })),
    "FOR $user IN SELECT * FROM users { IF user.active = true { CREATE active_users CONTENT $user } }",
    "FOR $user IN SELECT * FROM users { IF user.active = true\n\t{ CREATE active_users CONTENT $user }\n }"
)]
#[case::deeply_nested_object(
    Expr::Literal(Literal::Object(vec![
        ObjectEntry {
            key: "user".to_string(),
            value: Expr::Literal(Literal::Object(vec![
                ObjectEntry {
                    key: "name".to_string(),
                    value: Expr::Literal(Literal::String("Alice".to_string()))
                },
                ObjectEntry {
                    key: "settings".to_string(),
                    value: Expr::Literal(Literal::Object(vec![
                        ObjectEntry {
                            key: "theme".to_string(),
                            value: Expr::Literal(Literal::String("dark".to_string()))
                        },
                        ObjectEntry {
                            key: "notifications".to_string(),
                            value: Expr::Literal(Literal::Object(vec![
                                ObjectEntry {
                                    key: "email".to_string(),
                                    value: Expr::Literal(Literal::Bool(true))
                                },
                                ObjectEntry {
                                    key: "push".to_string(),
                                    value: Expr::Literal(Literal::Bool(false))
                                }
                            ]))
                        }
                    ]))
                },
                ObjectEntry {
                    key: "tags".to_string(),
                    value: Expr::Literal(Literal::Array(vec![
                        Expr::Literal(Literal::String("admin".to_string())),
                        Expr::Literal(Literal::String("premium".to_string()))
                    ]))
                }
            ]))
        }
    ])),
    "{ user: { name: 'Alice', settings: { theme: 'dark', notifications: { email: true, push: false } }, tags: ['admin', 'premium'] } }",
    "{\n\tuser: {\n\t\tname: 'Alice',\n\t\tsettings: {\n\t\t\ttheme: 'dark',\n\t\t\tnotifications: {\n\t\t\t\temail: true,\n\t\t\t\tpush: false\n\t\t\t}\n\t\t},\n\t\ttags: [\n\t\t\t'admin',\n\t\t\t'premium'\n\t\t]\n\t}\n}"
)]
#[case::top_level_begin(TopLevelExpr::Begin, "BEGIN", "BEGIN")]
#[case::top_level_cancel(TopLevelExpr::Cancel, "CANCEL", "CANCEL")]
#[case::top_level_commit(TopLevelExpr::Commit, "COMMIT", "COMMIT")]
#[case::top_level_access(TopLevelExpr::Access(Box::new(AccessStatement::Grant(
    AccessStatementGrant {
        ac: "user".to_string(),
        base: None,
        subject: Subject::Record(RecordIdLit { table: "user".to_string(), key: RecordIdKeyLit::Number(123) }),
    }))), "ACCESS user GRANT FOR RECORD user:123", "ACCESS user GRANT FOR RECORD user:123")]
#[case::top_level_kill(TopLevelExpr::Kill(KillStatement { id: Expr::Param(Param::new("id".to_string())) }), "KILL $id", "KILL $id")]
#[case::top_level_live(TopLevelExpr::Live(Box::new(LiveStatement { fields: Fields::all(), diff: false, what: Expr::Table("user".to_string()), cond: None, fetch: None })), "LIVE SELECT * FROM user", "LIVE SELECT * FROM user")]
#[case::top_level_live_diff(TopLevelExpr::Live(Box::new(LiveStatement { fields: Fields::none(), diff: true, what: Expr::Table("user".to_string()), cond: None, fetch: None })), "LIVE SELECT DIFF FROM user", "LIVE SELECT DIFF FROM user")]
#[case::top_level_option(TopLevelExpr::Option(OptionStatement { name: "IMPORT".to_string(), what: true }), "OPTION IMPORT", "OPTION IMPORT")]
#[case::top_level_use(TopLevelExpr::Use(UseStatement { ns: Some("ns".to_string()), db: Some("db".to_string()) }), "USE NS ns DB db", "USE NS ns DB db")]
#[case::top_level_show(TopLevelExpr::Show(ShowStatement { table: Some("user".to_string()), since: ShowSince::Versionstamp(123), limit: Some(10) }), "SHOW CHANGES FOR TABLE user SINCE 123 LIMIT 10", "SHOW CHANGES FOR TABLE user SINCE 123 LIMIT 10")]
#[case::top_level_expr(TopLevelExpr::Expr(Expr::Literal(Literal::Integer(1))), "1", "1")]
fn test_to_sql(#[case] v: impl ToSql, #[case] expected: &str, #[case] expected_pretty: &str) {
	assert_eq!(v.to_sql(), expected);
	assert_eq!(v.to_sql_pretty(), expected_pretty);
}
