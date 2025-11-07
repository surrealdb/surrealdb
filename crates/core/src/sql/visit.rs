use crate::sql::{
	graph::GraphSubject,
	order::Ordering,
	part::{DestructurePart, Recurse, RecurseInstruction},
	reference::{Reference, ReferenceDeleteStrategy, Refs},
	statements::{
		access::{
			AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke, AccessStatementShow,
			Subject,
		},
		define::{
			config::{api::ApiConfig, graphql::GraphQLConfig, ConfigInner},
			ApiAction, DefineConfigStatement,
		},
		rebuild::RebuildStatement,
		AccessStatement, AlterStatement, AlterTableStatement, AnalyzeStatement, CreateStatement,
		DefineAccessStatement, DefineAnalyzerStatement, DefineApiStatement,
		DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement,
		DefineFunctionStatement, DefineIndexStatement, DefineModelStatement,
		DefineNamespaceStatement, DefineParamStatement, DefineStatement, DefineTableStatement,
		DefineUserStatement, DeleteStatement, ForeachStatement, IfelseStatement, InfoStatement,
		InsertStatement, KillStatement, LiveStatement, OptionStatement, OutputStatement,
		RelateStatement, RemoveAccessStatement, RemoveAnalyzerStatement, RemoveDatabaseStatement,
		RemoveEventStatement, RemoveFieldStatement, RemoveFunctionStatement, RemoveIndexStatement,
		RemoveModelStatement, RemoveNamespaceStatement, RemoveParamStatement, RemoveStatement,
		RemoveTableStatement, RemoveUserStatement, SelectStatement, SetStatement, ShowStatement,
		SleepStatement, ThrowStatement, UpdateStatement, UpsertStatement, UseStatement,
	},
	Array, Block, Cast, Closure, Data, Edges, Entry, Expression, Field, Fields, Function, Future,
	Geometry, Graph, Id, IdRange, Idiom, Kind, Literal, Mock, Model, Number, Object, Operator,
	Output, Param, Part, Permission, Permissions, Query, Range, Relation, Statement, Statements,
	Subquery, TableType, Thing, Value, View,
};
use std::ops::Bound;

use super::{
	access_type::BearerAccess, statements::define::ApiDefinition, AccessType, JwtAccess,
	RecordAccess,
};

macro_rules! implement_visitor{
	($(fn $name:ident($this:ident, $value:ident: &$ty:ty) {
	    $($t:tt)*
	})*) => {

		#[allow(dead_code)]
		pub trait Visit<V: Visitor>{
			fn visit(&self, v: &mut V) -> Result<(), V::Error>;
		}

		#[allow(dead_code)]
		pub trait Visitor: Sized {
			type Error;

			$(
				fn $name(&mut self, $value: &$ty) -> Result<(), Self::Error>{
					$value.visit(self)
				}
			)*
		}

		$(
			impl<V: Visitor> Visit<V> for $ty {
				#[allow(unused_variables)]
				fn visit(&self, $this: &mut V) -> Result<(), V::Error>{
					let $value = self;
					$($t)*
				}
			}
		)*
	};
}

implement_visitor! {
	fn visit_statements(this, value: &Statements)  {
		for v in value.0.iter(){
			this.visit_statement(v)?;
		}
		Ok(())
	}

	fn visit_statement(this, value: &Statement) {
		match value {
			Statement::Value(v) => {
				this.visit_value(v)?;
			}
			Statement::Analyze(a) => {
				this.visit_analyze_stmt(a)?;
			}
			Statement::Begin(_) => {}
			Statement::Break(_) => {}
			Statement::Continue(_) => {}
			Statement::Cancel(_) => {}
			Statement::Commit(_) => {}
			Statement::Create(c) => {
				this.visit_create(c)?;
			}
			Statement::Define(d) => {
				this.visit_define(d)?;
			}
			Statement::Delete(d) => {
				this.visit_delete(d)?;
			}
			Statement::Foreach(f) => {
				this.visit_foreach(f)?;
			}
			Statement::Ifelse(i) => {
				this.visit_if_else(i)?;
			}
			Statement::Info(i) => {
				this.visit_info(i)?;
			}
			Statement::Insert(i) => {
				this.visit_insert(i)?;
			}
			Statement::Kill(k) => {
				this.visit_kill(k)?;
			}
			Statement::Live(l) => {
				this.visit_live(l)?;
			}
			Statement::Option(o) => {
				this.visit_option(o)?;
			}
			Statement::Output(o) => {
				this.visit_output_stmt(o)?;
			}
			Statement::Relate(r) => {
				this.visit_relate(r)?;
			}
			Statement::Remove(r) => {
				this.visit_remove(r)?;
			}
			Statement::Select(s) => {
				this.visit_select(s)?
			}
			Statement::Set(s) => {
				this.visit_set(s)?;
			}
			Statement::Show(s) => {
				this.visit_show(s)?;
			}
			Statement::Sleep(s) => {
				this.visit_sleep(s)?;
			}
			Statement::Update(u) => {
				this.visit_update(u)?;
			}
			Statement::Throw(t) => {
				this.visit_throw(t)?;
			}
			Statement::Use(u) => {
				this.visit_use(u)?;
			}
			Statement::Rebuild(r) => {
				this.visit_rebuild(r)?;
			}
			Statement::Upsert(u) => {
				this.visit_upsert(u)?;
			}
			Statement::Alter(a) => {
				this.visit_alter(a)?;
			}
			Statement::Access(a) => {
				this.visit_access(a)?;
			},
		}
		Ok(())
	}

	fn visit_access(this, a: &AccessStatement){
		match a {
			AccessStatement::Grant(a) => {
				this.visit_access_grant(a)?;
			},
			AccessStatement::Show(a) => {
				this.visit_access_show(a)?;
			},
			AccessStatement::Revoke(a) => {
				this.visit_access_revoke(a)?;
			},
			AccessStatement::Purge(a) => {
				this.visit_access_purge(a)?;
			},
		}
		Ok(())
	}

	fn visit_access_grant(this, a: &AccessStatementGrant){
		this.visit_access_subject(&a.subject)?;
		Ok(())
	}

	fn visit_access_subject(this, s: &Subject){
		match s{
			Subject::Record(thing) => {
				this.visit_thing(thing)?;
			},
			Subject::User(_) => { }
		}
		Ok(())
	}

	fn visit_access_show(this, a: &AccessStatementShow){
		if let Some(c) = a.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		Ok(())
	}
	fn visit_access_revoke(this, a: &AccessStatementRevoke){
		if let Some(c) = a.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		Ok(())
	}
	fn visit_access_purge(this, a: &AccessStatementPurge){
		Ok(())
	}

	fn visit_alter(this, a: &AlterStatement){
		match a {
			AlterStatement::Table(a) => {
				this.visit_alter_table(a)?;
			},
		}
		Ok(())
	}

	fn visit_alter_table(this, a: &AlterTableStatement){
		if let Some(p) = a.permissions.as_ref(){
			this.visit_permissions(p)?;
		}
		Ok(())
	}


	fn visit_upsert(this, u: &UpsertStatement){
		for v in u.what.0.iter(){
			this.visit_value(v)?;
		}
		if let Some(d) = u.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = u.output.as_ref(){
			this.visit_output(o)?;
		}
		if let Some(c) = u.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		Ok(())

	}

	fn visit_rebuild(this, r: &RebuildStatement){
		Ok(())
	}

	fn visit_use(this, t: &UseStatement){
		Ok(())
	}

	fn visit_throw(this, t: &ThrowStatement){
		this.visit_value(&t.error)
	}

	fn visit_update(this, u: &UpdateStatement){
		for v in u.what.0.iter(){
			this.visit_value(v)?;
		}
		if let Some(d) = u.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = u.output.as_ref(){
			this.visit_output(o)?;
		}
		if let Some(c) = u.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		Ok(())

	}

	fn visit_sleep(this, s: &SleepStatement){
		Ok(())
	}

	fn visit_show(this, s: &ShowStatement){
		Ok(())
	}

	fn visit_set(this, s: &SetStatement){
		if let Some(k) = s.kind.as_ref(){
			this.visit_kind(k)?;
		}
		this.visit_value(&s.what)?;
		Ok(())
	}

	fn visit_select(this, s: &SelectStatement){
		this.visit_fields(&s.expr)?;
		if let Some(o) = s.omit.as_ref(){
			for o in o.0.iter(){
				this.visit_idiom(o)?;
			}
		}
		for v in s.what.0.iter(){
			this.visit_value(v)?;
		}
		if let Some(c) = s.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		if let Some(s) = s.split.as_ref(){
			for s in s.0.iter(){
				this.visit_idiom(&s.0)?;
			}
		}
		if let Some(g) = s.group.as_ref(){
			for g in g.0.iter(){
				this.visit_idiom(&g.0)?;
			}
		}
		if let Some(o) = s.order.as_ref(){
			this.visit_ordering(o)?;
		}
		if let Some(l) = s.limit.as_ref(){
			this.visit_value(&l.0)?;
		}
		if let Some(f) = s.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_value(&f.0)?;
			}
		}
		if let Some(v) = s.version.as_ref(){
			this.visit_value(&v.0)?;
		}

		Ok(())
	}

	fn visit_remove(this, r: &RemoveStatement){
		match r {
			RemoveStatement::Namespace(r) => {
				this.visit_remove_namespace(r)?;
			},
			RemoveStatement::Database(r) => {
				this.visit_remove_database(r)?;
			},
			RemoveStatement::Function(r) => {
				this.visit_remove_function(r)?;
			},
			RemoveStatement::Analyzer(r) => {
				this.visit_remove_analyzer(r)?;
			},
			RemoveStatement::Access(r) => {
				this.visit_remove_access(r)?;
			},
			RemoveStatement::Param(r) => {
				this.visit_remove_param(r)?;
			},
			RemoveStatement::Table(r) => {
				this.visit_remove_table(r)?;
			},
			RemoveStatement::Event(r) => {
				this.visit_remove_event(r)?;
			},
			RemoveStatement::Field(r) => {
				this.visit_remove_field(r)?;
			},
			RemoveStatement::Index(r) => {
				this.visit_remove_index(r)?;
			},
			RemoveStatement::User(r) => {
				this.visit_remove_user(r)?;
			},
			RemoveStatement::Model(r) => {
				this.visit_remove_model(r)?;
			},
		}
		Ok(())
	}

	fn visit_remove_namespace(this, r: &RemoveNamespaceStatement){
		Ok(())
	}

	fn visit_remove_database(this, r: &RemoveDatabaseStatement){
		Ok(())
	}

	fn visit_remove_function(this, r: &RemoveFunctionStatement){
		Ok(())
	}

	fn visit_remove_analyzer(this, r: &RemoveAnalyzerStatement){
		Ok(())
	}

	fn visit_remove_access(this, r: &RemoveAccessStatement){
		Ok(())
	}

	fn visit_remove_param(this, r: &RemoveParamStatement){
		Ok(())
	}

	fn visit_remove_table(this, r: &RemoveTableStatement){
		Ok(())
	}

	fn visit_remove_event(this, r: &RemoveEventStatement){
		Ok(())
	}

	fn visit_remove_field(this, r: &RemoveFieldStatement){
		this.visit_idiom(&r.name)?;
		Ok(())
	}

	fn visit_remove_index(this, r: &RemoveIndexStatement){
		Ok(())
	}

	fn visit_remove_user(this, r: &RemoveUserStatement){
		Ok(())
	}

	fn visit_remove_model(this, r: &RemoveModelStatement){
		Ok(())
	}

	fn visit_relate(this, o: &RelateStatement){
		this.visit_value(&o.kind)?;
		this.visit_value(&o.from)?;
		this.visit_value(&o.with)?;
		if let Some(d) = o.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = o.output.as_ref(){
			this.visit_output(o)?;
		}
		Ok(())
	}

	fn visit_output_stmt(this, o: &OutputStatement){
		this.visit_value(&o.what)?;
		if let Some(f) = o.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_value(&f.0)?;
			}
		}
		Ok(())
	}

	fn visit_option(this, i: &OptionStatement){
		Ok(())
	}

	fn visit_live(this, l: &LiveStatement){
		this.visit_fields(&l.expr)?;
		this.visit_value(&l.what)?;
		if let Some(c) = l.cond.as_ref(){
			this.visit_value(c)?;
		}
		if let Some(f) = l.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_value(&f.0)?;
			}
		}

		if let Some(s) = l.session.as_ref(){
			this.visit_value(s)?;
		}
		Ok(())
	}

	fn visit_kill(this, i: &KillStatement){
		this.visit_value(&i.id)?;
		Ok(())
	}

	fn visit_insert(this, i: &InsertStatement){
		if let Some(v) = i.into.as_ref(){
			this.visit_value(v)?;
		}
		this.visit_data(&i.data)?;
		if let Some(update) = i.update.as_ref(){
			this.visit_data(update)?;
		}
		if let Some(o) = i.output.as_ref(){
			this.visit_output(o)?;
		}
		Ok(())
	}

	fn visit_info(this, i: &InfoStatement){
		Ok(())
	}

	fn visit_if_else(this, i: &IfelseStatement){
		for e in i.exprs.iter(){
			this.visit_value(&e.0)?;
			this.visit_value(&e.1)?;
		}
		if let Some(x) = i.close.as_ref(){
			this.visit_value(x)?;
		}
		Ok(())
	}

	fn visit_foreach(this, f: &ForeachStatement){
		this.visit_param(&f.param)?;
		this.visit_value(&f.range)?;
		this.visit_block(&f.block)?;
		Ok(())
	}

	fn visit_delete(this, d: &DeleteStatement){
		for v in d.what.iter(){
			this.visit_value(v)?;
		}
		if let Some(c) = d.cond.as_ref(){
			this.visit_value(&c.0)?;
		}
		if let Some(o) = d.output.as_ref(){
			this.visit_output(o)?;
		}
		Ok(())
	}

	fn visit_define(this, d: &DefineStatement){
		match d {
			DefineStatement::Namespace(d) => {
				this.visit_define_namespace(d)?;
			},
			DefineStatement::Database(d) => {
				this.visit_define_database(d)?;
			},
			DefineStatement::Function(d) => {
				this.visit_define_function(d)?;
			},
			DefineStatement::Analyzer(d) => {
				this.visit_define_analyzer(d)?;
			},
			DefineStatement::Param(d) => {
				this.visit_define_param(d)?;
			},
			DefineStatement::Table(d) => {
				this.visit_define_table(d)?;
			},
			DefineStatement::Event(d) => {
				this.visit_define_event(d)?;
			},
			DefineStatement::Field(d) => {
				this.visit_define_field(d)?;
			},
			DefineStatement::Index(d) => {
				this.visit_define_index(d)?;
			},
			DefineStatement::User(d) => {
				this.visit_define_user(d)?;
			},
			DefineStatement::Model(d) => {
				this.visit_define_model(d)?;
			},
			DefineStatement::Access(d) => {
				this.visit_define_access(d)?;
			},
			DefineStatement::Config(d) => {
				this.visit_define_config(d)?;
			},
			DefineStatement::Api(d) => {
				this.visit_define_api(d)?;
			},
		}
		Ok(())
	}

	fn visit_define_api(this, d: &DefineApiStatement) {
		this.visit_value(&d.path)?;
		for act in d.actions.iter(){
			this.visit_api_action(act)?;
		}
		if let Some(f) = d.fallback.as_ref(){
			this.visit_value(f)?;
		}
		if let Some(config) = d.config.as_ref(){
			this.visit_api_config(config)?;
		}

		Ok(())
	}

	fn visit_api_action(this, a: &ApiAction){
		this.visit_value(&a.action)?;
		if let Some(c) = a.config.as_ref(){
			this.visit_api_config(c)?;
		}
		Ok(())
	}


	fn visit_define_config(this, d: &DefineConfigStatement) {
		this.visit_config_inner(&d.inner)
	}

	fn visit_config_inner(this, d: &ConfigInner){
		match d {
			ConfigInner::GraphQL(graph_qlconfig) => {
				this.visit_graphql_config(graph_qlconfig)?;
			},
			ConfigInner::Api(api_config) => {
				this.visit_api_config(api_config)?;
			},
		}
		Ok(())
	}

	fn visit_api_config(this, d: &ApiConfig){
		if let Some(m) = d.middleware.as_ref(){
			for m in m.0.iter(){
				for v in m.1.iter(){
					this.visit_value(v)?;
				}
			}
		}
		if let Some(p) = d.permissions.as_ref(){
			this.visit_permission(p)?;
		}
		Ok(())
	}

	fn visit_graphql_config(this, d: &GraphQLConfig){
		Ok(())
	}


	fn visit_define_access(this, d: &DefineAccessStatement) {
		this.visit_access_type(&d.kind)?;

		if let Some(v) = d.authenticate.as_ref(){
			this.visit_value(v)?;
		}
		Ok(())
	}

	fn visit_access_type(this, a: &AccessType) {
		match a {
			AccessType::Record(ac) => this.visit_record_access(ac),
			AccessType::Jwt(ac) => this.visit_jwt_access(ac),
			AccessType::Bearer(ac) => this.visit_bearer_access(ac),
		}
	}

	fn visit_record_access(this, a: &RecordAccess){
		if let Some(s) = &a.signup{
			this.visit_value(s)?;
		}
		if let Some(s) = &a.signin{
			this.visit_value(s)?;
		}
		this.visit_jwt_access(&a.jwt)?;
		if let Some(ac) = &a.bearer{
			this.visit_bearer_access(ac)?;
		}
		Ok(())
	}

	fn visit_jwt_access(this, a: &JwtAccess){
		Ok(())
	}

	fn visit_bearer_access(this, a: &BearerAccess){
		this.visit_jwt_access(&a.jwt)?;
		Ok(())
	}

	fn visit_define_model(this, d: &DefineModelStatement) {
		this.visit_permission(&d.permissions)?;
		Ok(())
	}

	fn visit_define_user(this, d: &DefineUserStatement) {
		Ok(())
	}

	fn visit_define_index(this, d: &DefineIndexStatement) {
		for c in d.cols.0.iter(){
			this.visit_idiom(c)?;
		}
		Ok(())
	}

	fn visit_define_field(this, d: &DefineFieldStatement) {
		this.visit_idiom(&d.name)?;
		if let Some(k) = d.kind.as_ref(){
			this.visit_kind(k)?;
		}
		if let Some(v) = d.value.as_ref(){
			this.visit_value(v)?;
		}
		if let Some(v) = d.assert.as_ref(){
			this.visit_value(v)?;
		}
		if let Some(v) = d.default.as_ref(){
			this.visit_value(v)?;
		}
		this.visit_permissions(&d.permissions)?;
		if let Some(r) = d.reference.as_ref(){
			this.visit_reference(r)?;
		}
		Ok(())
	}

	fn visit_reference(this, d: &Reference){
		this.visit_delete_strategy(&d.on_delete)

	}

	fn visit_delete_strategy(this, d: &ReferenceDeleteStrategy){
		match d {
			ReferenceDeleteStrategy::Reject |
				ReferenceDeleteStrategy::Ignore |
				ReferenceDeleteStrategy::Cascade |
				ReferenceDeleteStrategy::Unset => {},
			ReferenceDeleteStrategy::Custom(value) => {
				this.visit_value(value)?;
			},

		}
		Ok(())
	}


	fn visit_define_event(this, d: &DefineEventStatement){
		this.visit_value(&d.when)?;
		for v in d.then.0.iter(){
			this.visit_value(v)?;
		}
		Ok(())
	}

	fn visit_define_table(this, d: &DefineTableStatement){
		if let Some(v) = d.view.as_ref(){
			this.visit_view(v)?;
		}
		this.visit_permissions(&d.permissions)?;

		Ok(())
	}

	fn visit_table_type(this, t: &TableType){
		match t {
			TableType::Any |
				TableType::Normal => {}
			TableType::Relation(relation) => {
				this.visit_relation(relation)?;
			},
		}
		Ok(())
	}

	fn visit_relation(this, r: &Relation){
		if let Some(k) = r.from.as_ref(){
			this.visit_kind(k)?;
		}
		if let Some(k) = r.to.as_ref(){
			this.visit_kind(k)?;
		}
		Ok(())
	}

	fn visit_permissions(this, d: &Permissions){
		this.visit_permission(&d.select)?;
		this.visit_permission(&d.create)?;
		this.visit_permission(&d.update)?;
		this.visit_permission(&d.delete)?;
		Ok(())
	}

	fn visit_view(this, v: &View){
		this.visit_fields(&v.expr)?;

		if let Some(c) = v.cond.as_ref(){
			this.visit_value(&c.0)?;
		}

		if let Some(g) = v.group.as_ref(){
			for g in g.0.iter(){
				this.visit_idiom(&g.0)?
			}
		}

		Ok(())
	}

	fn visit_define_param(this, d: &DefineParamStatement){
		this.visit_value(&d.value)?;
		this.visit_permission(&d.permissions)?;
		Ok(())
	}

	fn visit_define_analyzer(this, d: &DefineAnalyzerStatement){
		Ok(())
	}

	fn visit_define_function(this, d: &DefineFunctionStatement){
		for (_, k) in d.args.iter(){
			this.visit_kind(k)?;
		}
		this.visit_block(&d.block)?;
		this.visit_permission(&d.permissions)?;
		if let Some(k) = d.returns.as_ref(){
			this.visit_kind(k)?;
		}
		Ok(())
	}

	fn visit_permission(this, p: &Permission){
		match p {
			Permission::None |
				Permission::Full => {},
			Permission::Specific(value) => {
				this.visit_value(value)?;
			},
		}
		Ok(())
	}

	fn visit_define_database(this,  d: &DefineDatabaseStatement){
		Ok(())
	}

	fn visit_define_namespace(this,  d: &DefineNamespaceStatement){
		Ok(())
	}

	fn visit_create(this, c: &CreateStatement){
		for w in c.what.0.iter(){
			this.visit_value(w)?;
		}

		if let Some(data) = c.data.as_ref(){
			this.visit_data(data)?;
		}

		if let Some(output) = c.output.as_ref(){
			this.visit_output(output)?;
		}

		if let Some(v) = c.version.as_ref(){
			this.visit_value(&v.0)?
		}

		Ok(())
	}

	fn visit_output(this, o: &Output){
		match o {
			Output::None |
				Output::Null |
				Output::Diff |
				Output::After |
				Output::Before => {},
			Output::Fields(fields) => {
				this.visit_fields(fields)?;
			},
		}
		Ok(())
	}

	fn visit_data(this, d: &Data){
		match d {
			Data::EmptyExpression => {},
			Data::UpdateExpression(items) |
				Data::SetExpression(items) => {
					for (i, op, v) in items{
						this.visit_idiom(i)?;
						this.visit_operator(op)?;
						this.visit_value(v)?;
					}
				},
			Data::UnsetExpression(idioms) => {
				for i in idioms {
					this.visit_idiom(i)?;
				}
			},
			Data::PatchExpression(value) |
				Data::MergeExpression(value) |
				Data::ReplaceExpression(value) |
				Data::ContentExpression(value) |
				Data::SingleExpression(value) => {
					this.visit_value(value)?;
				},
			Data::ValuesExpression(items) => {
				for (i,v) in items.iter().flat_map(|x| x.iter()){
					this.visit_idiom(i)?;
					this.visit_value(v)?;
				}
			},
		}
		Ok(())
	}

	fn visit_analyze_stmt(this, v: &AnalyzeStatement){
		Ok(())
	}

	fn visit_value(this, value: &Value) {
		match value {

			Value::None => {}
			Value::Null => {}
			Value::Bool(_) => {}
			Value::Number(x) => {
				this.visit_number(x)?;
			}
			Value::Strand(_) => {}
			Value::Duration(_) => {}
			Value::Datetime(_) => {}
			Value::Uuid(_) => {}
			Value::Array(x) => {
				this.visit_array(x)?;
			}
			Value::Object(x) => {
				this.visit_object(x)?;
			}
			Value::Geometry(x) => {
				this.visit_geometry(x)?;
			}
			Value::Bytes(x) => {}
			Value::Thing(x) => {
				this.visit_thing(x)?;
			}
			Value::Param(x) => {
				this.visit_param(x)?;
			}
			Value::Idiom(x) => {
				this.visit_idiom(x)?;
			}
			Value::Table(x) => {}
			Value::Mock(x) => {
				this.visit_mock(x)?;
			}
			Value::Regex(x) => {}
			Value::Cast(x) => {
				this.visit_cast(x)?;
			}
			Value::Block(x) => {
				this.visit_block(x)?;
			}
			Value::Range(x) => {
				this.visit_range(x)?;
			}
			Value::Edges(x) => {
				this.visit_edges(x)?;
			}
			Value::Future(x) => {
				this.visit_future(x)?;
			}
			Value::Constant(_) => {}
			Value::Function(x) => {
				this.visit_function(x)?;
			}
			Value::Subquery(x) => {
				this.visit_subquery(x)?;
			}
			Value::Expression(x) => {
				this.visit_expression(x)?;
			}
			Value::Query(x) => {
				this.visit_query(x)?;
			}
			Value::Model(x) => {
				this.visit_model(x)?
			}
			Value::Closure(x) => {
				this.visit_closure(x)?;
			}
			Value::Refs(x) => {
				this.visit_refs(x)?;
			}
		}
		Ok(())
	}

	fn visit_refs(this, r: &Refs){
		for (_,i) in r.0.iter(){
			if let Some(i) = i.as_ref(){
				this.visit_idiom(i)?;
			}
		}
		Ok(())
	}


	fn visit_closure(this, c: &Closure){
		for (_,ref k) in c.args.iter(){
			this.visit_kind(k)?;
		}
		if let Some(k) = c.returns.as_ref(){
			this.visit_kind(k)?;
		}
		this.visit_value(&c.body)?;
		Ok(())
	}

	fn visit_model(this, m: &Model) {
		for a in m.args.iter(){
			this.visit_value(a)?;
		}
		Ok(())
	}

	fn visit_query(this, q: &Query){
		this.visit_statements(&q.0)

	}

	fn visit_expression(this, e: &Expression) {
		match e {
			Expression::Unary { o, v } => {
				this.visit_operator(o)?;
				this.visit_value(v)?;
			},
			Expression::Binary { l, o, r } => {
				this.visit_value(l)?;
				this.visit_operator(o)?;
				this.visit_value(r)?;
			},
		}
		Ok(())
	}

	fn visit_operator(this, o: &Operator){
		Ok(())
	}

	fn visit_subquery(this, s: &Subquery) {
		match s {
			Subquery::Value(value) => {
				this.visit_value(value)?;
			},
			Subquery::Ifelse(s) => {
				this.visit_if_else(s)?;
			},
			Subquery::Output(s) => {
				this.visit_output_stmt(s)?;
			},
			Subquery::Select(s) => {
				this.visit_select(s)?;
			},
			Subquery::Create(s) => {
				this.visit_create(s)?;
			},
			Subquery::Update(s) => {
				this.visit_update(s)?;
			},
			Subquery::Delete(s) => {
				this.visit_delete(s)?;
			},
			Subquery::Relate(s) => {
				this.visit_relate(s)?;
			},
			Subquery::Insert(s) => {
				this.visit_insert(s)?;
			},
			Subquery::Define(s) => {
				this.visit_define(s)?;
			},
			Subquery::Remove(s) => {
				this.visit_remove(s)?;
			},
			Subquery::Rebuild(s) => {
				this.visit_rebuild(s)?;
			},
			Subquery::Upsert(s) => {
				this.visit_upsert(s)?;
			},
			Subquery::Alter(s) => {
				this.visit_alter(s)?;
			},
		}
		Ok(())
	}

	fn visit_function(this, f: &Function){
		match f{
			Function::Normal(_, values) |
				Function::Custom(_, values) |
				Function::Script(_, values) => {
					for v in values{
						this.visit_value(v)?;
					}
				}
			Function::Anonymous(value, values, _) => {
				this.visit_value(value)?;
				for v in values{
					this.visit_value(v)?;
				}
			},
		}
		Ok(())
	}

	fn visit_future(this,f: &Future){
		this.visit_block(&f.0)
	}

	fn visit_edges(this, e: &Edges){
		this.visit_thing(&e.from)?;
		for s in e.what.0.iter(){
			this.visit_graph_subject(s)?;
		}
		Ok(())
	}

	fn visit_range(this, value: &Range){
		match value.beg{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_value(x)?;
			}
			Bound::Unbounded => {},
		}
		match value.end{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_value(x)?;
			}
			Bound::Unbounded => {},
		}
		Ok(())
	}

	fn visit_block(this, value: &Block){
		for v in value.0.iter(){
			this.visit_entry(v)?;
		}
		Ok(())

	}

	fn visit_entry(this, entry: &Entry){
		match entry{
			Entry::Value(value) => {
				this.visit_value(value)?;
			},
			Entry::Set(s) => {
				this.visit_set(s)?;
			},
			Entry::Ifelse(s) => {
				this.visit_if_else(s)?;
			},
			Entry::Select(s) => {
				this.visit_select(s)?;
			},
			Entry::Create(s) => {
				this.visit_create(s)?;
			},
			Entry::Update(s) => {
				this.visit_update(s)?;
			},
			Entry::Delete(s) => {
				this.visit_delete(s)?;
			},
			Entry::Relate(s) => {
				this.visit_relate(s)?;
			},
			Entry::Insert(s) => {
				this.visit_insert(s)?;
			},
			Entry::Output(s) => {
				this.visit_output_stmt(s)?;
			},
			Entry::Define(s) => {
				this.visit_define(s)?;
			},
			Entry::Remove(s) => {
				this.visit_remove(s)?;
			},
			Entry::Throw(s) => {
				this.visit_throw(s)?;
			},
			Entry::Break(_) => {},
			Entry::Continue(_) => {},
			Entry::Foreach(s) => {
				this.visit_foreach(s)?;
			},
			Entry::Rebuild(s) => {
				this.visit_rebuild(s)?;
			},
			Entry::Upsert(s) => {
				this.visit_upsert(s)?;
			},
			Entry::Alter(s) => {
				this.visit_alter(s)?;
			},
		}
		Ok(())
	}

	fn visit_cast(this, value: &Cast){
		this.visit_kind(&value.0)?;
		this.visit_value(&value.1)?;
		Ok(())
	}

	fn visit_kind(this, kind: &Kind){
		match kind {
			Kind::Any |
				Kind::Null |
				Kind::Bool |
				Kind::Bytes |
				Kind::Datetime |
				Kind::Decimal |
				Kind::Duration |
				Kind::Float |
				Kind::Int |
				Kind::Number |
				Kind::Object |
				Kind::Point |
				Kind::String |
				Kind::Uuid |
				Kind::Regex |
				Kind::Range |
				Kind::Record(_) |
				Kind::Geometry(_) => {}
			Kind::Option(kind) => {
				this.visit_kind(kind)?;
			},
			Kind::Either(kinds) => {
				for k in kinds.iter() {
					this.visit_kind(k)?;
				}
			},
			Kind::Set(kind, _) => {
				this.visit_kind(kind)?;
			},
			Kind::Array(kind, _) => {
				this.visit_kind(kind)?;
			},
			Kind::Function(kinds, kind) => {
				if let Some(kinds) = kinds.as_ref(){
					for k in kinds.iter() {
						this.visit_kind(k)?;
					}
				}
				if let Some(k) = kind {
					this.visit_kind(k)?;
				}
			},
			Kind::Literal(literal) => {
				this.visit_kind_literal(literal)?;
			},
			Kind::References(table, idiom) => {
				if let Some(idiom) = idiom {
					this.visit_idiom(idiom)?;
				}
			},
		}
		Ok(())
	}

	fn visit_kind_literal(this, k: &Literal){
		match k{
			Literal::String(_) => {},
			Literal::Bool(_) => {},
			Literal::Duration(_) => {},
			Literal::Number(number) => {
				this.visit_number(number)?;
			},
			Literal::Array(kinds) => {
				for k in kinds.iter(){
					this.visit_kind(k)?;
				}
			},
			Literal::Object(btree_map) => {
				for v in btree_map.values(){
					this.visit_kind(v)?;
				}
			},
			Literal::DiscriminatedObject(_, btree_maps) => {
				for v in btree_maps.iter(){
					for v in v.values(){
						this.visit_kind(v)?;
					}
				}
			},
		}
		Ok(())

	}

	fn visit_mock(_this, _value: &Mock) {
		Ok(())
	}

	fn visit_number(_this, _value: &Number) {
		Ok(())
	}

	fn visit_geometry(_this, _value: &Geometry) {
		Ok(())
	}

	fn visit_array(this, array: &Array) {
		for v in array.0.iter(){
			this.visit_value(v)?;
		}
		Ok(())
	}

	fn visit_object(this, obj: &Object) {
		for v in obj.0.values(){
			this.visit_value(v)?;
		}
		Ok(())
	}

	fn visit_thing(this, t: &Thing) {
		this.visit_id(&t.id)
	}

	fn visit_id(this, id: &Id) {
		match id {
			Id::Number(_) => {},
			Id::String(_) => {},
			Id::Uuid(_) => {},
			Id::Array(array) => {this.visit_array(array)?;},
			Id::Object(object) => {this.visit_object(object)?;},
			Id::Generate(_) => {},
			Id::Range(id_range) => {
				this.visit_id_range(id_range)?;
			},
		}
		Ok(())
	}

	fn visit_id_range(this, id_range: &IdRange)  {
		match id_range.beg{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_id(x)?;
			},
			Bound::Unbounded => {},
		}
		match id_range.end{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_id(x)?;
			},
			Bound::Unbounded => {},
		}
		Ok(())
	}

	fn visit_param(_this, _param: &Param) {
		Ok(())
	}

	fn visit_idiom(this, idiom: &Idiom) {
		for p in idiom.0.iter(){
			this.visit_part(p)?;
		}
		Ok(())
	}

	fn visit_part(this, part: &Part) {
		match part{
			Part::All |
				Part::Flatten |
				Part::Last |
				Part::First |
				Part::Optional |
				Part::Field(_) |
				Part::Doc |
				Part::RepeatRecurse  => {}
			Part::Index(number) => {
				this.visit_number(number)?;
			},
			Part::Where(value) | Part::Value(value) | Part::Start(value) => {
				this.visit_value(value)?;
			},
			Part::Graph(graph) => {
				this.visit_graph(graph)?;
			},
			Part::Method(_, values) => {
				for v in values {
					this.visit_value(v)?;
				}
			},
			Part::Destructure(destructure_parts) => {
				for p in destructure_parts.iter(){
					this.visit_destructure_part(p)?;
				}
			},
			Part::Recurse(recurse, idiom, recurse_instruction) => {
				this.visit_recurse(recurse)?;
				if let Some(idiom) = idiom.as_ref(){
					this.visit_idiom(idiom)?;
				}
				if let Some(instr) = recurse_instruction.as_ref(){
					this.visit_recurse_instruction(instr)?;
				}
			},
		}
		Ok(())
	}

	fn visit_recurse_instruction(this, r: &RecurseInstruction){
		match r {
			RecurseInstruction::Path { ..} |
				RecurseInstruction::Collect { ..} => {}
			RecurseInstruction::Shortest { expects, .. } => {
				this.visit_value(expects)?;
			},
		}
		Ok(())
	}

	fn visit_recurse(_this, _r: &Recurse){
		Ok(())
	}

	fn visit_destructure_part(this, p: &DestructurePart) {
		match p {
			DestructurePart::All(_) |
				DestructurePart::Field(_) => {},
				DestructurePart::Aliased(_, idiom) => {
					this.visit_idiom(idiom)?;
				},
				DestructurePart::Destructure(_, destructure_parts) => {
					for p in destructure_parts{
						this.visit_destructure_part(p)?;
					}
				},
		}
		Ok(())
	}

	fn visit_graph(this, graph: &Graph) {
		if let Some(expr) = graph.expr.as_ref(){
			this.visit_fields(expr)?;
		}

		for s in graph.what.0.iter(){
			this.visit_graph_subject(s)?;
		}

		if let Some(c) = graph.cond.as_ref(){
			this.visit_value(&c.0)?;
		}

		if let Some(s) = graph.split.as_ref(){
			for s in s.0.iter(){
				this.visit_idiom(&s.0)?;
			}
		}

		if let Some(groups) = graph.group.as_ref() {
			for g in groups.0.iter(){
				this.visit_idiom(&g.0)?;
			}
		}

		if let Some(order) = graph.order.as_ref(){
			this.visit_ordering(order)?;
		}

		if let Some(limit) = graph.limit.as_ref(){
			this.visit_value(&limit.0)?;
		}

		if let Some(start) = graph.start.as_ref(){
			this.visit_value(&start.0)?;
		}

		if let Some(alias) = graph.alias.as_ref(){
			this.visit_idiom(alias)?;
		}

		Ok(())
	}


	fn visit_ordering(this, ordering: &Ordering){
		match ordering{
			Ordering::Random => {},
			Ordering::Order(order_list) => {
				for o in order_list.0.iter(){
					this.visit_idiom(&o.value)?;
				}
			},
		}
		Ok(())
	}

	fn visit_fields(this, fields: &Fields) {
		for f in fields.0.iter() {
			this.visit_field(f)?;
		}
		Ok(())
	}

	fn visit_field(this, field: &Field){
		match field {
			Field::All => {},
			Field::Single { expr, alias } => {
				this.visit_value(expr)?;
				if let Some(alias) = alias.as_ref(){
					this.visit_idiom(alias)?;
				}
			},
		}
		Ok(())
	}

	fn visit_graph_subject(this, subject: &GraphSubject){
		match subject{
			GraphSubject::Table(_) => {},
			GraphSubject::Range(_, id_range) => {
				this.visit_id_range(id_range)?;
			},

		}
		Ok(())
	}

	fn visit_api_definition(this, def: &ApiDefinition){
		for a in def.actions.iter(){
			this.visit_api_action(a)?;
		}
		if let Some(v) = &def.fallback{
			this.visit_value(v)?;
		}
		if let Some(c) = &def.config{
			this.visit_api_config(c)?;
		}

		Ok(())
	}
}
