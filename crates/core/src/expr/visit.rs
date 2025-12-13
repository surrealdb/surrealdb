use std::ops::Bound;

use crate::catalog::{GraphQLConfig, Permission, Permissions, Relation, TableType};
use crate::expr::access_type::{BearerAccess, JwtAccessVerify};
use crate::expr::data::Assignment;
use crate::expr::field::Selector;
use crate::expr::lookup::LookupSubject;
use crate::expr::order::Ordering;
use crate::expr::part::{DestructurePart, Recurse, RecurseInstruction};
use crate::expr::reference::{Reference, ReferenceDeleteStrategy};
use crate::expr::statements::access::{
	AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke, AccessStatementShow, Subject,
};
use crate::expr::statements::alter::{
	AlterDefault, AlterFieldStatement, AlterIndexStatement, AlterKind, AlterSequenceStatement,
	AlterTableStatement,
};
use crate::expr::statements::define::config::ConfigInner;
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::statements::define::config::defaults::DefaultConfig;
use crate::expr::statements::define::{
	ApiAction, DefineBucketStatement, DefineConfigStatement, DefineDefault, DefineSequenceStatement,
};
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::statements::remove::{
	RemoveApiStatement, RemoveBucketStatement, RemoveSequenceStatement,
};
use crate::expr::statements::{
	AccessStatement, AlterStatement, CreateStatement, DefineAccessStatement,
	DefineAnalyzerStatement, DefineApiStatement, DefineDatabaseStatement, DefineEventStatement,
	DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement, DefineModelStatement,
	DefineModuleStatement, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
	DefineTableStatement, DefineUserStatement, DeleteStatement, ForeachStatement, IfelseStatement,
	InfoStatement, InsertStatement, KillStatement, LiveFields, LiveStatement, OptionStatement,
	OutputStatement, RelateStatement, RemoveAccessStatement, RemoveAnalyzerStatement,
	RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement, RemoveFunctionStatement,
	RemoveIndexStatement, RemoveModelStatement, RemoveModuleStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveStatement, RemoveTableStatement, RemoveUserStatement,
	SelectStatement, SetStatement, ShowStatement, SleepStatement, UpdateStatement, UpsertStatement,
	UseStatement,
};
use crate::expr::{
	AccessType, Block, ClosureExpr, Data, Expr, Field, Fields, Function, FunctionCall, Idiom,
	JwtAccess, Kind, KindLiteral, Literal, Lookup, Model, Output, Param, Part, RecordAccess,
	RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit, TopLevelExpr, View,
};

macro_rules! implement_visitor{
	($(fn $name:ident($this:ident, $value:ident: &$ty:ty) {
	    $($t:tt)*
	})*) => {

		/// A trait for types which can be visited with the `expr` visitor
		#[allow(dead_code)]
		pub(crate) trait Visit<V: Visitor>{
			fn visit(&self, v: &mut V) -> Result<(), V::Error>;
		}

		/// A visitor which can visit types from `expr`
		///
		/// Implementing a visitor is as simple as just implementing the trait and only defining
		/// the error type. All visiting functions have default implementations so to visit a
		/// specific type you just need to implement only the `visit_{type}` function.
		#[allow(dead_code)]
		pub(crate) trait Visitor: Sized {
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

macro_rules! implement_visitor_mut{
	($(fn $name:ident($this:ident, $value:ident: &mut $ty:ty) {
	    $($t:tt)*
	})*) => {

		/// A trait for types which can be visited with mutable access with the `expr` visitor
		#[allow(dead_code)]
		pub(crate) trait VisitMut<V: MutVisitor>{
			fn visit_mut(&mut self, v: &mut V) -> Result<(), V::Error>;
		}

		/// A mutable visitor which can visit types from `expr`
		///
		/// Implementing a visitor is as simple as just implementing the trait and only defining
		/// the error type. All visiting functions have default implementations so to visit a
		/// specific type you just need to implement only the `visit_mut_{type}` function.
		#[allow(dead_code)]
		pub(crate) trait MutVisitor: Sized {
			type Error;

			$(
				fn $name(&mut self, $value: &mut $ty) -> Result<(), Self::Error>{
					$value.visit_mut(self)
				}
			)*
		}

		$(
			impl<V: MutVisitor> VisitMut<V> for $ty {
				#[allow(unused_variables)]
				fn visit_mut(&mut self, $this: &mut V) -> Result<(), V::Error>{
					let $value = self;
					$($t)*
				}
			}
		)*
	};
}

// This macro implements the visitor for the expr types.
// When modifying also make sure to modify the mutable visitor implementation below it.
implement_visitor! {
	fn visit_top_level_expr(this, value: &TopLevelExpr) {
		match value {
			TopLevelExpr::Begin => {},
			TopLevelExpr::Cancel => {},
			TopLevelExpr::Commit => {},
			TopLevelExpr::Access(s) => {this.visit_access(s)? },
			TopLevelExpr::Kill(s) => {this.visit_kill(s)?; },
			TopLevelExpr::Live(s) => {this.visit_live(s)?; },
			TopLevelExpr::Option(s) =>{ this.visit_option(s)?; },
			TopLevelExpr::Use(s) => {this.visit_use(s)?; },
			TopLevelExpr::Show(s) => {this.visit_show(s)?; },
			TopLevelExpr::Expr(e) => {this.visit_expr(e)?; },
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
			Subject::Record(r) => {
				this.visit_record_id(r)?;
			},
			Subject::User(_) => { }
		}
		Ok(())
	}

	fn visit_access_show(this, a: &AccessStatementShow){
		if let Some(c) = a.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		Ok(())
	}
	fn visit_access_revoke(this, a: &AccessStatementRevoke){
		if let Some(c) = a.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		Ok(())
	}
	fn visit_access_purge(this, a: &AccessStatementPurge){
		Ok(())
	}


	fn visit_kill(this, i: &KillStatement){
		this.visit_expr(&i.id)?;
		Ok(())
	}

	fn visit_live(this, l: &LiveStatement){
		match &l.fields{
			LiveFields::Diff => {},
			LiveFields::Select(x) => {
				this.visit_fields(x)?;
			}
		}
		this.visit_expr(&l.what)?;
		if let Some(c) = l.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		if let Some(f) = l.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_expr(&f.0)?;
			}
		}

		Ok(())
	}

	fn visit_option(this, i: &OptionStatement){
		Ok(())
	}

	fn visit_show(this, s: &ShowStatement){
		Ok(())
	}

	fn visit_expr(this, s: &Expr){
		match s {
			Expr::Literal(literal) => {
				this.visit_literal(literal)?;
			}
			Expr::Param(p) => {
				this.visit_param(p)?;
			}
			Expr::Table(_) |
			Expr::Mock(_) |
			Expr::Constant(_) |
			Expr::Break |
			Expr::Continue => {},
			Expr::Idiom(idiom) => {
				this.visit_idiom(idiom)?;
			}
			Expr::Block(block) => {
				this.visit_block(block)?;
			}
			Expr::Prefix { expr, .. } => {
				this.visit_expr(expr)?;
			},
			Expr::Postfix { expr, .. } => {
				this.visit_expr(expr)?;
			},
			Expr::Binary { left, right, .. } => {
				this.visit_expr(left)?;
				this.visit_expr(right)?;
			},
			Expr::FunctionCall(f) => {
				this.visit_function_call(f)?;
			},
			Expr::Closure(c) => {
				this.visit_closure(c)?;
			},
			Expr::Return(o) => {
				this.visit_output_stmt(o)?;
			},
			Expr::Throw(e) => {
				this.visit_expr(e)?;
			},
			Expr::IfElse(s) => {
				this.visit_if_else(s)?;
			},
			Expr::Select(s) => { this.visit_select(s)?; },
			Expr::Create(s) => { this.visit_create(s)?; },
			Expr::Update(s) => { this.visit_update(s)?; },
			Expr::Upsert(s) => { this.visit_upsert(s)?; },
			Expr::Delete(s) => { this.visit_delete(s)?; },
			Expr::Relate(s) => { this.visit_relate(s)?; },
			Expr::Insert(s) => { this.visit_insert(s)?; },
			Expr::Define(s) => { this.visit_define(s)?; },
			Expr::Remove(s) => { this.visit_remove(s)?; },
			Expr::Rebuild(s) => {
				this.visit_rebuild(s)?;
			},
			Expr::Alter(s) => {
				this.visit_alter(s)?;
			},
			Expr::Info(s) => {
				this.visit_info(s)?;
			},
			Expr::Foreach(s) => {
				this.visit_foreach(s)?;
			},
			Expr::Let(s) => {
				this.visit_set(s)?;
			},
			Expr::Sleep(s) => {
				this.visit_sleep(s)?;
			},
		}

		Ok(())
	}

	fn visit_literal(this, s: &Literal){
		match s {
			Literal::None |
			Literal::Null |
			Literal::UnboundedRange |
			Literal::Bool(_) |
			Literal::Float(_) |
			Literal::Integer(_) |
			Literal::Decimal(_) |
			Literal::String(_) |
			Literal::Bytes(_) |
			Literal::Regex(_) |
			Literal::Datetime(_) |
			Literal::Duration(_) |
			Literal::Uuid(_) |
			Literal::File(_) |
			Literal::Geometry(_) => {},
			Literal::RecordId(r) => {
				this.visit_record_id(r)?;
			},
			Literal::Array(exprs) => {
				for expr in exprs.iter() {
					this.visit_expr(expr)?;
				}
			},
			Literal::Set(exprs) => {
				for expr in exprs.iter() {
					this.visit_expr(expr)?;
				}
			},
			Literal::Object(items) => {
				for entry in items{
					this.visit_expr(&entry.value)?
				}
			},
		}

		Ok(())
	}

	fn visit_alter(this, a: &AlterStatement){
		match a {
			AlterStatement::Table(a)=>{ this.visit_alter_table(a)?; },
			AlterStatement::Index(a) => { this.visit_alter_index(a)?; },
			AlterStatement::Sequence(a) => { this.visit_alter_sequence(a)?; },
			AlterStatement::Field(a) => { this.visit_alter_field(a)?; },
		}
		Ok(())
	}

	fn visit_alter_table(this, a: &AlterTableStatement){
		if let Some(p) = a.permissions.as_ref(){
			this.visit_permissions(p)?;
		}
		Ok(())
	}

	fn visit_alter_index(this, a: &AlterIndexStatement){
		Ok(())
	}

	fn visit_alter_sequence(this, a: &AlterSequenceStatement){
		Ok(())
	}

	fn visit_alter_field(this, a: &AlterFieldStatement){
		this.visit_idiom(&a.name)?;

		match a.value {
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref x) => this.visit_expr(x)?,
		}

		match a.assert{
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref x) => this.visit_expr(x)?,
		}

		match a.default{
			AlterDefault::None |
			AlterDefault::Drop => {},
			AlterDefault::Set(ref x) => this.visit_expr(x)?,
			AlterDefault::Always(ref x) => this.visit_expr(x)?,
		}

		if let Some(p) = a.permissions.as_ref(){
			this.visit_permissions(p)?;
		}

		match a.reference{
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref x) => this.visit_reference(x)?,
		}

		Ok(())
	}

	fn visit_upsert(this, u: &UpsertStatement){
		for v in u.what.iter(){
			this.visit_expr(v)?;
		}
		if let Some(d) = u.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = u.output.as_ref(){
			this.visit_output(o)?;
		}
		if let Some(c) = u.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		Ok(())

	}

	fn visit_rebuild(this, r: &RebuildStatement){
		Ok(())
	}

	fn visit_use(this, t: &UseStatement){
		Ok(())
	}

	fn visit_update(this, u: &UpdateStatement){
		for v in u.what.iter(){
			this.visit_expr(v)?;
		}
		if let Some(d) = u.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = u.output.as_ref(){
			this.visit_output(o)?;
		}
		if let Some(c) = u.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		Ok(())

	}

	fn visit_sleep(this, s: &SleepStatement){
		Ok(())
	}

	fn visit_set(this, s: &SetStatement){
		if let Some(k) = s.kind.as_ref(){
			this.visit_kind(k)?;
		}
		this.visit_expr(&s.what)?;
		Ok(())
	}

	fn visit_select(this, s: &SelectStatement){
		this.visit_fields(&s.expr)?;
		for o in s.omit.iter(){
			this.visit_expr(o)?;
		}
		for v in s.what.iter(){
			this.visit_expr(v)?;
		}
		if let Some(c) = s.cond.as_ref(){
			this.visit_expr(&c.0)?;
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
			this.visit_expr(&l.0)?;
		}
		if let Some(f) = s.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_expr(&f.0)?;
			}
		}
		this.visit_expr(&s.version)?;

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
			RemoveStatement::Api(r) => {
				this.visit_remove_api(r)?;
			},
			RemoveStatement::Bucket(r) => {
				this.visit_remove_bucket(r)?;
			},
			RemoveStatement::Sequence(r) => {
				this.visit_remove_sequence(r)?;
			},
			RemoveStatement::Module(r) => {
				this.visit_remove_module(r)?;
			},
		}
		Ok(())
	}

	fn visit_remove_namespace(this, r: &RemoveNamespaceStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_database(this, r: &RemoveDatabaseStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_function(this, r: &RemoveFunctionStatement){
		Ok(())
	}

	fn visit_remove_module(this, r: &RemoveModuleStatement){
		Ok(())
	}

	fn visit_remove_analyzer(this, r: &RemoveAnalyzerStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_access(this, r: &RemoveAccessStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_param(this, r: &RemoveParamStatement){
		Ok(())
	}

	fn visit_remove_table(this, r: &RemoveTableStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_event(this, r: &RemoveEventStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_field(this, r: &RemoveFieldStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_index(this, r: &RemoveIndexStatement){
		this.visit_expr(&r.name)?;
		this.visit_expr(&r.what)?;
		Ok(())
	}

	fn visit_remove_user(this, r: &RemoveUserStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_model(this, r: &RemoveModelStatement){
		Ok(())
	}


	fn visit_remove_api(this, r: &RemoveApiStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_bucket(this, r: &RemoveBucketStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_remove_sequence(this, r: &RemoveSequenceStatement){
		this.visit_expr(&r.name)?;
		Ok(())
	}

	fn visit_relate(this, o: &RelateStatement){
		this.visit_expr(&o.through)?;
		this.visit_expr(&o.from)?;
		this.visit_expr(&o.to)?;
		if let Some(d) = o.data.as_ref(){
			this.visit_data(d)?;
		}
		if let Some(o) = o.output.as_ref(){
			this.visit_output(o)?;
		}
		this.visit_expr(&o.timeout)?;
		Ok(())
	}

	fn visit_output_stmt(this, o: &OutputStatement){
		this.visit_expr(&o.what)?;
		if let Some(f) = o.fetch.as_ref(){
			for f in f.0.iter(){
				this.visit_expr(&f.0)?;
			}
		}
		Ok(())
	}

	fn visit_insert(this, i: &InsertStatement){
		this.visit_expr(&i.into)?;
		this.visit_data(&i.data)?;
		if let Some(update) = i.update.as_ref(){
			this.visit_data(update)?;
		}
		if let Some(o) = i.output.as_ref(){
			this.visit_output(o)?;
		}
		this.visit_expr(&i.timeout)?;
		this.visit_expr(&i.version)?;
		Ok(())
	}

	fn visit_info(this, i: &InfoStatement){
		match i{
			InfoStatement::Root(_) |
			InfoStatement::Ns(_) => {}
			InfoStatement::Db(_, expr) => {
				if let Some(e) = expr.as_ref(){
					this.visit_expr(e)?;
				}
			},
			InfoStatement::Tb(expr, _, expr1) => {
				this.visit_expr(expr)?;
				if let Some(e) = expr1.as_ref(){
					this.visit_expr(e)?;
				}
			},
			InfoStatement::User(expr, base, _) => {
				this.visit_expr(expr)?;
			},
			InfoStatement::Index(expr, expr1, _) => {
				this.visit_expr(expr)?;
				this.visit_expr(expr1)?;
			},
		}
		Ok(())
	}

	fn visit_if_else(this, i: &IfelseStatement){
		for e in i.exprs.iter(){
			this.visit_expr(&e.0)?;
			this.visit_expr(&e.1)?;
		}
		if let Some(x) = i.close.as_ref(){
			this.visit_expr(x)?;
		}
		Ok(())
	}

	fn visit_foreach(this, f: &ForeachStatement){
		this.visit_param(&f.param)?;
		this.visit_expr(&f.range)?;
		this.visit_block(&f.block)?;
		Ok(())
	}

	fn visit_delete(this, d: &DeleteStatement){
		for v in d.what.iter(){
			this.visit_expr(v)?;
		}
		if let Some(c) = d.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}
		if let Some(o) = d.output.as_ref(){
			this.visit_output(o)?;
		}
		this.visit_expr(&d.timeout)?;
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
			DefineStatement::Bucket(d) => {
				this.visit_define_bucket(d)?;
			},
			DefineStatement::Sequence(d) => {
				this.visit_define_sequence(d)?;
			},
			DefineStatement::Module(d) => {
				this.visit_define_module(d)?;
			},
		}
		Ok(())
	}

	fn visit_define_sequence(this, d: &DefineSequenceStatement) {
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.batch)?;
		this.visit_expr(&d.start)?;
		this.visit_expr(&d.timeout)?;
		Ok(())
	}

	fn visit_define_bucket(this, d: &DefineBucketStatement) {
		this.visit_expr(&d.name)?;
		if let Some(expr) = d.backend.as_ref(){
			this.visit_expr(expr)?;
		}
		this.visit_permission(&d.permissions)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_api(this, d: &DefineApiStatement) {
		this.visit_expr(&d.path)?;
		for act in d.actions.iter(){
			this.visit_api_action(act)?;
		}
		if let Some(f) = d.fallback.as_ref(){
			this.visit_expr(f)?;
		}
		this.visit_api_config(&d.config)?;
		this.visit_expr(&d.comment)?;

		Ok(())
	}

	fn visit_api_action(this, a: &ApiAction){
		this.visit_expr(&a.action)?;
		this.visit_api_config(&a.config)?;
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
			ConfigInner::Default(default_config) => {
				this.visit_default_config(default_config)?;
			},
		}
		Ok(())
	}

	fn visit_graphql_config(this, d: &GraphQLConfig){
		Ok(())
	}


	fn visit_api_config(this, d: &ApiConfig){
		for m in d.middleware.iter(){
			for v in m.args.iter(){
				this.visit_expr(v)?;
			}
		}
		this.visit_permission(&d.permissions)?;
		Ok(())
	}

	fn visit_default_config(this, d: &DefaultConfig) {
		this.visit_expr(&d.namespace)?;
		this.visit_expr(&d.database)?;
		Ok(())
	}

	fn visit_define_access(this, d: &DefineAccessStatement) {
		this.visit_expr(&d.name)?;
		this.visit_access_type(&d.access_type)?;
		if let Some(v) = d.authenticate.as_ref(){
			this.visit_expr(v)?;
		}

		this.visit_expr(&d.duration.grant)?;
		this.visit_expr(&d.duration.token)?;
		this.visit_expr(&d.duration.session)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_access_type(this, a: &AccessType) {
		match a {
			AccessType::Record(r) => { this.visit_record_access(r)?; },
			AccessType::Jwt(j) => { this.visit_jwt_access(j)?; },
			AccessType::Bearer(b) => { this.visit_bearer_access(b)?; },
		}
		Ok(())
	}

	fn visit_record_access(this, r: &RecordAccess){
		if let Some(e) = r.signup.as_ref(){
			this.visit_expr(e)?
		}
		if let Some(e) = r.signin.as_ref(){
			this.visit_expr(e)?
		}

		this.visit_jwt_access(&r.jwt)?;
		if let Some(b) = r.bearer.as_ref(){
			this.visit_bearer_access(b)?;
		}
		Ok(())
	}

	fn visit_jwt_access(this, j: &JwtAccess){
		match j.verify{
			JwtAccessVerify::Key(ref k) => {
				this.visit_expr(&k.key)?

			},
			JwtAccessVerify::Jwks(ref j) => {
				this.visit_expr(&j.url)?
			},
		}

		if let Some(i) = j.issue.as_ref(){
			this.visit_expr(&i.key)?
		}

		Ok(())
	}

	fn visit_bearer_access(this, r: &BearerAccess){
		this.visit_jwt_access(&r.jwt)?;
		Ok(())
	}

	fn visit_define_model(this, d: &DefineModelStatement) {
		this.visit_permission(&d.permissions)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_module(this, d: &DefineModuleStatement) {
		this.visit_permission(&d.permissions)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_user(this, d: &DefineUserStatement) {
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.duration.token)?;
		this.visit_expr(&d.duration.session)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_index(this, d: &DefineIndexStatement) {
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.what)?;
		for c in d.cols.iter(){
			this.visit_expr(c)?;
		}
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_field(this, d: &DefineFieldStatement) {
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.what)?;
		if let Some(k) = d.field_kind.as_ref(){
			this.visit_kind(k)?;
		}
		if let Some(v) = d.value.as_ref(){
			this.visit_expr(v)?;
		}
		if let Some(v) = d.assert.as_ref(){
			this.visit_expr(v)?;
		}
		if let Some(v) = d.computed.as_ref(){
			this.visit_expr(v)?;
		}
		match d.default{
			DefineDefault::None => {},
			DefineDefault::Always(ref expr) |
			DefineDefault::Set(ref expr) => this.visit_expr(expr)?,
		}
		this.visit_permissions(&d.permissions)?;
		if let Some(r) = d.reference.as_ref(){
			this.visit_reference(r)?;
		}
		this.visit_expr(&d.comment)?;
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
				this.visit_expr(value)?;
			},

		}
		Ok(())
	}


	fn visit_define_event(this, d: &DefineEventStatement){
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.target_table)?;
		this.visit_expr(&d.when)?;
		for v in d.then.iter(){
			this.visit_expr(v)?;
		}
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_table(this, d: &DefineTableStatement){
		this.visit_expr(&d.name)?;
		if let Some(v) = d.view.as_ref(){
			this.visit_view(v)?;
		}
		this.visit_permissions(&d.permissions)?;
		this.visit_expr(&d.comment)?;
		this.visit_table_type(&d.table_type)?;

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
			this.visit_expr(&c.0)?;
		}

		if let Some(g) = v.group.as_ref(){
			for g in g.0.iter(){
				this.visit_idiom(&g.0)?
			}
		}

		Ok(())
	}

	fn visit_define_param(this, d: &DefineParamStatement){
		this.visit_expr(&d.value)?;
		this.visit_expr(&d.comment)?;
		this.visit_permission(&d.permissions)?;
		Ok(())
	}

	fn visit_define_analyzer(this, d: &DefineAnalyzerStatement){
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_function(this, d: &DefineFunctionStatement){
		for (_, k) in d.args.iter(){
			this.visit_kind(k)?;
		}
		this.visit_block(&d.block)?;
		this.visit_permission(&d.permissions)?;
		this.visit_expr(&d.comment)?;
		if let Some(k) = d.returns.as_ref(){
			this.visit_kind(k)?;
		}
		Ok(())
	}

	fn visit_permission(this, p: &Permission){
		match p {
			Permission::None |
				Permission::Full => {},
			Permission::Specific(e) => {
				this.visit_expr(e)?;
			},
		}
		Ok(())
	}

	fn visit_define_database(this,  d: &DefineDatabaseStatement){
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_define_namespace(this,  d: &DefineNamespaceStatement){
		this.visit_expr(&d.name)?;
		this.visit_expr(&d.comment)?;
		Ok(())
	}

	fn visit_create(this, c: &CreateStatement){
		for w in c.what.iter(){
			this.visit_expr(w)?;
		}

		if let Some(data) = c.data.as_ref(){
			this.visit_data(data)?;
		}

		if let Some(output) = c.output.as_ref(){
			this.visit_output(output)?;
		}

		this.visit_expr(&c.timeout)?;

		this.visit_expr(&c.version)?;

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
			Data::UpdateExpression(assign) |
				Data::SetExpression(assign) => {
					for a in assign{
						this.visit_assignment(a)?;
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
					this.visit_expr(value)?;
				},
			Data::ValuesExpression(items) => {
				for (i,v) in items.iter().flat_map(|x| x.iter()){
					this.visit_idiom(i)?;
					this.visit_expr(v)?;
				}
			},
		}
		Ok(())
	}

	fn visit_assignment(this, a: &Assignment){
		this.visit_idiom(&a.place)?;
		this.visit_expr(&a.value)?;
		Ok(())
	}

	fn visit_closure(this, c: &ClosureExpr){
		for (_,k) in c.args.iter(){
			this.visit_kind(k)?;
		}
		if let Some(k) = c.returns.as_ref(){
			this.visit_kind(k)?;
		}
		this.visit_expr(&c.body)?;
		Ok(())
	}

	fn visit_model(this, m: &Model) {
		Ok(())
	}


	fn visit_function_call(this, f: &FunctionCall){
		this.visit_function(&f.receiver)?;
		for a in f.arguments.iter(){
			this.visit_expr(a)?;
		}
		Ok(())
	}

	fn visit_function(this, f: &Function){
		Ok(())
	}


	fn visit_block(this, value: &Block){
		for v in value.0.iter(){
			this.visit_expr(v)?;
		}
		Ok(())

	}

	fn visit_kind(this, kind: &Kind){
		match kind {
			Kind::None |
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
				Kind::String |
				Kind::Uuid |
				Kind::Regex |
				Kind::Range |
				Kind::Record(_) |
				Kind::Geometry(_) |
				Kind::Table(_) |
				Kind::File(_) => {}
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
		}
		Ok(())
	}

	fn visit_kind_literal(this, k: &KindLiteral){
		match k{
			KindLiteral::String(_) => {},
			KindLiteral::Bool(_) => {},
			KindLiteral::Duration(_) => {},
			KindLiteral::Float(_) => {},
			KindLiteral::Integer(_) => {},
			KindLiteral::Decimal(_) => {},
			KindLiteral::Array(kinds) => {
				for k in kinds.iter(){
					this.visit_kind(k)?;
				}
			},
			KindLiteral::Object(btree_map) => {
				for v in btree_map.values(){
					this.visit_kind(v)?;
				}
			},
		}
		Ok(())

	}

	fn visit_record_id(this, t: &RecordIdLit) {
		this.visit_record_id_key(&t.key)
	}

	fn visit_record_id_key(this, id: &RecordIdKeyLit) {
		match id {
			RecordIdKeyLit::Number(_) => {},
			RecordIdKeyLit::String(_) => {},
			RecordIdKeyLit::Uuid(_) => {},
			RecordIdKeyLit::Array(array) => {
				for e in array.iter(){
					this.visit_expr(e)?;
				}
			},
			RecordIdKeyLit::Object(object) => {
				for e in object.iter(){
					this.visit_expr(&e.value)?;
				}
			},
			RecordIdKeyLit::Generate(_) => {},
			RecordIdKeyLit::Range(id_range) => {
				this.visit_record_id_key_range(id_range)?;
			},
		}
		Ok(())
	}

	fn visit_record_id_key_range(this, id_range: &RecordIdKeyRangeLit)  {
		match id_range.start{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_record_id_key(x)?;
			},
			Bound::Unbounded => {},
		}
		match id_range.end{
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				this.visit_record_id_key(x)?;
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
			Part::Where(value) | Part::Value(value) | Part::Start(value) => {
				this.visit_expr(value)?;
			},
			Part::Method(_, values) => {
				for v in values {
					this.visit_expr(v)?;
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
			Part::Lookup(l) => {
				this.visit_lookup(l)?;
			}
		}
		Ok(())
	}

	fn visit_lookup(this, l: &Lookup){
		if let Some(f)  = l.expr.as_ref(){
			this.visit_fields(f)?;
		}
		for w in l.what.iter(){
			this.visit_lookup_subject(w)?;
		}

		if let Some(c) = l.cond.as_ref(){
			this.visit_expr(&c.0)?;
		}

		if let Some(s) = l.split.as_ref(){
			for s in s.0.iter(){
				this.visit_idiom(&s.0)?;
			}
		}

		if let Some(groups) = l.group.as_ref() {
			for g in groups.0.iter(){
				this.visit_idiom(&g.0)?;
			}
		}

		if let Some(order) = l.order.as_ref(){
			this.visit_ordering(order)?;
		}

		if let Some(limit) = l.limit.as_ref(){
			this.visit_expr(&limit.0)?;
		}

		if let Some(start) = l.start.as_ref(){
			this.visit_expr(&start.0)?;
		}

		if let Some(alias) = l.alias.as_ref(){
			this.visit_idiom(alias)?;
		}

		Ok(())

	}

	fn visit_recurse_instruction(this, r: &RecurseInstruction){
		match r {
			RecurseInstruction::Path { ..} |
				RecurseInstruction::Collect { ..} => {}
			RecurseInstruction::Shortest { expects, .. } => {
				this.visit_expr(expects)?;
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
		match fields {
			Fields::Value(field) => {
				this.visit_selector(field)?;
			},
			Fields::Select(fields) => {
				for f in fields.iter(){
					this.visit_field(f)?;
				}
			},
		}

		Ok(())
	}

	fn visit_field(this, field: &Field){
		match field {
			Field::All => {},
			Field::Single(s) => {
				this.visit_selector(s)?;
			},
		}
		Ok(())
	}

	fn visit_selector(this, selector: &Selector){
		this.visit_expr(&selector.expr)?;
		if let Some(alias) = &selector.alias {
			this.visit_idiom(alias)?;
		}
		Ok(())
	}

	fn visit_lookup_subject(this, subject: &LookupSubject){
		match subject{
			LookupSubject::Table { .. } => {},
			LookupSubject::Range { range, .. } => {
				this.visit_record_id_key_range(range)?;
			},

		}
		Ok(())
	}
}

implement_visitor_mut! {
	fn visit_mut_top_level_expr(this, value: &mut TopLevelExpr) {
		match value {
			TopLevelExpr::Begin => {},
			TopLevelExpr::Cancel => {},
			TopLevelExpr::Commit => {},
			TopLevelExpr::Access(s) => {this.visit_mut_access(s)? },
			TopLevelExpr::Kill(s) => {this.visit_mut_kill(s)?; },
			TopLevelExpr::Live(s) => {this.visit_mut_live(s)?; },
			TopLevelExpr::Option(s) =>{ this.visit_mut_option(s)?; },
			TopLevelExpr::Use(s) => {this.visit_mut_use(s)?; },
			TopLevelExpr::Show(s) => {this.visit_mut_show(s)?; },
			TopLevelExpr::Expr(e) => {this.visit_mut_expr(e)?; },
		}
		Ok(())
	}

	fn visit_mut_access(this, a: &mut AccessStatement){
		match a {
			AccessStatement::Grant(a) => {
				this.visit_mut_access_grant(a)?;
			},
			AccessStatement::Show(a) => {
				this.visit_mut_access_show(a)?;
			},
			AccessStatement::Revoke(a) => {
				this.visit_mut_access_revoke(a)?;
			},
			AccessStatement::Purge(a) => {
				this.visit_mut_access_purge(a)?;
			},
		}
		Ok(())
	}

	fn visit_mut_access_grant(this, a: &mut AccessStatementGrant){
		this.visit_mut_access_subject(&mut a.subject)?;
		Ok(())
	}

	fn visit_mut_access_subject(this, s: &mut Subject){
		match s{
			Subject::Record(r) => {
				this.visit_mut_record_id(r)?;
			},
			Subject::User(_) => { }
		}
		Ok(())
	}

	fn visit_mut_access_show(this, a: &mut AccessStatementShow){
		if let Some(c) = a.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		Ok(())
	}
	fn visit_mut_access_revoke(this, a: &mut AccessStatementRevoke){
		if let Some(c) = a.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		Ok(())
	}
	fn visit_mut_access_purge(this, a: &mut AccessStatementPurge){
		Ok(())
	}


	fn visit_mut_kill(this, i: &mut KillStatement){
		this.visit_mut_expr(&mut i.id)?;
		Ok(())
	}

	fn visit_mut_live(this, l: &mut LiveStatement){
		match &mut l.fields{
			LiveFields::Diff => {},
			LiveFields::Select(x) => {
				this.visit_mut_fields(x)?;
			}
		}
		this.visit_mut_expr(&mut l.what)?;
		if let Some(c) = l.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		if let Some(f) = l.fetch.as_mut(){
			for f in f.0.iter_mut(){
				this.visit_mut_expr(&mut f.0)?;
			}
		}

		Ok(())
	}

	fn visit_mut_option(this, i: &mut OptionStatement){
		Ok(())
	}

	fn visit_mut_show(this, s: &mut ShowStatement){
		Ok(())
	}

	fn visit_mut_expr(this, s: &mut Expr){
		match s {
			Expr::Literal(literal) => {
				this.visit_mut_liter_mutal(literal)?;
			}
			Expr::Param(p) => {
				this.visit_mut_param(p)?;
			}
			Expr::Table(_) |
			Expr::Mock(_) |
			Expr::Constant(_) |
			Expr::Break |
			Expr::Continue => {},
			Expr::Idiom(idiom) => {
				this.visit_mut_idiom(idiom)?;
			}
			Expr::Block(block) => {
				this.visit_mut_block(block)?;
			}
			Expr::Prefix { expr, .. } => {
				this.visit_mut_expr(expr)?;
			},
			Expr::Postfix { expr, .. } => {
				this.visit_mut_expr(expr)?;
			},
			Expr::Binary { left, right, .. } => {
				this.visit_mut_expr(left)?;
				this.visit_mut_expr(right)?;
			},
			Expr::FunctionCall(f) => {
				this.visit_mut_function_call(f)?;
			},
			Expr::Closure(c) => {
				this.visit_mut_closure(c)?;
			},
			Expr::Return(o) => {
				this.visit_mut_output_stmt(o)?;
			},
			Expr::Throw(e) => {
				this.visit_mut_expr(e)?;
			},
			Expr::IfElse(s) => {
				this.visit_mut_if_else(s)?;
			},
			Expr::Select(s) => { this.visit_mut_select(s)?; },
			Expr::Create(s) => { this.visit_mut_create(s)?; },
			Expr::Update(s) => { this.visit_mut_update(s)?; },
			Expr::Upsert(s) => { this.visit_mut_upsert(s)?; },
			Expr::Delete(s) => { this.visit_mut_delete(s)?; },
			Expr::Relate(s) => { this.visit_mut_relate(s)?; },
			Expr::Insert(s) => { this.visit_mut_insert(s)?; },
			Expr::Define(s) => { this.visit_mut_define(s)?; },
			Expr::Remove(s) => { this.visit_mut_remove(s)?; },
			Expr::Rebuild(s) => {
				this.visit_mut_rebuild(s)?;
			},
			Expr::Alter(s) => {
				this.visit_mut_alter(s)?;
			},
			Expr::Info(s) => {
				this.visit_mut_info(s)?;
			},
			Expr::Foreach(s) => {
				this.visit_mut_foreach(s)?;
			},
			Expr::Let(s) => {
				this.visit_mut_set(s)?;
			},
			Expr::Sleep(s) => {
				this.visit_mut_sleep(s)?;
			},
		}

		Ok(())
	}

	fn visit_mut_liter_mutal(this, s: &mut Literal){
		match s {
			Literal::None |
			Literal::Null |
			Literal::UnboundedRange |
			Literal::Bool(_) |
			Literal::Float(_) |
			Literal::Integer(_) |
			Literal::Decimal(_) |
			Literal::String(_) |
			Literal::Bytes(_) |
			Literal::Regex(_) |
			Literal::Datetime(_) |
			Literal::Duration(_) |
			Literal::Uuid(_) |
			Literal::File(_) |
			Literal::Geometry(_) => {},
			Literal::RecordId(r) => {
				this.visit_mut_record_id(r)?;
			},
			Literal::Array(exprs) => {
				for expr in exprs.iter_mut() {
					this.visit_mut_expr(expr)?;
				}
			},
			Literal::Set(exprs) => {
				for expr in exprs.iter_mut() {
					this.visit_mut_expr(expr)?;
				}
			},
			Literal::Object(items) => {
				for entry in items{
					this.visit_mut_expr(&mut entry.value)?
				}
			},
		}

		Ok(())
	}

	fn visit_mut_alter(this, a: &mut AlterStatement){
		match a {
			AlterStatement::Table(a)=>{ this.visit_mut_alter_table(a)?;},
			AlterStatement::Index(a)=>{ this.visit_mut_alter_index(a)?;},
			AlterStatement::Sequence(a) => { this.visit_mut_alter_sequence(a)?; },
			AlterStatement::Field(a) => { this.visit_mut_alter_field(a)?; },
		}
		Ok(())
	}

	fn visit_mut_alter_table(this, a: &mut AlterTableStatement){
		if let Some(p) = a.permissions.as_mut(){
			this.visit_mut_permissions(p)?;
		}
		Ok(())
	}

	fn visit_mut_alter_index(this, a: &mut AlterIndexStatement){
		Ok(())
	}

	fn visit_mut_alter_sequence(this, a: &mut AlterSequenceStatement){
		Ok(())
	}

	fn visit_mut_alter_field(this, a: &mut AlterFieldStatement){
		this.visit_mut_idiom(&mut a.name)?;

		match a.value {
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref mut x) => this.visit_mut_expr(x)?,
		}

		match a.assert{
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref mut x) => this.visit_mut_expr(x)?,
		}

		match a.default{
			AlterDefault::None |
			AlterDefault::Drop => {},
			AlterDefault::Set(ref mut x) => this.visit_mut_expr(x)?,
			AlterDefault::Always(ref mut x) => this.visit_mut_expr(x)?,
		}

		if let Some(p) = a.permissions.as_mut(){
			this.visit_mut_permissions(p)?;
		}

		match a.reference{
			AlterKind::None |
			AlterKind::Drop => {},
			AlterKind::Set(ref mut x) => this.visit_mut_reference(x)?,
		}

		Ok(())
	}

	fn visit_mut_upsert(this, u: &mut UpsertStatement){
		for v in u.what.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(d) = u.data.as_mut(){
			this.visit_mut_data(d)?;
		}
		if let Some(o) = u.output.as_mut(){
			this.visit_mut_output(o)?;
		}
		if let Some(c) = u.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		Ok(())

	}

	fn visit_mut_rebuild(this, r: &mut RebuildStatement){
		Ok(())
	}

	fn visit_mut_use(this, t: &mut UseStatement){
		Ok(())
	}

	fn visit_mut_update(this, u: &mut UpdateStatement){
		for v in u.what.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(d) = u.data.as_mut(){
			this.visit_mut_data(d)?;
		}
		if let Some(o) = u.output.as_mut(){
			this.visit_mut_output(o)?;
		}
		if let Some(c) = u.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		Ok(())

	}

	fn visit_mut_sleep(this, s: &mut SleepStatement){
		Ok(())
	}

	fn visit_mut_set(this, s: &mut SetStatement){
		if let Some(k) = s.kind.as_mut(){
			this.visit_mut_kind(k)?;
		}
		this.visit_mut_expr(&mut s.what)?;
		Ok(())
	}

	fn visit_mut_select(this, s: &mut SelectStatement){
		this.visit_mut_fields(&mut s.expr)?;
		for o in s.omit.iter_mut(){
			this.visit_mut_expr(o)?;
		}
		for v in s.what.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(c) = s.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		if let Some(s) = s.split.as_mut(){
			for s in s.0.iter_mut(){
				this.visit_mut_idiom(&mut s.0)?;
			}
		}
		if let Some(g) = s.group.as_mut(){
			for g in g.0.iter_mut(){
				this.visit_mut_idiom(&mut g.0)?;
			}
		}
		if let Some(o) = s.order.as_mut(){
			this.visit_mut_ordering(o)?;
		}
		if let Some(l) = s.limit.as_mut(){
			this.visit_mut_expr(&mut l.0)?;
		}
		if let Some(f) = s.fetch.as_mut(){
			for f in f.0.iter_mut(){
				this.visit_mut_expr(&mut f.0)?;
			}
		}
		this.visit_mut_expr(&mut s.version)?;

		Ok(())
	}

	fn visit_mut_remove(this, r: &mut RemoveStatement){
		match r {
			RemoveStatement::Namespace(r) => {
				this.visit_mut_remove_namespace(r)?;
			},
			RemoveStatement::Database(r) => {
				this.visit_mut_remove_database(r)?;
			},
			RemoveStatement::Function(r) => {
				this.visit_mut_remove_function(r)?;
			},
			RemoveStatement::Analyzer(r) => {
				this.visit_mut_remove_analyzer(r)?;
			},
			RemoveStatement::Access(r) => {
				this.visit_mut_remove_access(r)?;
			},
			RemoveStatement::Param(r) => {
				this.visit_mut_remove_param(r)?;
			},
			RemoveStatement::Table(r) => {
				this.visit_mut_remove_table(r)?;
			},
			RemoveStatement::Event(r) => {
				this.visit_mut_remove_event(r)?;
			},
			RemoveStatement::Field(r) => {
				this.visit_mut_remove_field(r)?;
			},
			RemoveStatement::Index(r) => {
				this.visit_mut_remove_index(r)?;
			},
			RemoveStatement::User(r) => {
				this.visit_mut_remove_user(r)?;
			},
			RemoveStatement::Model(r) => {
				this.visit_mut_remove_model(r)?;
			},
			RemoveStatement::Api(r) => {
				this.visit_mut_remove_api(r)?;
			},
			RemoveStatement::Bucket(r) => {
				this.visit_mut_remove_bucket(r)?;
			},
			RemoveStatement::Sequence(r) => {
				this.visit_mut_remove_sequence(r)?;
			},
			RemoveStatement::Module(r) => {
				this.visit_mut_remove_module(r)?;
			},
		}
		Ok(())
	}

	fn visit_mut_remove_namespace(this, r: &mut RemoveNamespaceStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_database(this, r: &mut RemoveDatabaseStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_function(this, r: &mut RemoveFunctionStatement){
		Ok(())
	}

	fn visit_mut_remove_module(this, r: &mut RemoveModuleStatement){
		Ok(())
	}

	fn visit_mut_remove_analyzer(this, r: &mut RemoveAnalyzerStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_access(this, r: &mut RemoveAccessStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_param(this, r: &mut RemoveParamStatement){
		Ok(())
	}

	fn visit_mut_remove_table(this, r: &mut RemoveTableStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_event(this, r: &mut RemoveEventStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_field(this, r: &mut RemoveFieldStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_index(this, r: &mut RemoveIndexStatement){
		this.visit_mut_expr(&mut r.name)?;
		this.visit_mut_expr(&mut r.what)?;
		Ok(())
	}

	fn visit_mut_remove_user(this, r: &mut RemoveUserStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_model(this, r: &mut RemoveModelStatement){
		Ok(())
	}


	fn visit_mut_remove_api(this, r: &mut RemoveApiStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_bucket(this, r: &mut RemoveBucketStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_remove_sequence(this, r: &mut RemoveSequenceStatement){
		this.visit_mut_expr(&mut r.name)?;
		Ok(())
	}

	fn visit_mut_relate(this, o: &mut RelateStatement){
		this.visit_mut_expr(&mut o.through)?;
		this.visit_mut_expr(&mut o.from)?;
		this.visit_mut_expr(&mut o.to)?;
		if let Some(d) = o.data.as_mut(){
			this.visit_mut_data(d)?;
		}
		if let Some(o) = o.output.as_mut(){
			this.visit_mut_output(o)?;
		}
		this.visit_mut_expr(&mut o.timeout)?;
		Ok(())
	}

	fn visit_mut_output_stmt(this, o: &mut OutputStatement){
		this.visit_mut_expr(&mut o.what)?;
		if let Some(f) = o.fetch.as_mut(){
			for f in f.0.iter_mut(){
				this.visit_mut_expr(&mut f.0)?;
			}
		}
		Ok(())
	}

	fn visit_mut_insert(this, i: &mut InsertStatement){
		this.visit_mut_expr(&mut i.into)?;
		this.visit_mut_data(&mut i.data)?;
		if let Some(update) = i.update.as_mut(){
			this.visit_mut_data(update)?;
		}
		if let Some(o) = i.output.as_mut(){
			this.visit_mut_output(o)?;
		}
		this.visit_mut_expr(&mut i.timeout)?;
		this.visit_mut_expr(&mut i.version)?;
		Ok(())
	}

	fn visit_mut_info(this, i: &mut InfoStatement){
		match i{
			InfoStatement::Root(_) |
			InfoStatement::Ns(_) => {}
			InfoStatement::Db(_, expr) => {
				if let Some(e) = expr.as_mut(){
					this.visit_mut_expr(e)?;
				}
			},
			InfoStatement::Tb(expr, _, expr1) => {
				this.visit_mut_expr(expr)?;
				if let Some(e) = expr1.as_mut(){
					this.visit_mut_expr(e)?;
				}
			},
			InfoStatement::User(expr, base, _) => {
				this.visit_mut_expr(expr)?;
			},
			InfoStatement::Index(expr, expr1, _) => {
				this.visit_mut_expr(expr)?;
				this.visit_mut_expr(expr1)?;
			},
		}
		Ok(())
	}

	fn visit_mut_if_else(this, i: &mut IfelseStatement){
		for e in i.exprs.iter_mut(){
			this.visit_mut_expr(&mut e.0)?;
			this.visit_mut_expr(&mut e.1)?;
		}
		if let Some(x) = i.close.as_mut(){
			this.visit_mut_expr(x)?;
		}
		Ok(())
	}

	fn visit_mut_foreach(this, f: &mut ForeachStatement){
		this.visit_mut_param(&mut f.param)?;
		this.visit_mut_expr(&mut f.range)?;
		this.visit_mut_block(&mut f.block)?;
		Ok(())
	}

	fn visit_mut_delete(this, d: &mut DeleteStatement){
		for v in d.what.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(c) = d.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}
		if let Some(o) = d.output.as_mut(){
			this.visit_mut_output(o)?;
		}
		this.visit_mut_expr(&mut d.timeout)?;
		Ok(())
	}

	fn visit_mut_define(this, d: &mut DefineStatement){
		match d {
			DefineStatement::Namespace(d) => {
				this.visit_mut_define_namespace(d)?;
			},
			DefineStatement::Database(d) => {
				this.visit_mut_define_database(d)?;
			},
			DefineStatement::Function(d) => {
				this.visit_mut_define_function(d)?;
			},
			DefineStatement::Analyzer(d) => {
				this.visit_mut_define_analyzer(d)?;
			},
			DefineStatement::Param(d) => {
				this.visit_mut_define_param(d)?;
			},
			DefineStatement::Table(d) => {
				this.visit_mut_define_table(d)?;
			},
			DefineStatement::Event(d) => {
				this.visit_mut_define_event(d)?;
			},
			DefineStatement::Field(d) => {
				this.visit_mut_define_field(d)?;
			},
			DefineStatement::Index(d) => {
				this.visit_mut_define_index(d)?;
			},
			DefineStatement::User(d) => {
				this.visit_mut_define_user(d)?;
			},
			DefineStatement::Model(d) => {
				this.visit_mut_define_model(d)?;
			},
			DefineStatement::Access(d) => {
				this.visit_mut_define_access(d)?;
			},
			DefineStatement::Config(d) => {
				this.visit_mut_define_config(d)?;
			},
			DefineStatement::Api(d) => {
				this.visit_mut_define_api(d)?;
			},
			DefineStatement::Bucket(d) => {
				this.visit_mut_define_bucket(d)?;
			},
			DefineStatement::Sequence(d) => {
				this.visit_mut_define_sequence(d)?;
			},
			DefineStatement::Module(d) => {
				this.visit_mut_define_module(d)?;
			},
		}
		Ok(())
	}

	fn visit_mut_define_sequence(this, d: &mut DefineSequenceStatement) {
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.batch)?;
		this.visit_mut_expr(&mut d.start)?;

		this.visit_mut_expr(&mut d.timeout)?;
		Ok(())
	}

	fn visit_mut_define_bucket(this, d: &mut DefineBucketStatement) {
		this.visit_mut_expr(&mut d.name)?;
		if let Some(expr) = d.backend.as_mut(){
			this.visit_mut_expr(expr)?;
		}
		this.visit_mut_permission(&mut d.permissions)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_api(this, d: &mut DefineApiStatement) {
		this.visit_mut_expr(&mut d.path)?;
		for act in d.actions.iter_mut(){
			this.visit_mut_api_action(act)?;
		}
		if let Some(f) = d.fallback.as_mut(){
			this.visit_mut_expr(f)?;
		}
		this.visit_mut_api_config(&mut d.config)?;
		this.visit_mut_expr(&mut d.comment)?;

		Ok(())
	}

	fn visit_mut_api_action(this, a: &mut ApiAction){
		this.visit_mut_expr(&mut a.action)?;
		this.visit_mut_api_config(&mut a.config)?;
		Ok(())
	}


	fn visit_mut_define_config(this, d: &mut DefineConfigStatement) {
		this.visit_mut_config_inner(&mut d.inner)
	}

	fn visit_mut_config_inner(this, d: &mut ConfigInner){
		match d {
			ConfigInner::GraphQL(graph_qlconfig) => {
				// TODO: Reintroduce once graphql is fixed.
				//this.visit_mut_graphql_config(graph_qlconfig)?;
			},
			ConfigInner::Api(api_config) => {
				this.visit_mut_api_config(api_config)?;
			},
			ConfigInner::Default(default_config) => {
				this.visit_mut_default_config(default_config)?;
			},
		}
		Ok(())
	}

	fn visit_mut_api_config(this, d: &mut ApiConfig){
		for m in d.middleware.iter_mut(){
			for v in m.args.iter_mut(){
				this.visit_mut_expr(v)?;
			}
		}
		this.visit_mut_permission(&mut d.permissions)?;
		Ok(())
	}

	fn visit_mut_default_config(this, d: &mut DefaultConfig) {
		this.visit_mut_expr(&mut d.namespace)?;
		this.visit_mut_expr(&mut d.database)?;
		Ok(())
	}

	fn visit_mut_define_access(this, d: &mut DefineAccessStatement) {
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_access_type(&mut d.access_type)?;
		if let Some(v) = d.authenticate.as_mut(){
			this.visit_mut_expr(v)?;
		}
		this.visit_mut_expr(&mut d.duration.grant)?;
		this.visit_mut_expr(&mut d.duration.token)?;
		this.visit_mut_expr(&mut d.duration.session)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_access_type(this, a: &mut AccessType) {
		match a {
			AccessType::Record(r) => { this.visit_mut_record_access(r)?; },
			AccessType::Jwt(j) => { this.visit_mut_jwt_access(j)?; },
			AccessType::Bearer(b) => { this.visit_mut_bearer_access(b)?; },
		}
		Ok(())
	}

	fn visit_mut_record_access(this, r: &mut RecordAccess){
		if let Some(e) = r.signup.as_mut(){
			this.visit_mut_expr(e)?
		}
		if let Some(e) = r.signin.as_mut(){
			this.visit_mut_expr(e)?
		}

		this.visit_mut_jwt_access(&mut r.jwt)?;
		if let Some(b) = r.bearer.as_mut(){
			this.visit_mut_bearer_access(b)?;
		}
		Ok(())
	}

	fn visit_mut_jwt_access(this, j: &mut JwtAccess){
		match j.verify{
			JwtAccessVerify::Key(ref mut k) => {
				this.visit_mut_expr(&mut k.key)?

			},
			JwtAccessVerify::Jwks(ref mut j) => {
				this.visit_mut_expr(&mut j.url)?
			},
		}

		if let Some(i) = j.issue.as_mut(){
			this.visit_mut_expr(&mut i.key)?
		}

		Ok(())
	}

	fn visit_mut_bearer_access(this, r: &mut BearerAccess){
		this.visit_mut_jwt_access(&mut r.jwt)?;
		Ok(())
	}

	fn visit_mut_define_model(this, d: &mut DefineModelStatement) {
		this.visit_mut_permission(&mut d.permissions)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_module(this, d: &mut DefineModuleStatement) {
		this.visit_mut_permission(&mut d.permissions)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_user(this, d: &mut DefineUserStatement) {
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.duration.token)?;
		this.visit_mut_expr(&mut d.duration.session)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_index(this, d: &mut DefineIndexStatement) {
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.what)?;
		for c in d.cols.iter_mut(){
			this.visit_mut_expr(c)?;
		}
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_field(this, d: &mut DefineFieldStatement) {
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.what)?;
		if let Some(k) = d.field_kind.as_mut(){
			this.visit_mut_kind(k)?;
		}
		if let Some(v) = d.value.as_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(v) = d.assert.as_mut(){
			this.visit_mut_expr(v)?;
		}
		if let Some(v) = d.computed.as_mut(){
			this.visit_mut_expr(v)?;
		}
		match d.default{
			DefineDefault::None => {},
			DefineDefault::Always(ref mut expr) |
			DefineDefault::Set(ref mut expr) => this.visit_mut_expr(expr)?,
		}
		this.visit_mut_permissions(&mut d.permissions)?;
		if let Some(r) = d.reference.as_mut(){
			this.visit_mut_reference(r)?;
		}
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_reference(this, d: &mut Reference){
		this.visit_mut_delete_strategy(&mut d.on_delete)

	}

	fn visit_mut_delete_strategy(this, d: &mut ReferenceDeleteStrategy){
		match d {
			ReferenceDeleteStrategy::Reject |
				ReferenceDeleteStrategy::Ignore |
				ReferenceDeleteStrategy::Cascade |
				ReferenceDeleteStrategy::Unset => {},
			ReferenceDeleteStrategy::Custom(value) => {
				this.visit_mut_expr(value)?;
			},

		}
		Ok(())
	}


	fn visit_mut_define_event(this, d: &mut DefineEventStatement){
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.target_table)?;
		this.visit_mut_expr(&mut d.when)?;
		for v in d.then.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_table(this, d: &mut DefineTableStatement){
		this.visit_mut_expr(&mut d.name)?;
		if let Some(v) = d.view.as_mut(){
			this.visit_mut_view(v)?;
		}
		this.visit_mut_permissions(&mut d.permissions)?;
		this.visit_mut_expr(&mut d.comment)?;

		this.visit_mut_table_type(&mut d.table_type)?;

		Ok(())
	}

	fn visit_mut_table_type(this, t: &mut TableType){
		match t {
			TableType::Any |
				TableType::Normal => {}
			TableType::Relation(relation) => {
				this.visit_mut_relation(relation)?;
			},
		}
		Ok(())
	}

	fn visit_mut_relation(this, r: &mut Relation){
		if let Some(k) = r.from.as_mut(){
			this.visit_mut_kind(k)?;
		}
		if let Some(k) = r.to.as_mut(){
			this.visit_mut_kind(k)?;
		}
		Ok(())
	}

	fn visit_mut_permissions(this, d: &mut Permissions){
		this.visit_mut_permission(&mut d.select)?;
		this.visit_mut_permission(&mut d.create)?;
		this.visit_mut_permission(&mut d.update)?;
		this.visit_mut_permission(&mut d.delete)?;
		Ok(())
	}

	fn visit_mut_view(this, v: &mut View){
		this.visit_mut_fields(&mut v.expr)?;

		if let Some(c) = v.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}

		if let Some(g) = v.group.as_mut(){
			for g in g.0.iter_mut(){
				this.visit_mut_idiom(&mut g.0)?
			}
		}

		Ok(())
	}

	fn visit_mut_define_param(this, d: &mut DefineParamStatement){
		this.visit_mut_expr(&mut d.value)?;
		this.visit_mut_expr(&mut d.comment)?;
		this.visit_mut_permission(&mut d.permissions)?;
		Ok(())
	}

	fn visit_mut_define_analyzer(this, d: &mut DefineAnalyzerStatement){
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_function(this, d: &mut DefineFunctionStatement){
		for (_, k) in d.args.iter_mut(){
			this.visit_mut_kind(k)?;
		}
		this.visit_mut_block(&mut d.block)?;
		this.visit_mut_permission(&mut d.permissions)?;
		this.visit_mut_expr(&mut d.comment)?;
		if let Some(k) = d.returns.as_mut(){
			this.visit_mut_kind(k)?;
		}
		Ok(())
	}

	fn visit_mut_permission(this, p: &mut Permission){
		match p {
			Permission::None |
				Permission::Full => {},
			Permission::Specific(e) => {
				this.visit_mut_expr(e)?;
			},
		}
		Ok(())
	}

	fn visit_mut_define_database(this,  d: &mut DefineDatabaseStatement){
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_namespace(this,  d: &mut DefineNamespaceStatement){
		this.visit_mut_expr(&mut d.name)?;
		this.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_create(this, c: &mut CreateStatement){
		for w in c.what.iter_mut(){
			this.visit_mut_expr(w)?;
		}

		if let Some(data) = c.data.as_mut(){
			this.visit_mut_data(data)?;
		}

		if let Some(output) = c.output.as_mut(){
			this.visit_mut_output(output)?;
		}

		this.visit_mut_expr(&mut c.timeout)?;

		this.visit_mut_expr(&mut c.version)?;

		Ok(())
	}

	fn visit_mut_output(this, o: &mut Output){
		match o {
			Output::None |
				Output::Null |
				Output::Diff |
				Output::After |
				Output::Before => {},
			Output::Fields(fields) => {
				this.visit_mut_fields(fields)?;
			},
		}
		Ok(())
	}

	fn visit_mut_data(this, d: &mut Data){
		match d {
			Data::EmptyExpression => {},
			Data::UpdateExpression(assign) |
				Data::SetExpression(assign) => {
					for a in assign{
						this.visit_mut_assignment(a)?;
					}
				},
			Data::UnsetExpression(idioms) => {
				for i in idioms {
					this.visit_mut_idiom(i)?;
				}
			},
			Data::PatchExpression(value) |
				Data::MergeExpression(value) |
				Data::ReplaceExpression(value) |
				Data::ContentExpression(value) |
				Data::SingleExpression(value) => {
					this.visit_mut_expr(value)?;
				},
			Data::ValuesExpression(items) => {
				for (i,v) in items.iter_mut().flat_map(|x| x.iter_mut()){
					this.visit_mut_idiom(i)?;
					this.visit_mut_expr(v)?;
				}
			},
		}
		Ok(())
	}

	fn visit_mut_assignment(this, a: &mut Assignment){
		this.visit_mut_idiom(&mut a.place)?;
		this.visit_mut_expr(&mut a.value)?;
		Ok(())
	}


	/*fn visit_mut_refs(this, r: &mut Refs){
		for (_,i) in r.0.iter_mut(){
			if let Some(i) = i.as_mut(){
				this.visit_mut_idiom(i)?;
			}
		}
		Ok(())
	}*/


	fn visit_mut_closure(this, c: &mut ClosureExpr){
		for (_,k) in c.args.iter_mut(){
			this.visit_mut_kind(k)?;
		}
		if let Some(k) = c.returns.as_mut(){
			this.visit_mut_kind(k)?;
		}
		this.visit_mut_expr(&mut c.body)?;
		Ok(())
	}

	fn visit_mut_model(this, m: &mut Model) {
		Ok(())
	}


	fn visit_mut_function_call(this, f: &mut FunctionCall){
		this.visit_mut_function(&mut f.receiver)?;
		for a in f.arguments.iter_mut(){
			this.visit_mut_expr(a)?;
		}
		Ok(())
	}

	fn visit_mut_function(this, f: &mut Function){
		Ok(())
	}


	fn visit_mut_block(this, value: &mut Block){
		for v in value.0.iter_mut(){
			this.visit_mut_expr(v)?;
		}
		Ok(())

	}

	fn visit_mut_kind(this, kind: &mut Kind){
		match kind {
			Kind::None |
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
				Kind::String |
				Kind::Uuid |
				Kind::Regex |
				Kind::Range |
				Kind::Record(_) |
				Kind::Geometry(_) |
				Kind::Table(_) |
				Kind::File(_) => {}
			Kind::Either(kinds) => {
				for k in kinds.iter_mut() {
					this.visit_mut_kind(k)?;
				}
			},
			Kind::Set(kind, _) => {
				this.visit_mut_kind(kind)?;
			},
			Kind::Array(kind, _) => {
				this.visit_mut_kind(kind)?;
			},
			Kind::Function(kinds, kind) => {
				if let Some(kinds) = kinds.as_mut(){
					for k in kinds.iter_mut() {
						this.visit_mut_kind(k)?;
					}
				}
				if let Some(k) = kind {
					this.visit_mut_kind(k)?;
				}
			},
			Kind::Literal(literal) => {
				this.visit_mut_kind_liter_mutal(literal)?;
			},
		}
		Ok(())
	}

	fn visit_mut_kind_liter_mutal(this, k: &mut KindLiteral){
		match k{
			KindLiteral::String(_) => {},
			KindLiteral::Bool(_) => {},
			KindLiteral::Duration(_) => {},
			KindLiteral::Float(_) => {},
			KindLiteral::Integer(_) => {},
			KindLiteral::Decimal(_) => {},
			KindLiteral::Array(kinds) => {
				for k in kinds.iter_mut(){
					this.visit_mut_kind(k)?;
				}
			},
			KindLiteral::Object(btree_map) => {
				for v in btree_map.values_mut(){
					this.visit_mut_kind(v)?;
				}
			},
		}
		Ok(())

	}

	fn visit_mut_record_id(this, t: &mut RecordIdLit) {
		this.visit_mut_record_id_key(&mut t.key)
	}

	fn visit_mut_record_id_key(this, id: &mut RecordIdKeyLit) {
		match id {
			RecordIdKeyLit::Number(_) => {},
			RecordIdKeyLit::String(_) => {},
			RecordIdKeyLit::Uuid(_) => {},
			RecordIdKeyLit::Array(array) => {
				for e in array.iter_mut(){
					this.visit_mut_expr(e)?;
				}
			},
			RecordIdKeyLit::Object(object) => {
				for e in object.iter_mut(){
					this.visit_mut_expr(&mut e.value)?;
				}
			},
			RecordIdKeyLit::Generate(_) => {},
			RecordIdKeyLit::Range(id_range) => {
				this.visit_mut_record_id_key_range(id_range)?;
			},
		}
		Ok(())
	}

	fn visit_mut_record_id_key_range(this, id_range: &mut RecordIdKeyRangeLit)  {
		match id_range.start{
			Bound::Included(ref mut x) | Bound::Excluded(ref mut x) => {
				this.visit_mut_record_id_key(x)?;
			},
			Bound::Unbounded => {},
		}
		match id_range.end{
			Bound::Included(ref mut x) | Bound::Excluded(ref mut x) => {
				this.visit_mut_record_id_key(x)?;
			},
			Bound::Unbounded => {},
		}
		Ok(())
	}

	fn visit_mut_param(_this, _param: &mut Param) {
		Ok(())
	}

	fn visit_mut_idiom(this, idiom: &mut Idiom) {
		for p in idiom.0.iter_mut(){
			this.visit_mut_part(p)?;
		}
		Ok(())
	}

	fn visit_mut_part(this, part: &mut Part) {
		match part{
			Part::All |
				Part::Flatten |
				Part::Last |
				Part::First |
				Part::Optional |
				Part::Field(_) |
				Part::Doc |
				Part::RepeatRecurse  => {}
			Part::Where(value) | Part::Value(value) | Part::Start(value) => {
				this.visit_mut_expr(value)?;
			},
			Part::Method(_, values) => {
				for v in values {
					this.visit_mut_expr(v)?;
				}
			},
			Part::Destructure(destructure_parts) => {
				for p in destructure_parts.iter_mut(){
					this.visit_mut_destructure_part(p)?;
				}
			},
			Part::Recurse(recurse, idiom, recurse_instruction) => {
				this.visit_mut_recurse(recurse)?;
				if let Some(idiom) = idiom.as_mut(){
					this.visit_mut_idiom(idiom)?;
				}
				if let Some(instr) = recurse_instruction.as_mut(){
					this.visit_mut_recurse_instruction(instr)?;
				}
			},
			Part::Lookup(l) => {
				this.visit_mut_lookup(l)?;
			}
		}
		Ok(())
	}

	fn visit_mut_lookup(this, l: &mut Lookup){
		if let Some(f)  = l.expr.as_mut(){
			this.visit_mut_fields(f)?;
		}
		for w in l.what.iter_mut(){
			this.visit_mut_lookup_subject(w)?;
		}

		if let Some(c) = l.cond.as_mut(){
			this.visit_mut_expr(&mut c.0)?;
		}

		if let Some(s) = l.split.as_mut(){
			for s in s.0.iter_mut(){
				this.visit_mut_idiom(&mut s.0)?;
			}
		}

		if let Some(groups) = l.group.as_mut() {
			for g in groups.0.iter_mut(){
				this.visit_mut_idiom(&mut g.0)?;
			}
		}

		if let Some(order) = l.order.as_mut(){
			this.visit_mut_ordering(order)?;
		}

		if let Some(limit) = l.limit.as_mut(){
			this.visit_mut_expr(&mut limit.0)?;
		}

		if let Some(start) = l.start.as_mut(){
			this.visit_mut_expr(&mut start.0)?;
		}

		if let Some(alias) = l.alias.as_mut(){
			this.visit_mut_idiom(alias)?;
		}

		Ok(())

	}

	fn visit_mut_recurse_instruction(this, r: &mut RecurseInstruction){
		match r {
			RecurseInstruction::Path { ..} |
				RecurseInstruction::Collect { ..} => {}
			RecurseInstruction::Shortest { expects, .. } => {
				this.visit_mut_expr(expects)?;
			},
		}
		Ok(())
	}

	fn visit_mut_recurse(_this, _r: &mut Recurse){
		Ok(())
	}

	fn visit_mut_destructure_part(this, p: &mut DestructurePart) {
		match p {
			DestructurePart::All(_) |
				DestructurePart::Field(_) => {},
				DestructurePart::Aliased(_, idiom) => {
					this.visit_mut_idiom(idiom)?;
				},
				DestructurePart::Destructure(_, destructure_parts) => {
					for p in destructure_parts{
						this.visit_mut_destructure_part(p)?;
					}
				},
		}
		Ok(())
	}


	fn visit_mut_ordering(this, ordering: &mut Ordering){
		match ordering{
			Ordering::Random => {},
			Ordering::Order(order_list) => {
				for o in order_list.0.iter_mut(){
					this.visit_mut_idiom(&mut o.value)?;
				}
			},
		}
		Ok(())
	}

	fn visit_mut_fields(this, fields: &mut Fields) {
		match fields {
			Fields::Value(field) => {
				this.visit_mut_selector(field)?;
			},
			Fields::Select(fields) => {
				for f in fields.iter_mut(){
					this.visit_mut_field(f)?;
				}
			},
		}

		Ok(())
	}

	fn visit_mut_field(this, field: &mut Field){
		match field {
			Field::All => {},
			Field::Single(s) => this.visit_mut_selector(s)?,
		}
		Ok(())
	}

	fn visit_mut_selector(this, selector: &mut Selector){
		this.visit_mut_expr(&mut selector.expr)?;
		if let Some(alias) = &mut selector.alias{
			this.visit_mut_idiom(alias)?;
		}
		Ok(())
	}

	fn visit_mut_lookup_subject(this, subject: &mut LookupSubject){
		match subject{
			LookupSubject::Table { .. } => {},
			LookupSubject::Range { range, .. } => {
				this.visit_mut_record_id_key_range(range)?;
			},
		}
		Ok(())
	}
}
