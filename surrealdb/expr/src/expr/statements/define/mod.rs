pub mod access;
pub mod analyzer;
pub mod api;
pub mod bucket;
pub mod config;
pub mod database;
pub mod event;
pub mod field;
pub mod function;
pub mod index;
pub mod model;
pub mod module;
pub mod namespace;
pub mod param;
pub mod sequence;
pub mod table;
pub mod user;

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use api::{ApiAction, DefineApiStatement};
pub use bucket::DefineBucketStatement;
pub use config::DefineConfigStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::{DefineDefault, DefineFieldStatement, kind_contains_object};
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use module::DefineModuleStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use sequence::DefineSequenceStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub enum DefineKind {
	#[default]
	Default,
	Overwrite,
	IfNotExists,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	/// Boxed, like its `sql` twin: it is by far the largest variant, so an
	/// unboxed one sizes every `DefineStatement` allocation to it.
	Field(Box<DefineFieldStatement>),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
	Access(DefineAccessStatement),
	Config(DefineConfigStatement),
	Api(DefineApiStatement),
	Bucket(DefineBucketStatement),
	Sequence(DefineSequenceStatement),
	Module(DefineModuleStatement),
}
