use surrealdb_strand::Strand;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveParamStatement {
	pub name: Strand,
	pub if_exists: bool,
}
