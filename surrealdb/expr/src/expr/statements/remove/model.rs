use surrealdb_strand::Strand;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveModelStatement {
	pub name: Strand,
	pub version: Strand,
	pub if_exists: bool,
}
