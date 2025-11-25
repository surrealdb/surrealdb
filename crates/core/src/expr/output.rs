use crate::expr::field::Fields;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum Output {
	#[default]
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}
