use anyhow::{Result, bail};

use crate::catalog;
use crate::exec::Error as ExecError;

/// The checks `ALTER ACCESS` makes of the definition its clauses have formed.
pub(crate) fn alter_access_definition_validate(ac: &catalog::AccessDefinition) -> Result<()> {
	// Mirrors the check in `DefineAccessStatement::to_definition`: tokens
	// issued by record-access methods must expire, so reject ALTER paths
	// that would leave the resulting `token_duration` as NONE.
	if matches!(ac.access_type, catalog::AccessType::Record(_)) && ac.token_duration.is_none() {
		bail!(ExecError::AccessRecordTokenDurationRequired);
	}
	// Mirrors the check in `define_access_statement_to_definition`: an ALTER
	// that could install a CONTEXT clause DEFINE would refuse is the same hole
	// with an extra step.
	crate::ddl::validate_access_context(&ac.base, &ac.access_type, ac.context.as_ref())?;
	Ok(())
}
