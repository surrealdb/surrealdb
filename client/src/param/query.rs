use crate::Result;
use surrealdb::sql;
use surrealdb::sql::statements::*;
use surrealdb::sql::Statement;
use surrealdb::sql::Statements;

/// A trait for converting inputs into SQL statements
pub trait Query {
	/// Converts an input into SQL statements
	fn try_into_query(self) -> Result<Vec<Statement>>;
}

impl Query for sql::Query {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		let sql::Query(Statements(statements)) = self;
		Ok(statements)
	}
}

impl Query for Statements {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		let Statements(statements) = self;
		Ok(statements)
	}
}

impl Query for Vec<Statement> {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(self)
	}
}

impl Query for Statement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![self])
	}
}

impl Query for UseStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Use(self)])
	}
}

impl Query for SetStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Set(self)])
	}
}

impl Query for InfoStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Info(self)])
	}
}

impl Query for LiveStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Live(self)])
	}
}

impl Query for KillStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Kill(self)])
	}
}

impl Query for BeginStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Begin(self)])
	}
}

impl Query for CancelStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Cancel(self)])
	}
}

impl Query for CommitStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Commit(self)])
	}
}

impl Query for OutputStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Output(self)])
	}
}

impl Query for IfelseStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Ifelse(self)])
	}
}

impl Query for SelectStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Select(self)])
	}
}

impl Query for CreateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Create(self)])
	}
}

impl Query for UpdateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Update(self)])
	}
}

impl Query for RelateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Relate(self)])
	}
}

impl Query for DeleteStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Delete(self)])
	}
}

impl Query for InsertStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Insert(self)])
	}
}

impl Query for DefineStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Define(self)])
	}
}

impl Query for RemoveStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Remove(self)])
	}
}

impl Query for OptionStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Option(self)])
	}
}

impl Query for &str {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(self)?.try_into_query()
	}
}

impl Query for &String {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(self)?.try_into_query()
	}
}

impl Query for String {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(&self)?.try_into_query()
	}
}
