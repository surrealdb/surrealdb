//! Credential preparation at the execution boundary and redaction for logs.
use std::convert::Infallible;

use rand::distr::{Alphanumeric, SampleString};

use super::statements::DefineUserStatement;
use super::statements::alter::AlterUserStatement;
use super::visit::{MutVisitor, Visit, VisitMut, Visitor};

pub(crate) struct PrepareCredentials {
	changed: bool,
}
impl MutVisitor for PrepareCredentials {
	type Error = Infallible;
	fn visit_mut_define_user(&mut self, user: &mut DefineUserStatement) -> Result<(), Infallible> {
		if let Some(password) = user.pending_password.take() {
			self.changed = true;
			if user.scram.is_none() {
				user.scram = crate::scram::derive(&password.0);
			}
			user.hash = crate::iam::hash_password(&password.0);
		}
		if user.code.is_empty() {
			self.changed = true;
			user.code = Alphanumeric.sample_string(&mut rand::rng(), 128);
		}
		user.visit_mut(self)
	}
	fn visit_mut_alter_user(&mut self, user: &mut AlterUserStatement) -> Result<(), Infallible> {
		if let Some(password) = user.pending_password.take() {
			self.changed = true;
			if user.scram.is_none() {
				// A new password always replaces the verifier: without one, the old
				// verifier is cleared so the old password stops authenticating over SCRAM.
				user.scram = Some(crate::scram::derive(&password.0));
			}
			user.hash = Some(crate::iam::hash_password(&password.0));
		}
		user.visit_mut(self)
	}
}

pub(crate) fn prepare_changed<T: VisitMut<PrepareCredentials>>(value: &mut T) -> bool {
	let mut visitor = PrepareCredentials {
		changed: false,
	};
	match value.visit_mut(&mut visitor) {
		Ok(()) => visitor.changed,
		Err(e) => match e {},
	}
}

pub(crate) fn prepare<T: VisitMut<PrepareCredentials>>(value: &mut T) {
	prepare_changed(value);
}

pub(crate) struct RedactCredentials;
impl MutVisitor for RedactCredentials {
	type Error = Infallible;
	fn visit_mut_define_user(&mut self, user: &mut DefineUserStatement) -> Result<(), Infallible> {
		if user.pending_password.take().is_some() || !user.hash.is_empty() {
			"[REDACTED]".clone_into(&mut user.hash);
		}
		user.scram = None;
		user.visit_mut(self)
	}
	fn visit_mut_alter_user(&mut self, user: &mut AlterUserStatement) -> Result<(), Infallible> {
		if user.pending_password.take().is_some() || user.hash.is_some() {
			user.hash = Some("[REDACTED]".to_owned());
		}
		user.scram = None;
		user.visit_mut(self)
	}
}

pub(crate) fn redact<T: VisitMut<RedactCredentials>>(value: &mut T) {
	match value.visit_mut(&mut RedactCredentials) {
		Ok(()) => {}
		Err(e) => match e {},
	}
}

pub(crate) struct UnpreparedCredentials(bool);
impl Visitor for UnpreparedCredentials {
	type Error = Infallible;
	fn visit_define_user(&mut self, user: &DefineUserStatement) -> Result<(), Infallible> {
		self.0 |= user.pending_password.is_some() || user.code.is_empty();
		user.visit(self)
	}
	fn visit_alter_user(&mut self, user: &AlterUserStatement) -> Result<(), Infallible> {
		self.0 |= user.pending_password.is_some();
		user.visit(self)
	}
}

/// Durable text must contain derived credentials, including in nested definitions.
pub(crate) fn for_storage<T>(value: &T) -> std::borrow::Cow<'_, T>
where
	T: Clone + Visit<UnpreparedCredentials> + VisitMut<PrepareCredentials>,
{
	let mut visitor = UnpreparedCredentials(false);
	match value.visit(&mut visitor) {
		Ok(()) => {}
		Err(e) => match e {},
	}
	if visitor.0 {
		let mut prepared = value.clone();
		prepare(&mut prepared);
		std::borrow::Cow::Owned(prepared)
	} else {
		std::borrow::Cow::Borrowed(value)
	}
}

impl super::Block {
	/// Render durable block text with derived credentials.
	pub fn to_stored_sql(&self) -> String {
		use surrealdb_types::ToSql;
		for_storage(self).to_sql()
	}
}
impl super::Fields {
	/// Render durable selection text with derived credentials.
	pub fn to_stored_sql(&self) -> String {
		use surrealdb_types::ToSql;
		for_storage(self).to_sql()
	}
}
impl super::Idiom {
	/// Render durable path text with derived credentials in computed parts.
	pub fn to_stored_sql(&self) -> String {
		for_storage(self).to_raw_string()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::user::PendingPassword;

	fn defined() -> DefineUserStatement {
		let mut user = DefineUserStatement {
			pending_password: Some(PendingPassword("secret".to_owned())),
			..DefineUserStatement::default()
		};
		let Ok(()) = PrepareCredentials {
			changed: false,
		}
		.visit_mut_define_user(&mut user);
		user
	}

	fn altered() -> AlterUserStatement {
		let mut user = AlterUserStatement {
			pending_password: Some(PendingPassword("secret".to_owned())),
			..AlterUserStatement::default()
		};
		let Ok(()) = PrepareCredentials {
			changed: false,
		}
		.visit_mut_alter_user(&mut user);
		user
	}

	#[cfg(feature = "scram")]
	#[test]
	fn a_password_stores_a_scram_verifier() {
		assert!(defined().scram.is_some());
		assert!(matches!(altered().scram, Some(Some(_))));
	}

	#[cfg(not(feature = "scram"))]
	#[test]
	fn without_scram_a_password_stores_no_verifier_and_clears_the_old_one() {
		assert!(defined().scram.is_none());
		assert!(matches!(altered().scram, Some(None)), "a new password clears the old verifier");
	}
}
