use cedar_policy::{Authorizer, Context, Decision, Entities, Entity, EntityUid, Request, Response};

mod policy_set;

use policy_set::*;

use crate::iam::{Action, Actor, Resource};

/// Checks if the actor is allowed to do the action on the resource, given the context and based on the default policy set.
pub fn is_allowed(
	actor: &Actor,
	action: &Action,
	resource: &Resource,
	context: Context,
) -> (bool, Response) {
	_is_allowed(
		Some(actor.into()),
		Some(action.into()),
		Some(resource.into()),
		Entities::from_entities(_get_entities(Some(actor), Some(resource))).unwrap(),
		context,
	)
}

fn _get_entities(actor: Option<&Actor>, resource: Option<&Resource>) -> Vec<Entity> {
	let mut entities = Vec::new();
	if let Some(actor) = actor {
		entities.extend(actor.cedar_entities());
	}
	if let Some(resource) = resource {
		entities.extend(resource.cedar_entities());
	}
	entities
}

fn _is_allowed(
	actor: Option<EntityUid>,
	action: Option<EntityUid>,
	resource: Option<EntityUid>,
	entities: Entities,
	context: Context,
) -> (bool, Response) {
	let policy_set = POLICY_SET.to_owned();

	let authorizer = Authorizer::new();

	let req = Request::new(actor, action, resource, context);

	let res = authorizer.is_authorized(&req, &policy_set, &entities);

	(res.decision() == Decision::Allow, res)
}

#[cfg(test)]
mod tests {
	use cedar_policy::{ValidationMode, ValidationResult, Validator};

	use crate::iam::{default_schema, entities::Level, ResourceKind, Role};

	use super::*;

	#[test]
	fn validate_policy_set() {
		let validator = Validator::new(default_schema());
		let result = Validator::validate(&validator, &POLICY_SET, ValidationMode::default());

		if !ValidationResult::validation_passed(&result) {
			let e = ValidationResult::validation_errors(&result);
			for err in e {
				println!("{}", err);
			}
			panic!("Policy set validation failed");
		}
	}

	#[test]
	fn test_is_allowed() {
		// Returns true if the actor is allowed to do the action on the resource
		let actor = Actor::new("test".into(), vec![Role::Viewer], Level::Root);
		let res = ResourceKind::Namespace.on_root();

		let (allowed, _) = is_allowed(&actor, &Action::View, &res, Context::empty());
		assert!(allowed);

		// Returns false if the actor is not allowed to do the action on the resource
		let actor = Actor::new(
			"test".into(),
			vec![Role::Viewer],
			Level::Database("test".into(), "test".into()),
		);
		let res = ResourceKind::Namespace.on_root();

		let (allowed, _) = is_allowed(&actor, &Action::View, &res, Context::empty());
		assert!(!allowed);
	}
}
