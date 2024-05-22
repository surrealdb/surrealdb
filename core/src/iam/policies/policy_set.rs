use std::str::FromStr;

use cedar_policy::PolicySet;
use once_cell::sync::Lazy;

pub static POLICY_SET: Lazy<PolicySet> = Lazy::new(|| {
	PolicySet::from_str(
    r#"
    // All roles can view all resources on the same level hierarchy or below
    permit(
        principal,
        action == Action::"View",
        resource
    ) when {
        principal.roles.containsAny([Role::"Viewer", Role::"Editor", Role::"Owner"]) &&
        resource.level in principal.level
    };

    // Editor role can edit all non-IAM resources on the same level hierarchy or below
    permit(
        principal,
        action == Action::"Edit",
        resource
    ) when {
        principal.roles.contains(Role::"Editor") &&
        resource.level in principal.level &&
        ["Namespace", "Database", "Record", "Table", "Document", "Option", "Function", "Analyzer", "Parameter", "Event", "Field", "Index"].contains(resource.type)
    };

    // Owner role can edit all resources on the same level hierarchy or below
    permit(
        principal,
        action == Action::"Edit",
        resource
    ) when {
        principal.roles.contains(Role::"Owner") &&
        resource.level in principal.level
    };
"#).unwrap()
});
