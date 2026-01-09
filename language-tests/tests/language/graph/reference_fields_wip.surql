/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "WIP: Test reference field syntax (<~) - alias syntax is broken"
wip = true
*/

-- Get direct reports via reference field
person:lead_infra<~person;

-- Reference field traversal for directors
person:dir_platform<~person AS reports;
person:dir_product<~person AS reports;

-- Who has org:engineering as parent?
org:engineering<~org AS child_orgs;

-- Teams that reference this org
org:infrastructure<~team AS teams;

