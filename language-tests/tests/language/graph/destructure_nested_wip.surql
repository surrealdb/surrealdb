/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "WIP: Test nested destructuring with AS alias (currently has parsing errors)"
wip = true
*/

-- Nested field access (manager's team) - BROKEN: team.name AS team_name doesn't parse
person:alice->reports_to->person.{name, team.name AS team_name};

-- Nested field access (project's team name)
person:alice->works_on->project.{name, team.name AS project_team};

-- Multiple levels of nesting
person:alice->reports_to->person.{
    name,
    title,
    team.name AS team_name,
    location.name AS location_name
};

