/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test nested destructuring with aliasing"

# 0
[[test.results]]
value = "[{ name: 'Priya Patel', team_name: 'Infrastructure Team' }]"

# 1
[[test.results]]
value = "[{ name: 'Authentication Service', project_team: 'Infrastructure Team' }, { name: 'Core Database', project_team: 'Infrastructure Team' }]"

# 2
[[test.results]]
value = "[{ location_name: 'SF Headquarters', name: 'Priya Patel', team_name: 'Infrastructure Team', title: 'Infrastructure Lead' }]"

*/

-- 0: Nested field access (manager's team)
person:alice->reports_to->person.{name, team_name: team.name};

-- 1: Nested field access (project's team name)
person:alice->works_on->project.{name, project_team: team.name};

-- 2: Multiple levels of nesting
person:alice->reports_to->person.{
    name,
    title,
    team_name: team.name,
    location_name: location.name
};

