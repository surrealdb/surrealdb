/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test target node filtering with WHERE clauses on destinations"

[[test.results]]
value = "[project:auth, project:database]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[project:auth]"

[[test.results]]
value = "[skill:go, skill:rust]"

[[test.results]]
value = "[skill:postgresql, skill:redis]"

[[test.results]]
value = "[skill:react, skill:typescript]"
*/

-- Only active projects
person:alice->works_on->(project WHERE status = "active");

-- High priority projects (priority 1)
person:alice->works_on->(project WHERE priority = 1);

-- Lower priority projects (priority > 1)
person:alice->works_on->(project WHERE priority > 1);

-- Backend skills only
person:alice->has_skill->(skill WHERE category = "backend");

-- Database skills only
person:alice->has_skill->(skill WHERE category = "database");

-- Frontend skills (charlie's skills)
person:charlie->has_skill->(skill WHERE category = "frontend");

