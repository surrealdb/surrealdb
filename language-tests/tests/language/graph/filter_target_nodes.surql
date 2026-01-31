/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test target node filtering with WHERE clauses on destinations"

# 0
[[test.results]]
value = "[project:auth, project:database]"

# 1
[[test.results]]
value = "[project:database]"

# 2
[[test.results]]
value = "[project:auth]"

# 3
[[test.results]]
value = "[skill:go, skill:rust]"

# 4
[[test.results]]
value = "[skill:postgresql, skill:redis]"

# 5
[[test.results]]
value = "[skill:react, skill:typescript]"
*/

-- 0: Only active projects
person:alice->works_on->(project WHERE status = "active");

-- 1: High priority projects (priority 1)
person:alice->works_on->(project WHERE priority = 1);

-- 2: Lower priority projects (priority > 1)
person:alice->works_on->(project WHERE priority > 1);

-- 3: Backend skills only
person:alice->has_skill->(skill WHERE category = "backend");

-- 4: Database skills only
person:alice->has_skill->(skill WHERE category = "database");

-- 5: Frontend skills (charlie's skills)
person:charlie->has_skill->(skill WHERE category = "frontend");

