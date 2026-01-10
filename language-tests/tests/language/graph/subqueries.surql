/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test subqueries in graph paths"

# 0
[[test.results]]
value = """[{ "<-(SELECT * FROM knows ORDER BY since DESC LIMIT 5)": [{ id: knows:charlie_alice, in: person:charlie, out: person:alice, since: d'2020-06-15T00:00:00Z', strength: 'moderate' }, { id: knows:bob_alice, in: person:bob, out: person:alice, since: d'2020-03-20T00:00:00Z', strength: 'strong' }] }]"""

# 1
[[test.results]]
value = """[{ "<-(SELECT * FROM knows WHERE strength = 'strong')": [{ id: knows:bob_alice, in: person:bob, out: person:alice, since: d'2020-03-20T00:00:00Z', strength: 'strong' }] }]"""

# 2
[[test.results]]
value = "[{ mentorship: [{ area: 'leadership', id: mentors:dp_li, in: person:dir_platform, out: person:lead_infra, started: d'2018-03-01T00:00:00Z' }], reports: [{ id: reports_to:alice_to_li, in: person:alice, out: person:lead_infra, since: d'2019-01-10T00:00:00Z' }, { id: reports_to:bob_to_li, in: person:bob, out: person:lead_infra, since: d'2020-03-15T00:00:00Z' }] }]"

# 3
[[test.results]]
value = """[{ "->(SELECT count() AS cnt, role, array::group(out) AS projects FROM works_on GROUP BY role)": [{ cnt: 1, projects: [project:auth], role: 'contributor' }, { cnt: 1, projects: [project:database], role: 'lead' }] }]"""

# 4
[[test.results]]
value = "[{ id: knows:charlie_alice, in: person:charlie, out: person:alice, since: d'2020-06-15T00:00:00Z', strength: 'moderate' }, { id: knows:bob_alice, in: person:bob, out: person:alice, since: d'2020-03-20T00:00:00Z', strength: 'strong' }]"

# 5
[[test.results]]
value = "[{ hours: 30, id: works_on:alice_db, in: person:alice, out: project:database, role: 'lead' }, { hours: 10, id: works_on:alice_auth, in: person:alice, out: project:auth, role: 'contributor' }]"

# 6
[[test.results]]
value = "[{ id: has_skill:alice_pg, in: person:alice, level: 'expert', out: skill:postgresql, years: 6 }, { id: has_skill:alice_rust, in: person:alice, level: 'expert', out: skill:rust, years: 5 }, { id: has_skill:alice_redis, in: person:alice, level: 'advanced', out: skill:redis, years: 4 }]"

# 7
[[test.results]]
value = "[{ id: has_skill:alice_pg, in: person:alice, level: 'expert', out: skill:postgresql, years: 6 }, { id: has_skill:alice_rust, in: person:alice, level: 'expert', out: skill:rust, years: 5 }]"

# 8
[[test.results]]
value = "[project:database]"
*/

-- 0: Custom ordering of edges via subquery
SELECT <-(SELECT * FROM knows ORDER BY since DESC LIMIT 5) FROM person:alice;

-- 1: Filtered edges via subquery
SELECT <-(SELECT * FROM knows WHERE strength = "strong") FROM person:alice;

-- 2: Multiple edge tables combined via subquery
SELECT <-(SELECT * FROM reports_to) AS reports, <-(SELECT * FROM mentors) AS mentorship FROM person:lead_infra;

-- 3: Subquery with aggregation
SELECT ->(SELECT count() AS cnt, role, array::group(out) AS projects FROM works_on GROUP BY role) FROM person:alice;

-- 4: Most recent knows connections
person:alice<-(SELECT * FROM knows ORDER BY since DESC);

-- 5: Highest hour work assignments
person:alice->(SELECT * FROM works_on ORDER BY hours DESC);

-- 6: Top-rated skills
person:alice->(SELECT * FROM has_skill ORDER BY years DESC LIMIT 3);

-- 7: Expert skills only via subquery
person:alice->(SELECT * FROM has_skill WHERE level = "expert");

-- 8: Subquery edges then traverse to target
person:alice->(SELECT * FROM works_on WHERE hours > 20)->project;

