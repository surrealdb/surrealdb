/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test statistical functions on edges (count, sum, mean)"

[[test.results]]
value = "[{ id: person:alice, knows_count: 2, works_count: 2 }, { id: person:bob, knows_count: 3, works_count: 2 }, { id: person:ceo, knows_count: 0, works_count: 0 }, { id: person:charlie, knows_count: 2, works_count: 2 }, { id: person:dana, knows_count: 1, works_count: 1 }, { id: person:dir_platform, knows_count: 1, works_count: 1 }, { id: person:dir_product, knows_count: 1, works_count: 0 }, { id: person:lead_frontend, knows_count: 1, works_count: 1 }, { id: person:lead_infra, knows_count: 1, works_count: 2 }, { id: person:vp_eng, knows_count: 0, works_count: 0 }]"

[[test.results]]
value = "[{ id: person:alice, total_hours: 40 }, { id: person:bob, total_hours: 55 }, { id: person:charlie, total_hours: 40 }, { id: person:dana, total_hours: 30 }, { id: person:dir_platform, total_hours: 3 }, { id: person:lead_frontend, total_hours: 5 }, { id: person:lead_infra, total_hours: 20 }]"

[[test.results]]
value = "[{ avg_skill_years: 4.5f, id: person:alice }, { avg_skill_years: 2.6666666666666665f, id: person:bob }, { avg_skill_years: 4.666666666666667f, id: person:charlie }, { avg_skill_years: 2f, id: person:dana }, { avg_skill_years: 4f, id: person:lead_frontend }, { avg_skill_years: 4.5f, id: person:lead_infra }]"

[[test.results]]
value = "[{ edge_count: 2, in: person:alice }, { edge_count: 3, in: person:bob }, { edge_count: 2, in: person:charlie }, { edge_count: 1, in: person:dana }, { edge_count: 1, in: person:dir_platform }, { edge_count: 1, in: person:dir_product }, { edge_count: 1, in: person:lead_frontend }, { edge_count: 1, in: person:lead_infra }]"

[[test.results]]
value = "[{ edges: [knows:alice_bob, knows:alice_ceo], id: person:alice }, { edges: [knows:bob_alice, knows:bob_charlie, knows:bob_dana], id: person:bob }, { edges: [knows:dana_charlie], id: person:dana }]"
*/

-- Count edges per person
SELECT id, count(->knows) AS knows_count, count(->works_on) AS works_count FROM person;

-- Sum hours per person
SELECT id, math::sum(->works_on.hours) AS total_hours FROM person WHERE ->works_on;

-- Average years of skill experience
SELECT id, math::mean(->has_skill.years) AS avg_skill_years FROM person WHERE ->has_skill;

-- Count edges by source
SELECT in, count() AS edge_count FROM knows GROUP BY in;

-- Edge selection for multiple people
SELECT id, ->knows AS edges FROM person:alice, person:bob, person:dana ORDER BY id;

