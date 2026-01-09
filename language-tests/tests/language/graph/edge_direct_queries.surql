/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test direct queries on edge tables"

[[test.results]]
value = "[{ id: reports_to:alice_to_li, in: person:alice, out: person:lead_infra, since: d'2019-01-10T00:00:00Z' }, { id: reports_to:bob_to_li, in: person:bob, out: person:lead_infra, since: d'2020-03-15T00:00:00Z' }, { id: reports_to:charlie_to_lf, in: person:charlie, out: person:lead_frontend, since: d'2019-07-22T00:00:00Z' }, { id: reports_to:dana_to_lf, in: person:dana, out: person:lead_frontend, since: d'2021-01-05T00:00:00Z' }, { id: reports_to:dp_to_vp, in: person:dir_platform, out: person:vp_eng, since: d'2017-06-01T00:00:00Z' }, { id: reports_to:dpr_to_vp, in: person:dir_product, out: person:vp_eng, since: d'2017-09-15T00:00:00Z' }, { id: reports_to:lf_to_dpr, in: person:lead_frontend, out: person:dir_product, since: d'2018-05-20T00:00:00Z' }, { id: reports_to:li_to_dp, in: person:lead_infra, out: person:dir_platform, since: d'2018-02-01T00:00:00Z' }, { id: reports_to:vp_to_ceo, in: person:vp_eng, out: person:ceo, since: d'2016-03-20T00:00:00Z' }]"

[[test.results]]
value = "[{ id: knows:alice_bob, in: person:alice, out: person:bob, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { id: knows:alice_ceo, in: person:alice, out: person:ceo, since: d'2022-12-15T00:00:00Z', strength: 'weak' }, { id: knows:bob_alice, in: person:bob, out: person:alice, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { id: knows:bob_charlie, in: person:bob, out: person:charlie, since: d'2020-04-15T00:00:00Z', strength: 'moderate' }, { id: knows:bob_dana, in: person:bob, out: person:dana, since: d'2021-02-01T00:00:00Z', strength: 'weak' }, { id: knows:charlie_alice, in: person:charlie, out: person:alice, since: d'2020-06-15T00:00:00Z', strength: 'moderate' }, { id: knows:charlie_dana, in: person:charlie, out: person:dana, since: d'2021-01-10T00:00:00Z', strength: 'strong' }, { id: knows:dana_charlie, in: person:dana, out: person:charlie, since: d'2021-01-10T00:00:00Z', strength: 'strong' }, { id: knows:dp_dpr, in: person:dir_platform, out: person:dir_product, since: d'2017-10-01T00:00:00Z', strength: 'strong' }, { id: knows:dpr_dp, in: person:dir_product, out: person:dir_platform, since: d'2017-10-01T00:00:00Z', strength: 'strong' }, { id: knows:lf_li, in: person:lead_frontend, out: person:lead_infra, since: d'2018-06-01T00:00:00Z', strength: 'moderate' }, { id: knows:li_lf, in: person:lead_infra, out: person:lead_frontend, since: d'2018-06-01T00:00:00Z', strength: 'moderate' }]"

[[test.results]]
value = "[{ id: knows:alice_bob, in: person:alice, out: person:bob, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { id: knows:alice_ceo, in: person:alice, out: person:ceo, since: d'2022-12-15T00:00:00Z', strength: 'weak' }]"

[[test.results]]
value = "[{ hours: 30, id: works_on:alice_db, in: person:alice, out: project:database, role: 'lead' }, { hours: 25, id: works_on:bob_db, in: person:bob, out: project:database, role: 'contributor' }, { hours: 5, id: works_on:li_db, in: person:lead_infra, out: project:database, role: 'sponsor' }]"

[[test.results]]
value = "[{ hours: 30, id: works_on:alice_db, in: person:alice, out: project:database, role: 'lead' }]"

[[test.results]]
value = "[{ in: person:alice, out: person:bob, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { in: person:alice, out: person:ceo, since: d'2022-12-15T00:00:00Z', strength: 'weak' }, { in: person:bob, out: person:alice, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { in: person:bob, out: person:charlie, since: d'2020-04-15T00:00:00Z', strength: 'moderate' }, { in: person:bob, out: person:dana, since: d'2021-02-01T00:00:00Z', strength: 'weak' }, { in: person:charlie, out: person:alice, since: d'2020-06-15T00:00:00Z', strength: 'moderate' }, { in: person:charlie, out: person:dana, since: d'2021-01-10T00:00:00Z', strength: 'strong' }, { in: person:dana, out: person:charlie, since: d'2021-01-10T00:00:00Z', strength: 'strong' }, { in: person:dir_platform, out: person:dir_product, since: d'2017-10-01T00:00:00Z', strength: 'strong' }, { in: person:dir_product, out: person:dir_platform, since: d'2017-10-01T00:00:00Z', strength: 'strong' }, { in: person:lead_frontend, out: person:lead_infra, since: d'2018-06-01T00:00:00Z', strength: 'moderate' }, { in: person:lead_infra, out: person:lead_frontend, since: d'2018-06-01T00:00:00Z', strength: 'moderate' }]"

[[test.results]]
value = "[{ hours: 10, in: person:alice, out: project:auth, role: 'contributor' }, { hours: 30, in: person:alice, out: project:database, role: 'lead' }]"
*/

-- All reports_to relationships
SELECT * FROM reports_to;

-- All knows relationships
SELECT * FROM knows;

-- Filter by source (in)
SELECT * FROM knows WHERE in = person:alice;

-- Filter by destination (out)
SELECT * FROM works_on WHERE out = project:database;

-- Filter by both in and out
SELECT * FROM works_on WHERE in = person:alice AND out = project:database;

-- Select specific edge properties
SELECT in, out, since, strength FROM knows;

-- Edge properties for specific relationships
SELECT in, out, role, hours FROM works_on WHERE in = person:alice;

