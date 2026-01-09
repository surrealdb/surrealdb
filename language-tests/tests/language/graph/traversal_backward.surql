/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test backward edge traversal (<-) patterns"

[[test.results]]
value = "[person:alice, person:bob]"

[[test.results]]
value = "[person:bob, person:charlie]"

[[test.results]]
value = "['Alice Zhang', 'Bob Martinez']"

[[test.results]]
value = "[person:lead_infra]"

[[test.results]]
value = "[{ direct_reports: ['Alice Zhang', 'Bob Martinez'], id: person:lead_infra, name: 'Priya Patel' }]"
*/

-- Single hop backward: who reports to lead_infra?
person:lead_infra<-reports_to<-person;

-- Single hop backward: who knows alice? (incoming edges)
person:alice<-knows<-person;

-- Backward traversal with field access
person:lead_infra<-reports_to<-person.name;

-- Find all direct reports (people who report to someone)
person:dir_platform<-reports_to<-person;

-- Backward traversal in SELECT
SELECT id, name, <-reports_to<-person.name AS direct_reports FROM person:lead_infra;

