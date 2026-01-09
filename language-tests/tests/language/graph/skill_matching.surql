/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test skill matching via shared skills traversal"

[[test.results]]
value = "[person:alice, person:alice, person:bob, person:lead_infra, person:alice, person:alice, person:bob, person:lead_infra]"

[[test.results]]
value = "[person:alice, person:bob, person:lead_infra, person:alice, person:bob, person:lead_infra]"

[[test.results]]
value = "[person:alice, person:bob, person:lead_infra]"

[[test.results]]
value = "[{ my_skills: ['Go', 'PostgreSQL', 'Redis', 'Rust'], people_with_same_skills: ['Alice Zhang', 'Alice Zhang', 'Bob Martinez', 'Priya Patel', 'Alice Zhang', 'Alice Zhang', 'Bob Martinez', 'Priya Patel'] }]"

[[test.results]]
value = "[person:alice, person:bob, person:lead_infra]"

[[test.results]]
value = "[person:alice]"

[[test.results]]
value = "[person:bob]"
*/

-- People who share skills with alice
person:alice->has_skill->skill<-has_skill<-person;

-- People who share expert-level skills
person:alice->(has_skill WHERE level = "expert")->skill<-has_skill<-person;

-- People with specific skills (rust)
skill:rust<-has_skill<-person;

-- People with overlapping skill sets (multiple skills)
SELECT 
    @->has_skill->skill.name AS my_skills,
    @->has_skill->skill<-has_skill<-person.name AS people_with_same_skills
FROM person:alice;

-- Find all backend developers
skill:rust<-has_skill<-person;
skill:go<-has_skill<-person;
skill:python<-has_skill<-person;

