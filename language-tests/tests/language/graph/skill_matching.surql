/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test skill matching via shared skills traversal"

# 0
[[test.results]]
value = "[person:alice, person:alice, person:bob, person:lead_infra, person:alice, person:alice, person:bob, person:lead_infra]"

# 1
[[test.results]]
value = "[person:alice, person:bob, person:lead_infra, person:alice, person:bob, person:lead_infra]"

# 2
[[test.results]]
value = "[person:alice, person:bob, person:lead_infra]"

# 3
[[test.results]]
value = "[{ my_skills: ['Go', 'PostgreSQL', 'Redis', 'Rust'], people_with_same_skills: ['Alice Zhang', 'Alice Zhang', 'Bob Martinez', 'Priya Patel', 'Alice Zhang', 'Alice Zhang', 'Bob Martinez', 'Priya Patel'] }]"

# 4
[[test.results]]
value = "[person:alice, person:bob, person:lead_infra]"

# 5
[[test.results]]
value = "[person:alice]"

# 6
[[test.results]]
value = "[person:bob]"
*/

-- 0: People who share skills with alice
person:alice->has_skill->skill<-has_skill<-person;

-- 1: People who share expert-level skills
person:alice->(has_skill WHERE level = "expert")->skill<-has_skill<-person;

-- 2: People with specific skills (rust)
skill:rust<-has_skill<-person;

-- 3: People with overlapping skill sets (multiple skills)
SELECT 
    @->has_skill->skill.name AS my_skills,
    @->has_skill->skill<-has_skill<-person.name AS people_with_same_skills
FROM person:alice;

-- 4: Find all backend developers
skill:rust<-has_skill<-person;

-- 5: Find all Go developers
skill:go<-has_skill<-person;

-- 6: Find all Python developers
skill:python<-has_skill<-person;

