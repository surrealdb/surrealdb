/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "type::field() must traverse record links, matching normal idiom evaluation"
issue = 6980

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ friend: person:friend, id: person:me, name: { first: 'John' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ friend: person:friend, id: person:me, name: { first: 'John' } }]"

[[test.results]]
value = "[{ friends_name: NONE }, { friends_name: 'Ben' }]"

[[test.results]]
value = "[{ friends_name: NONE }, { friends_name: 'Ben' }]"
*/

{
    DEFINE FIELD OVERWRITE name ON person TYPE object;
    DEFINE FIELD OVERWRITE name.first ON person TYPE string;
    DEFINE FIELD OVERWRITE friend ON person TYPE option<record<person>> REFERENCE;
    UPSERT person:friend SET name = { first: "Ben" };
    UPSERT person:me SET name = { first: "John" }, friend = person:friend;
    RETURN "OK";
};

-- Normal idiom traversal (baseline)
SELECT * FROM person WHERE friend.name.first = "Ben";

-- type::field must produce the same result
LET $field = "friend.name.first";
SELECT * FROM person WHERE type::field($field) = "Ben";

-- Projection: normal idiom
SELECT friend.name.first as friends_name FROM person;

-- Projection: type::field must match
SELECT type::field('friend.name.first') as friends_name FROM person;
