/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test social network style queries (friends of friends)"

[[test.results]]
value = "[person:alice, person:charlie, person:dana]"

[[test.results]]
value = "[{ direct_friends: [person:bob, person:ceo], friends_of_friends: [person:alice, person:charlie, person:dana] }]"

[[test.results]]
value = "[person:alice, person:bob, person:bob, person:alice, person:bob, person:charlie, person:bob, person:dana, person:bob, person:alice, person:charlie, person:alice, person:alice, person:bob, person:alice, person:ceo, person:bob, person:charlie, person:dana, person:charlie, person:charlie, person:alice, person:charlie, person:dana, person:bob, person:alice, person:charlie, person:alice, person:alice, person:bob, person:alice, person:ceo, person:bob, person:alice, person:charlie, person:alice, person:alice, person:bob, person:alice, person:ceo, person:alice, person:bob, person:bob, person:alice, person:bob, person:charlie, person:bob, person:dana, person:bob, person:alice, person:charlie, person:alice, person:alice, person:bob, person:alice, person:ceo, person:alice, person:ceo]"

[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:dana, person:charlie]"
*/

-- Friends of friends (2-hop knows)
person:alice->knows->person->knows->person;

-- Friends of friends excluding direct friends
SELECT 
    @->knows->person->knows->person AS friends_of_friends,
    @->knows->person AS direct_friends
FROM person:alice;

-- Mutual connections (bidirectional)
person:alice<->knows<->person<->knows<->person;

-- Extended network (3 degrees of separation)
person:alice.{1..3}->knows->person;

