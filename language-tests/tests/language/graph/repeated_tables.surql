/**
[test]
reason = "A table a hop names more than once is read once for each time it is named, through an inline adjacency cache (member) or not (person)"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "[person:2, person:2, person:3, person:3]"

# 2
[[test.results]]
value = "[member:2, member:2, member:3, member:3]"

# 3
[[test.results]]
value = "[person:1, person:1]"

# 4
[[test.results]]
value = "[member:1, member:1]"

# 5
[[test.results]]
value = "[person:2, person:2, person:3, person:3, person:3, person:3, person:2, person:2, person:3, person:3]"

# 6
[[test.results]]
value = "[member:2, member:2, member:3, member:3, member:3, member:3, member:2, member:2, member:3, member:3]"

# 7
[[test.results]]
value = "[person:2, person:3, person:2, person:3]"

# 8
[[test.results]]
value = "[member:2, member:3, member:2, member:3]"

# 9
[[test.results]]
value = "[4, 5, 1]"

# 10
[[test.results]]
value = "[4, 5, 1]"

# 11
[[test.results]]
value = "2"

# 12
[[test.results]]
value = "2"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE member SCHEMALESS INLINE EDGES 16;
	DEFINE TABLE knows TYPE RELATION;
	DEFINE TABLE likes TYPE RELATION;
	CREATE person:1, person:2, person:3, member:1, member:2, member:3 RETURN NONE;
	RELATE person:1->knows:1->person:2 RETURN NONE;
	RELATE person:1->knows:2->person:3 RETURN NONE;
	RELATE person:1->likes:1->person:3 RETURN NONE;
	RELATE member:1->knows:3->member:2 RETURN NONE;
	RELATE member:1->knows:4->member:3 RETURN NONE;
	RELATE member:1->likes:2->member:3 RETURN NONE;
	RETURN NONE;
};

SELECT VALUE ->knows->(person, person) FROM ONLY person:1;
SELECT VALUE ->knows->(member, member) FROM ONLY member:1;
SELECT VALUE <-knows<-(person, person) FROM ONLY person:2;
SELECT VALUE <-knows<-(member, member) FROM ONLY member:2;
SELECT VALUE ->(knows, likes, knows)->(person, person) FROM ONLY person:1;
SELECT VALUE ->(knows, likes, knows)->(member, member) FROM ONLY member:1;
SELECT VALUE ->(knows, knows)->person FROM ONLY person:1;
SELECT VALUE ->(knows, knows)->member FROM ONLY member:1;
SELECT VALUE [count(->(knows, knows)), count(->(knows, likes, knows)), count(->likes)] FROM ONLY person:1;
SELECT VALUE [count(->(knows, knows)), count(->(knows, likes, knows)), count(->likes)] FROM ONLY member:1;
SELECT VALUE count(<-(knows, knows)) FROM ONLY person:2;
SELECT VALUE count(<-(knows, knows)) FROM ONLY member:2;
