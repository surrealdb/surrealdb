/**
[test]
run = false
reason = "Setup only: the users and posts of _nested_correlation, with every column declared, so that a column no table of a subquery has is a column of a query above it"
*/

DEFINE TABLE users SCHEMAFULL;
DEFINE FIELD name ON users TYPE string;
DEFINE TABLE posts SCHEMAFULL;
DEFINE FIELD owner_id ON posts TYPE int;
DEFINE FIELD title ON posts TYPE string;

CREATE users:1 SET name = 'ann' RETURN NONE;
CREATE users:2 SET name = 'ben' RETURN NONE;
CREATE users:3 SET name = 'cy' RETURN NONE;
CREATE posts:10 SET owner_id = 1, title = 'x' RETURN NONE;
CREATE posts:11 SET owner_id = 1, title = 'y' RETURN NONE;
CREATE posts:12 SET owner_id = 2, title = 'z' RETURN NONE;
