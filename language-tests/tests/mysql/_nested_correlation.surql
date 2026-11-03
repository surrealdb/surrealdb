/**
[test]
run = false
reason = "Setup only: three users and three posts, two of them one user's, read by the nested_correlation tests"
*/

CREATE users:1 SET name = 'ann' RETURN NONE;
CREATE users:2 SET name = 'ben' RETURN NONE;
CREATE users:3 SET name = 'cy' RETURN NONE;
CREATE posts:10 SET owner_id = 1, title = 'x' RETURN NONE;
CREATE posts:11 SET owner_id = 1, title = 'y' RETURN NONE;
CREATE posts:12 SET owner_id = 2, title = 'z' RETURN NONE;
