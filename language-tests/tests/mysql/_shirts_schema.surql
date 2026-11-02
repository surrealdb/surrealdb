/**
[test]
run = false
reason = "Setup only: a shirts table with an inline enum column and a prefs table with a set column, read by the MySQL enum ordering and SET tests"
*/

DEFINE TABLE shirts SCHEMAFULL;
DEFINE FIELD size ON shirts TYPE none | null | enum<'small', 'medium', 'large'>;
CREATE shirts:1 SET size = 'medium' RETURN NONE;
CREATE shirts:2 SET size = 'large' RETURN NONE;
CREATE shirts:3 SET size = 'small' RETURN NONE;
CREATE shirts:4 SET size = 'medium' RETURN NONE;
DEFINE TABLE prefs SCHEMAFULL;
DEFINE FIELD tags ON prefs TYPE none | null | set<enum<'c', 'b', 'a'>>;
