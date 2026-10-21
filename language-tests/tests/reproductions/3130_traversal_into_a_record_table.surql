/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A traversal that steps into the record table between hops (->e->t->f->t) and one that reads the edge's endpoint field (->e.out->f.out) reach the same records."
issue = 3130

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pokemon:1, pokemon:7], [pokemon:2, pokemon:8], [pokemon:3, pokemon:9], [pokemon:4, pokemon:10]]"

[[test.results]]
value = "[[pokemon:1, pokemon:7], [pokemon:2, pokemon:8], [pokemon:3, pokemon:9], [pokemon:4, pokemon:10]]"

[[test.results]]
value = "[[pokemon:1, pokemon:7], [pokemon:2, pokemon:8], [pokemon:3, pokemon:9], [pokemon:4, pokemon:10]]"
*/

FOR $t IN 0..6 { CREATE type::record('types', $t); };
FOR $t IN 0..6 { RELATE (type::record('types', $t))->double_damage_to->(type::record('types', ($t + 1) % 6)); };
FOR $p IN 0..12 { CREATE type::record('pokemon', $p); RELATE (type::record('pokemon', $p))->pokemon_type->(type::record('types', $p % 6)); };
FOR $m IN 0..4 { CREATE type::record('moves', $m); RELATE (type::record('moves', $m))->move_type->(type::record('types', $m)); };

SELECT VALUE array::sort(array::flatten(array::flatten(->move_type->types->double_damage_to->types<-pokemon_type<-pokemon))) FROM moves ORDER BY id;
SELECT VALUE array::sort(array::flatten(array::flatten(->move_type.out->double_damage_to.out<-pokemon_type<-pokemon))) FROM moves ORDER BY id;
SELECT VALUE array::sort(array::flatten(array::flatten(->move_type.out->double_damage_to.out<-pokemon_type.in))) FROM moves ORDER BY id;
