/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY over columns a FROM subquery computes, one of them a sum over a parameter correlated through $parent, with a LIMIT keeps the first rows of the full order: the rows match the same order applied to the gathered rows."
issue = 5735

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bucket: 2f, id: person:4, total: 3 }, { bucket: 2f, id: person:6, total: 1 }, { bucket: 3f, id: person:7, total: 3 }, { bucket: 3f, id: person:3, total: 1 }]"

[[test.results]]
value = "[{ bucket: 2f, id: person:4, total: 3 }, { bucket: 2f, id: person:6, total: 1 }, { bucket: 3f, id: person:7, total: 3 }, { bucket: 3f, id: person:3, total: 1 }]"
*/

FOR $f IN 0..6 { CREATE type::record('feature', $f) SET name = 'f' + <string>$f; };
FOR $u IN 0..12 {
	CREATE type::record('person', $u) SET first_name = 'u' + <string>$u, location = <point> [$u * 0.5, 0.0];
	FOR $k IN 0..2 { RELATE (type::record('person', $u))->has_feature->(type::record('feature', ($u + $k * 3) % 6)); };
};
FOR $k IN 0..3 { RELATE person:5->likes_feature->(type::record('feature', $k * 2)) SET weight = $k + 1; };

LET $source = person:5;
LET $liked = ($source->likes_feature.{ out, weight });
LET $rows = SELECT id, name, math::sum(SELECT VALUE weight FROM $liked WHERE out IN $parent.features) AS total, bucket FROM (
	SELECT id, first_name AS name, ->has_feature->feature AS features, math::ceil(geo::distance($source.location, location) / 50000) AS bucket FROM person WHERE id != $source.id
);
SELECT id, total, bucket FROM (
	SELECT id, name, math::sum(SELECT VALUE weight FROM $liked WHERE out IN $parent.features) AS total, bucket FROM (
		SELECT id, first_name AS name, ->has_feature->feature AS features, math::ceil(geo::distance($source.location, location) / 50000) AS bucket FROM person WHERE id != $source.id
	)
) ORDER BY bucket, total DESC, id LIMIT 4;
RETURN (SELECT id, total, bucket FROM $rows ORDER BY bucket, total DESC, id LIMIT 4);
