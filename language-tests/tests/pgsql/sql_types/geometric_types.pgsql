/**
[env]
namespace = true
database = true

[test]
reason = "point, lseg, path, polygon, box, line and circle columns read their PostgreSQL text on write and on a cast from text, and a cast back to text prints it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: { type: 'Polygon', coordinates: [[[2f, 3f], [0f, 3f], [0f, 0f], [2f, 0f], [2f, 3f]]] }, c: { r: 3f, x: 1f, y: 2f }, id: 1, l: { a: 1f, b: -1f, c: 0f }, p: (1f, 2f), pa: { type: 'Polygon', coordinates: [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 0f]]] }, pg: { type: 'Polygon', coordinates: [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 1f], [0f, 0f]]] }, s: { type: 'LineString', coordinates: [[0f, 0f], [3f, 4f]] } }]"

[[test.results]]
value = "[{ b: { type: 'Polygon', coordinates: [[[2f, 3f], [0f, 3f], [0f, 0f], [2f, 0f], [2f, 3f]]] }, c: { r: 3f, x: 1f, y: 2f }, l: { a: 1f, b: -1f, c: 0f }, p: (1f, 2f), pa: { type: 'Polygon', coordinates: [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 0f]]] }, pg: { type: 'Polygon', coordinates: [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 1f], [0f, 0f]]] }, s: { type: 'LineString', coordinates: [[0f, 0f], [3f, 4f]] } }]"

[[test.results]]
value = "[{ p: (1.5f, -2f) }]"

[[test.results]]
value = "[{ b: '(2,3),(0,0)', c: '<(1,2),3>', p: '(1,2)' }]"

*/

CREATE TABLE shape (id int PRIMARY KEY, p point, s lseg, pa path, pg polygon, b box, l line, c circle);

INSERT INTO shape (id, p, s, pa, pg, b, l, c) VALUES
	(1, '(1,2)', '[(0,0),(3,4)]', '((0,0),(1,0),(1,1))', '((0,0),(1,0),(1,1),(0,1))', '(0,0),(2,3)', '{1,-1,0}', '<(1,2),3>');

SELECT p, s, pa, pg, b, l, c FROM shape;

SELECT '(1.5,-2)'::point AS p;

SELECT '( 1 , 2 )'::point::text AS p, '(0,0),(2,3)'::box::text AS b, '<(1,2),3>'::circle::text AS c;
