/**
[test]
reason = "Tests type::geometry passthrough of geometry values and rejection of non-geometry inputs"

[[test.results]]
value = "(1.5f, 2.5f)"

[[test.results]]
value = "(1.5f, 2.5f)"

[[test.results]]
value = "{ type: 'Polygon', coordinates: [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 0f]]] }"

[[test.results]]
value = "{ type: 'GeometryCollection', geometries: [(0f, 0f)] }"

[[test.results]]
error = "Could not cast into `geometry` using input `'POINT(1 2)'`"

[[test.results]]
error = "Could not cast into `geometry` using input `[1.5f, 2.5f]`"

[[test.results]]
error = "Could not cast into `geometry` using input `NONE`"
*/

type::geometry((1.5, 2.5));
type::geometry({ type: "Point", coordinates: [1.5, 2.5] });
type::geometry({ type: "Polygon", coordinates: [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]] });
type::geometry({ type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0.0, 0.0] }] });
type::geometry("POINT(1 2)");
type::geometry([1.5, 2.5]);
type::geometry(NONE);
