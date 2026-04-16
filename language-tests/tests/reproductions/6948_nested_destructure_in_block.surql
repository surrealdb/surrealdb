/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Nested .* destructure inside a block must resolve record links"
issue = 6948

[[test.results]]
value = "{ city: { name: 'New York' }, name: 'Alice' }"
*/

{
  LET $city_id = CREATE ONLY city CONTENT { name: 'New York' } RETURN VALUE id;
  LET $id = CREATE ONLY person CONTENT { name: "Alice", city: $city_id } RETURN VALUE id;
  LET $data = $id.{ name, city.* };
  RETURN $data;
};
