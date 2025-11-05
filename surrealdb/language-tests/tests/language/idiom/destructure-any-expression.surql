/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ doc: { id: place:{ name: 'Earth' }, level: 1 }, double_level: 2, rels: [{ doc: { id: place:{ name: 'US' }, level: 2 }, double_level: 4, rels: [{ doc: { id: place:{ name: 'NYC' }, level: 3 }, double_level: 6, rels: [] }] }] }"

*/
{
    DELETE place, contains;

    UPSERT place:{ name: "Earth" } SET level = 1;
    UPSERT place:{ name: "US" } SET level = 2;
    UPSERT place:{ name: "NYC" } SET level = 3;

    RELATE place:{ name: "Earth" }->contains->place:{ name: "US" };
    RELATE place:{ name: "US" }->contains->place:{ name: "NYC" };
	NONE
};

place:{ name: "Earth" }.{..}.{
    doc: $this,
    rels: ->?->?.@,
    double_level: level * 2
}
