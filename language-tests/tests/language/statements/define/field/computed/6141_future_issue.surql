## https://github.com/surrealdb/surrealdb/issues/6141

/**

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
value = "{ bar: 1, foo: 1, foobar: 'foo', id: bug:1 }"

[[test.results]]
value = "{ bar: 2, foo: 2, foobar: 'bar', id: bug:2 }"

[[test.results]]
value = "[{ bar: 1, foo: 1, foobar: 'foo', id: bug:1 }, { bar: 2, foo: 2, foobar: 'bar', id: bug:2 }]"
*/

REMOVE TABLE IF EXISTS bug;
DEFINE TABLE IF NOT EXISTS bug SCHEMAFULL;
DEFINE FIELD foobar ON TABLE bug TYPE "foo"|"bar";

## Computed fields
DEFINE FIELD OVERWRITE foo ON TABLE bug COMPUTED {
    IF foobar == "foo" THEN
        RETURN 1;
    END;
    RETURN 2;
} COMMENT "This IF-END syntax always resolves to 2";

DEFINE FIELD OVERWRITE bar ON TABLE bug COMPUTED {
    IF foobar == "foo" {
        RETURN 1;
    };
    RETURN 2;
} COMMENT "This new-scope syntax works properly";

# Fill Test data
UPSERT ONLY bug:1 CONTENT {
    foobar: "foo",
};
UPSERT ONLY bug:2 CONTENT {
    foobar: "bar",
};
# Show data
SELECT * FROM bug;