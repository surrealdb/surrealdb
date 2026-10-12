/**
[env]
namespace = true
database = true

[test]
reason = "A field is defined only inside a defined field that can hold it: a named field inside an object, `*` inside an object, array or set, and an index inside an array or set"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `address.city`: the field `address` it is nested in is not defined. Define `address` first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `contact.phone.home`: the field `contact.phone` it is nested in is not defined. Define `contact.phone` first"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `age.years` in field `age` of type `int`: only an object holds a field"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `anything.inner` in field `anything` of type `any`: only an object holds a field"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `untyped.inner` in field `untyped` of type `any`: only an object holds a field"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `friend.name` in field `friend` of type `none | record<person>`: only an object holds a field"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `alias.first` in field `alias` of type `string | object`: only an object holds a field"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `tags.first` in field `tags` of type `array<string>`: only an object holds a field"

[[test.results]]
error = "Cannot define field `tags.*.first` in field `tags.*` of type `string`: only an object holds a field"

[[test.results]]
error = "Cannot define field `age.*` in field `age` of type `int`: only an object, array or set holds `*`"

[[test.results]]
error = "Cannot define field `address[0]` in field `address` of type `object`: only an array or set holds an index"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `body.text`: the field `body` it is nested in is not defined. Define `body` first"

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
error = "Cannot give field `body` the type `string`: field `body.title` is defined in it, and only an object holds a field"

[[test.results]]
error = "Cannot give field `body` the type `int`: field `body.title` is defined in it, and only an object holds a field"

[[test.results]]
error = "Cannot set field `body.title` with type `string` as it mismatched with field `body` with type `{ title: int }`"

[[test.results]]
value = "'DEFINE FIELD body ON doc TYPE object PERMISSIONS FULL'"

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
error = "Cannot give field `lines.*` the type `string`: field `lines.*.sku` is defined in it, and only an object holds a field"

[[test.results]]
value = "['DEFINE FIELD lines ON doc TYPE array<object> PERMISSIONS FULL', 'DEFINE FIELD lines.* ON doc TYPE object PERMISSIONS FULL']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['body', 'body.title', 'lines', 'lines.*']"
*/

DEFINE TABLE person SCHEMAFULL;

-- Every field a nested field is in must be defined first.
DEFINE FIELD address.city ON person TYPE string;
DEFINE FIELD address ON person TYPE object;
DEFINE FIELD address.city ON person TYPE string;
DEFINE FIELD contact ON person TYPE object;
DEFINE FIELD contact.phone.home ON person TYPE string;

-- Only an object holds a named field. `any`, and a field with no TYPE, hold
-- nothing: their value need not be an object.
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD age.years ON person;
DEFINE FIELD anything ON person TYPE any;
DEFINE FIELD anything.inner ON person;
DEFINE FIELD untyped ON person;
DEFINE FIELD untyped.inner ON person;
DEFINE FIELD friend ON person TYPE option<record<person>>;
DEFINE FIELD friend.name ON person;
DEFINE FIELD alias ON person TYPE string | object;
DEFINE FIELD alias.first ON person;
DEFINE FIELD tags ON person TYPE array<string>;
DEFINE FIELD tags.first ON person;
DEFINE FIELD tags[*].first ON person;

-- Only an object, array or set holds `*`, and only an array or set an index.
DEFINE FIELD age.* ON person;
DEFINE FIELD address[0] ON person;

-- An object, an optional object, a literal object, a map of objects, and the
-- elements of an array or set of objects each hold their fields.
DEFINE FIELD settings ON person TYPE option<object>;
DEFINE FIELD settings.theme ON person TYPE option<string>;
DEFINE FIELD point ON person TYPE option<{ x: int, y: int }>;
DEFINE FIELD point.x ON person TYPE int;
DEFINE FIELD meta ON person TYPE option<object>;
DEFINE FIELD meta.* ON person TYPE object;
DEFINE FIELD meta.*.name ON person TYPE string;
DEFINE FIELD items ON person TYPE option<array<object>>;
DEFINE FIELD items[*].sku ON person TYPE string;
DEFINE FIELD pair ON person TYPE option<array<int, 2>>;
DEFINE FIELD pair[1] ON person TYPE int;
DEFINE FIELD codes ON person TYPE option<set<object>>;
DEFINE FIELD codes[*].value ON person TYPE string;

-- A schemaless table holds its nested fields the same way.
DEFINE TABLE note SCHEMALESS;
DEFINE FIELD body.text ON note TYPE string;
DEFINE FIELD body ON note TYPE object;
DEFINE FIELD body.text ON note TYPE string;

-- A kind that cannot hold the fields defined in it is refused, whether it is
-- redefined or altered.
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD body ON doc TYPE object;
DEFINE FIELD body.title ON doc TYPE string;
DEFINE FIELD OVERWRITE body ON doc TYPE string;
ALTER FIELD body ON doc TYPE int;
DEFINE FIELD OVERWRITE body ON doc TYPE { title: int };
-- A refused statement leaves the definition it would have replaced.
(INFO FOR TABLE doc).fields.body;
DEFINE FIELD OVERWRITE body ON doc TYPE { title: string };
ALTER FIELD body ON doc TYPE option<object>;
ALTER FIELD body ON doc COMMENT 'the body';
DEFINE FIELD IF NOT EXISTS body.title ON doc TYPE int;

-- An array's elements are redefined with it, so a kind whose elements cannot
-- hold a field defined in them is refused too, until that field is removed.
DEFINE FIELD lines ON doc TYPE array<object>;
DEFINE FIELD lines[*].sku ON doc TYPE string;
DEFINE FIELD OVERWRITE lines ON doc TYPE array<string>;
[(INFO FOR TABLE doc).fields.lines, (INFO FOR TABLE doc).fields['lines.*']];
REMOVE FIELD lines[*].sku ON doc;
DEFINE FIELD OVERWRITE lines ON doc TYPE array<string>;
object::keys((INFO FOR TABLE doc).fields);
