/**
[env]
namespace = true
database = true

[test]
reason = """
	While a MATERIALIZE INTO clause fills a destination, the destination field
	cannot be redefined, altered or removed, and its table cannot be removed;
	removing the referencing field or the clause frees them. A removed clause
	leaves the lists as plain data, which are no longer kept.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: author:a, posts: [] }]"

[[test.results]]
value = "[{ author: author:a, id: post:1 }]"

[[test.results]]
error = "An error occurred: Field `posts` of table `author` cannot be altered or removed while the MATERIALIZE INTO clause of field `author` of table `post` fills it"

[[test.results]]
error = "An error occurred: Field `posts` of table `author` cannot be altered or removed while the MATERIALIZE INTO clause of field `author` of table `post` fills it"

[[test.results]]
error = "An error occurred: Field `posts` of table `author` cannot be altered or removed while the MATERIALIZE INTO clause of field `author` of table `post` fills it"

[[test.results]]
error = "An error occurred: Table `author` cannot be removed while the MATERIALIZE INTO clause of field `author` of table `post` fills one of its fields"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: author:a, id: post:2 }]"

[[test.results]]
value = "[[post:1]]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
value = "[[post:1]]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
CREATE author:a;
CREATE post:1 SET author = author:a;
DEFINE FIELD OVERWRITE posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
ALTER FIELD posts ON author COMMENT 'x';
REMOVE FIELD posts ON author;
REMOVE TABLE author;
-- the clause goes with the REFERENCE it belongs to
ALTER FIELD author ON post DROP REFERENCE;
CREATE post:2 SET author = author:a;
SELECT VALUE posts FROM author:a;
ALTER FIELD posts ON author COMMENT 'x';
UPDATE author:a SET posts = [];
SELECT VALUE posts FROM author:a;
