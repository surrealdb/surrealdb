/**
[env]
namespace = false
database = false
clean = true

[test]
reason = "A block that removes and defines the namespace and database the session names resolves them again for the statements after it, which write to the new database"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: NONE, namespace: 'blk_ns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'blk_db', namespace: 'blk_ns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: 2 }]"

[[test.results]]
value = "['t']"

[[test.results]]
error = "The table 'gone' does not exist"

*/

DEFINE NAMESPACE blk_ns;
USE NS blk_ns;
DEFINE DATABASE blk_db;
USE NS blk_ns DB blk_db;

DEFINE TABLE gone;
{
    REMOVE NAMESPACE blk_ns;
    DEFINE NAMESPACE blk_ns;
    DEFINE DATABASE blk_db;
    DEFINE TABLE t;
    CREATE t:1 SET v = 2;
    RETURN SELECT * FROM t;
};
object::keys((INFO FOR DB).tables);
SELECT * FROM gone;
