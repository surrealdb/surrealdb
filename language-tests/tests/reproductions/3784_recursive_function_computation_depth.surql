/**
[test]
reason = "Ensure recursive functions work similarly when called directly vs in SELECT queries"
issue = 3784

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ caption: 'dir', children: [], id: tree:dir }]"

[[test.results]]
value = "[{ caption: 'to', children: [tree:dir], id: tree:to }]"

[[test.results]]
value = "[{ caption: 'path', children: [tree:to], id: tree:path }]"

[[test.results]]
value = "[{ caption: 'root', children: [tree:path], id: tree:root }]"

[[test.results]]
value = "'/'"

[[test.results]]
value = '''[{ "fn::build_tree_path": '/path/to/dir' }, { "fn::build_tree_path": '/path' }, { "fn::build_tree_path": '/' }, { "fn::build_tree_path": '/path/to' }]'''

[env]
namespace = "test"
database = "test"

*/

DEFINE TABLE tree SCHEMAFULL;
DEFINE FIELD caption ON TABLE tree TYPE string;
DEFINE FIELD children ON TABLE tree TYPE array<record<tree>> DEFAULT [];

DEFINE FUNCTION OVERWRITE fn::build_tree_path($node: record) {
    // Get the children for this node
    LET $temp = (SELECT * FROM ONLY tree WHERE children CONTAINS $node LIMIT 1);
    // Return the path or keep recursing
    RETURN IF $node = tree:root THEN
        "/";
	ELSE IF $temp.id = tree:root THEN
        string::concat("/", $node.caption);
	ELSE
        string::join('/', fn::build_tree_path($temp.id), $node.caption);
	END;
};

CREATE tree:dir SET caption = "dir", children = [];
CREATE tree:to SET caption = "to", children = [tree:dir];
CREATE tree:path SET caption = "path", children = [tree:to];
CREATE tree:root SET caption = "root", children = [tree:path];

fn::build_tree_path(tree:root);

SELECT fn::build_tree_path(id) FROM tree;
