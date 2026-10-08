/**
[test]
reason = """
	Setup for exists_permissions: a table a record user may select only some
	rows of, a table it may select none of, and a record whose object key
	holds an `id` component.
"""
run = false
*/
DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE partial SCHEMALESS PERMISSIONS FOR select WHERE visible = true;
DEFINE TABLE closed SCHEMALESS PERMISSIONS NONE;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;
CREATE open:1, partial:shown SET visible = true;
CREATE partial:hidden SET visible = false;
CREATE closed:1;
