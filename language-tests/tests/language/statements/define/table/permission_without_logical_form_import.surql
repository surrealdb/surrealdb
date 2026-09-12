/**
[test]
reason = """
	Setup for permission_without_logical_form: a table and a field whose
	SELECT guards use a mock record set in scalar position, a table whose
	guard has a logical form, and the record user that triggers them.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE mock_guarded SCHEMALESS PERMISSIONS FOR select WHERE id IN |mock_guarded:1..=3|;
DEFINE FIELD tag ON mock_guarded PERMISSIONS FOR select WHERE id IN |mock_guarded:1..=2|;
CREATE mock_guarded:1 SET n = 1, tag = 'one';
CREATE mock_guarded:2 SET n = 2, tag = 'two';
CREATE mock_guarded:3 SET n = 3, tag = 'three';
CREATE mock_guarded:4 SET n = 4, tag = 'four';
CREATE mock_guarded:5 SET n = 5, tag = 'five';

DEFINE TABLE plain SCHEMALESS PERMISSIONS FOR select WHERE guarded = true;
CREATE plain:1 SET guarded = true, other = mock_guarded:1;
CREATE plain:2 SET guarded = false, other = mock_guarded:1;
