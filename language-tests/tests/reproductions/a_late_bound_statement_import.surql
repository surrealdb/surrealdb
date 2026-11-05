/**
[test]
reason = """
	Setup for the late-bound reproductions: a function whose body holds a
	schema statement the plan cannot know runs, so the statements of a block
	after a call of it are planned late-bound. The schema statement never
	runs. Runs as owner.
"""
run = false
*/

DEFINE FUNCTION fn::late($when: string) { IF $when = 'never' { DEFINE TABLE late_never }; RETURN 0; } PERMISSIONS FULL;
