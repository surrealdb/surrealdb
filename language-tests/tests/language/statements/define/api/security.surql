/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 6734
reason = "SECURITY INVOKER is part of the stored definition and of its rendering, which is what INFO and EXPORT show and what an import replays; SECURITY DEFINER is the default and renders as no clause. ALTER API changes the mode and nothing else changes it, and DEFINE API OVERWRITE without the clause resets it to the default rather than inheriting the old one."

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
value = "NONE"

[[test.results]]
value = """{ "/comment_only": "DEFINE API '/comment_only' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } } SECURITY INVOKER COMMENT 'unrelated'", "/commented": "DEFINE API '/commented' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } } FOR post PERMISSIONS FULL THEN { RETURN { status: 201 } } SECURITY INVOKER COMMENT 'c'", "/default": "DEFINE API '/default' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } }", "/definer": "DEFINE API '/definer' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } }", "/invoker": "DEFINE API '/invoker' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } } SECURITY INVOKER", "/overwritten": "DEFINE API '/overwritten' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } }", "/redefined": "DEFINE API '/redefined' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } } SECURITY INVOKER", "/to_definer": "DEFINE API '/to_definer' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } }", "/to_invoker": "DEFINE API '/to_invoker' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } } SECURITY INVOKER" }"""

[[test.results]]
value = "[{ path: '/comment_only', security: 'invoker' }, { path: '/commented', security: 'invoker' }, { path: '/default', security: 'definer' }, { path: '/definer', security: 'definer' }, { path: '/invoker', security: 'invoker' }, { path: '/overwritten', security: 'definer' }, { path: '/redefined', security: 'invoker' }, { path: '/to_definer', security: 'definer' }, { path: '/to_invoker', security: 'invoker' }]"

*/

DEFINE API "/invoker" FOR get THEN { RETURN { status: 200 } } SECURITY INVOKER;
DEFINE API "/definer" FOR get THEN { RETURN { status: 200 } } SECURITY DEFINER;
DEFINE API "/default" FOR get THEN { RETURN { status: 200 } };
DEFINE API "/commented" FOR get THEN { RETURN { status: 200 } } COMMENT "c" SECURITY INVOKER;
DEFINE API "/to_invoker" FOR get THEN { RETURN { status: 200 } };
DEFINE API "/to_definer" FOR get THEN { RETURN { status: 200 } } SECURITY INVOKER;
DEFINE API "/comment_only" FOR get THEN { RETURN { status: 200 } } SECURITY INVOKER;
DEFINE API "/overwritten" FOR get THEN { RETURN { status: 200 } } SECURITY INVOKER;
DEFINE API "/redefined" FOR get THEN { RETURN { status: 200 } } SECURITY INVOKER;
ALTER API "/to_invoker" SECURITY INVOKER;
ALTER API "/to_definer" SECURITY DEFINER;
ALTER API "/comment_only" COMMENT "unrelated";
ALTER API "/commented" FOR post THEN { RETURN { status: 201 } };
DEFINE API OVERWRITE "/overwritten" FOR get THEN { RETURN { status: 200 } };
DEFINE API IF NOT EXISTS "/redefined" FOR get THEN { RETURN { status: 200 } };
(INFO FOR DB).apis;
(INFO FOR DB STRUCTURE).apis.map(|$a| { path: $a.path, security: $a.security ?? 'definer' });
