/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "RELATION with 3-field UNIQUE index including option field NONE: index scan must return rows matching in/out (issue #7280)"
issue = 7280

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
value = "[{ id: project:test_project_1, name: 'Test Project' }]"

[[test.results]]
value = "[{ id: agent:test_agent_1, is_built_in: false, name: 'Test Agent' }]"

[[test.results]]
value = "[{ id: uses_agent:x, in: project:test_project_1, is_active: true, out: agent:test_agent_1 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ is_active: true }]"

[[test.results]]
value = "[{ is_active: true }]"

[[test.results]]
value = "[{ id: uses_agent:x, in: project:test_project_1, out: agent:test_agent_1 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: uses_agent:x, in: project:test_project_1, is_active: true, out: agent:test_agent_1, phase: 'execution' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ is_active: true }]"

*/

DEFINE TABLE OVERWRITE project SCHEMAFULL;
DEFINE FIELD OVERWRITE name ON project TYPE string;
DEFINE TABLE OVERWRITE agent SCHEMAFULL;
DEFINE FIELD OVERWRITE name ON agent TYPE string;
DEFINE FIELD OVERWRITE is_built_in ON agent TYPE bool;
DEFINE TABLE OVERWRITE uses_agent SCHEMAFULL TYPE RELATION FROM project TO agent ENFORCED;
DEFINE FIELD OVERWRITE is_active ON uses_agent TYPE bool DEFAULT false;
DEFINE FIELD OVERWRITE phase ON uses_agent TYPE option<string> DEFAULT NONE;
DEFINE INDEX OVERWRITE uses_agent_unique_idx ON uses_agent FIELDS in, out, phase UNIQUE;

CREATE project:test_project_1 CONTENT { name: 'Test Project' };
CREATE agent:test_agent_1 CONTENT { name: 'Test Agent', is_built_in: false };
RELATE project:test_project_1 -> uses_agent -> agent:test_agent_1 SET is_active = true;

-- Index plan: must find the NONE-phase edge (reported as [] in v3.0.4+ for some index paths — #7280)
SELECT is_active FROM uses_agent
WHERE in = type::record('project', 'test_project_1')
AND out = type::record('agent', 'test_agent_1');

-- Table scan: must find the row
SELECT is_active FROM uses_agent WITH NOINDEX
WHERE in = type::record('project', 'test_project_1')
AND out = type::record('agent', 'test_agent_1');

-- Graph traversal: must find the edge
SELECT id, in, out FROM project:test_project_1->uses_agent WHERE is_active = true;

-- Concrete phase value: create second edge with phase set, query by full key (sanity)
RELATE project:test_project_1 -> uses_agent -> agent:test_agent_1 SET is_active = true, phase = 'execution';
SELECT is_active FROM uses_agent
WHERE in = type::record('project', 'test_project_1')
AND out = type::record('agent', 'test_agent_1')
AND phase = 'execution';
