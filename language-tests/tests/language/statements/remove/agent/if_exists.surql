/**
[test]
reason = "Test REMOVE AGENT IF EXISTS does not error when agent does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

-- Define and remove
DEFINE AGENT temp_agent
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'Temporary agent.';

REMOVE AGENT IF EXISTS temp_agent;

-- Should silently succeed even though agent is already gone
REMOVE AGENT IF EXISTS temp_agent;
