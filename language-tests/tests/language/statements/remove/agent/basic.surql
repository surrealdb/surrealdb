/**
[test]
reason = "Test REMOVE AGENT deletes the agent definition"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The agent 'support' does not exist"

*/

-- Define and then remove an agent
DEFINE AGENT support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'You are a customer support agent.'
    TOOLS [
        {
            name: 'lookup',
            description: 'Look up data',
            function: |$id: string| {
                RETURN $id
            }
        }
    ];

REMOVE AGENT support;

-- Should fail: agent no longer exists
REMOVE AGENT support;
