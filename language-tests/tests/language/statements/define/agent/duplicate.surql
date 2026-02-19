/**
[test]
reason = "Test that defining a duplicate agent errors, and OVERWRITE replaces it"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The agent 'support' already exists"

[[test.results]]
value = "NONE"

*/

DEFINE AGENT support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'Version 1'
    TOOLS [
        {
            name: 'greet',
            description: 'Greet a user',
            function: |$name: string| {
                RETURN 'Hello ' + $name
            }
        }
    ];

-- Should fail: agent already exists
DEFINE AGENT support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'Version 2';

-- Should succeed: OVERWRITE replaces the existing definition
DEFINE AGENT OVERWRITE support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'Version 2 (overwritten)';
