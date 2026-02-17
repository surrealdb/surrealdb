/**
[test]
reason = "Test IF NOT EXISTS silently succeeds when the agent already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE AGENT greeter
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'You greet people warmly.'
    TOOLS [
        {
            name: 'get_greeting',
            description: 'Get a personalized greeting',
            function: |$name: string| {
                RETURN 'Welcome, ' + $name + '!'
            }
        }
    ];

-- Should silently succeed without error, original definition unchanged
DEFINE AGENT IF NOT EXISTS greeter
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'This prompt should be ignored since greeter already exists.';
