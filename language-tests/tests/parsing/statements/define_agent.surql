/**
[test]
reason = "Test that DEFINE AGENT syntax parses correctly with tools, config, and various options"

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

*/

-- Minimal agent definition
DEFINE AGENT minimal MODEL 'openai:gpt-4' PROMPT 'Hello';

-- Agent with CONFIG
DEFINE AGENT with_config
    MODEL 'openai:gpt-4'
    PROMPT 'Helper'
    CONFIG { temperature: 0.5, max_tokens: 1000 };

-- Agent with one tool
DEFINE AGENT with_tool
    MODEL 'openai:gpt-4'
    PROMPT 'Helper'
    TOOLS [
        {
            name: 'echo',
            description: 'Echo the input',
            function: |$input: string| {
                RETURN $input
            }
        }
    ];

-- Agent with tool + parameter descriptions + config
DEFINE AGENT full_agent
    MODEL 'openai:gpt-4'
    PROMPT 'Full agent'
    CONFIG { temperature: 0.7, max_tokens: 2000, top_p: 0.9, max_rounds: 10 }
    TOOLS [
        {
            name: 'search',
            description: 'Search records',
            function: |$query: string, $limit: int| {
                RETURN [$query, $limit]
            },
            parameters: [
                { name: 'query', description: 'Search text' },
                { name: 'limit', description: 'Max results' }
            ]
        }
    ]
    COMMENT 'A fully configured agent';

-- Remove agent parses correctly
REMOVE AGENT IF EXISTS full_agent;
