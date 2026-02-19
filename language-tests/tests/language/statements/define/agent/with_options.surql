/**
[test]
reason = "Test DEFINE AGENT with CONFIG object, multiple tools, and parameter descriptions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

-- Agent with CONFIG and multiple tools
DEFINE AGENT researcher
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'You are a research assistant. Search for articles and summarize them.'
    CONFIG {
        temperature: 0.5,
        max_tokens: 2000,
        top_p: 0.9,
        max_rounds: 10
    }
    TOOLS [
        {
            name: 'search_articles',
            description: 'Search the articles database',
            function: |$query: string, $limit: int| {
                SELECT * FROM articles WHERE title @@ $query LIMIT $limit
            },
            parameters: [
                { name: 'query', description: 'The search query text' },
                { name: 'limit', description: 'Maximum number of articles to return' }
            ]
        },
        {
            name: 'get_article',
            description: 'Get a specific article by ID',
            function: |$id: string| {
                SELECT * FROM articles WHERE id = type::thing('articles', $id)
            },
            parameters: [
                { name: 'id', description: 'The article record ID' }
            ]
        }
    ]
    COMMENT 'Research assistant agent with article tools';

-- Agent with no tools and no config (just a chat wrapper)
DEFINE AGENT simple_chat
    MODEL 'google:gemini-2.0-flash'
    PROMPT 'You answer questions briefly.';
