/**
[test]
reason = "Test basic DEFINE AGENT with inline TOOLS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

-- Define an agent with an inline tool
DEFINE AGENT support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'You are a helpful customer support agent.'
    TOOLS [
        {
            name: 'lookup_order',
            description: 'Look up order details by order number',
            function: |$order_id: string| {
                RETURN 'Order ' + $order_id + ' is shipped'
            }
        }
    ];

-- IF NOT EXISTS should be a no-op when agent already exists
DEFINE AGENT IF NOT EXISTS support
    MODEL 'openai:gpt-4-turbo'
    PROMPT 'This should not replace the original.';
