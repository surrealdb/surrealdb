/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "['First message', 'Second message', 'Third message']"

[[test.results]]
value = "['First message', 'Second message', 'Third message']"

[[test.results]]
value = "['Second message', 'Third message']"

[[test.results]]
value = "['Second message']"

[[test.results]]
value = "['Second message', 'Third message', 'Fourth message', 'Fifth message']"

[[test.results]]
value = "['First message', 'Second message', 'Third message', 'Fourth message', 'Fifth message']"

*/

CREATE chat:bla RETURN NONE;

DEFINE FIELD chat ON message TYPE record<chat> REFERENCE;

// The IDs would probably be ULIDs
INSERT INTO message [
    { id: 1, chat: chat:bla, message: "First message" },
    { id: 2, chat: chat:bla, message: "Second message" },
    { id: 3, chat: chat:bla, message: "Third message" },
    { id: 4, chat: chat:bla, message: "Fourth message" },
    { id: 5, chat: chat:bla, message: "Fifth message" },
] RETURN NONE;

chat:bla<~(message:..=3 FIELD chat).message;
chat:bla<~(message:1..=3 FIELD chat).message;
chat:bla<~(message:1>..=3 FIELD chat).message;
chat:bla<~(message:1>..3 FIELD chat).message;
chat:bla<~(message:1>.. FIELD chat).message;
chat:bla<~(message FIELD chat).message;