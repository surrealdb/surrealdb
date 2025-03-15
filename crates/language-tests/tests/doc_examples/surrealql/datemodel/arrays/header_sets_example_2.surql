/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ cards: { clubs: ['queen', '10'], diamonds: ['king'], hearts: ['jack', '2'], spades: [] }, suits: ['clubs', 'diamonds', 'hearts', 'spades'] }]"

*/
DEFINE FIELD suits ON TABLE hand TYPE option<set<string>> VALUE object::keys(cards);

CREATE hand SET cards = {
    clubs: [
        "queen", "10"
    ],
    hearts: [
        "jack", "2"    
    ],
    spades: [
        
    ],
    diamonds: [
        "king"
    ]
} RETURN cards,suits;
