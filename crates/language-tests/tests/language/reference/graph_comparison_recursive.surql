/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: country:canada, next: [province:alberta] }]"

[[test.results]]
value = "[{ id: province:alberta, next: [city:calgary] }]"

[[test.results]]
value = "[{ id: city:calgary }]"

[[test.results]]
value = "[{ id: next:canada_alberta, in: country:canada, out: province:alberta }]"

[[test.results]]
value = "[{ id: next:alberta_calgary, in: province:alberta, out: city:calgary }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "{ id: country:canada, next: [{ id: province:alberta, next: [{ id: city:calgary, next: NONE }] }] }"

[[test.results]]
value = "{ id: country:canada, next: [{ id: province:alberta, next: [{ id: city:calgary, next: [] }] }] }"

*/

DEFINE FIELD next ON country TYPE array<record<province>> REFERENCE;
DEFINE FIELD next ON province TYPE array<record<city>> REFERENCE;

CREATE country:canada SET next = [province:alberta];
CREATE province:alberta SET next = [city:calgary];
CREATE city:calgary;

RELATE country:canada->next:canada_alberta->province:alberta;
RELATE province:alberta->next:alberta_calgary->city:calgary;

-- All these are identical output
city:calgary.{2}<-next<-? = city:calgary.{2}<~?;
country:canada.{2}->next->? = country:canada.{2}.next;
country:canada.{..+collect}.next = country:canada.{..+collect}->next->?;
country:canada.{..+path}.next = country:canada.{..+path}->next->?;
country:canada.{..+shortest=city:calgary}.next = country:canada.{..+shortest=city:calgary}->next->?;

-- These two are slightly different
country:canada.{..}.{
    id,
    next: next.@
};

country:canada.{..}.{
    id,
    next: ->next->?.@
};