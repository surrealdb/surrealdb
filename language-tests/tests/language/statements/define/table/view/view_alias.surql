/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: moves_to:1, target: city:london }]"

[[test.results]]
value = "[{ id: moves_to:2, target: city:new_york }]"

[[test.results]]
value = "[{ city_id: city:london, count: 1, id: move_count:[city:london] }, { city_id: city:new_york, count: 1, id: move_count:[city:new_york] }]"

*/

define table moves_to;

define table move_count as
select
    target as city_id,
    count()
from moves_to
group by city_id;

create moves_to:1 set target = city:london;
create moves_to:2 set target = city:new_york;


/*
define table move_count_2 as
select
    out,
    count()
from moves_to
group by out;

select * from move_count_2;
*/
select * from move_count;
