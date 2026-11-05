/**
[test]
reason = """
Setup for 3.02.04_record_id_datetime.surql: the old binary writes records whose
compound ids hold a Datetime, with and without fractional seconds, so the new
binary looks them up by keys an old writer produced."""
run = false
*/
CREATE reading:['a', d'2026-08-28T09:00:00Z'] SET n = 0;
CREATE reading:['a', d'2026-08-28T09:00:00.152Z'] SET n = 1;
CREATE reading:['a', d'2026-08-28T09:00:00.152300Z'] SET n = 2;
CREATE reading:{ at: d'2026-08-28T09:00:01Z', k: 'a' } SET n = 3;
