// Similar to 2946 and 2988 but more complex
//
// https://github.com/surrealdb/surrealdb/issues/4881

/**
[test]
issue = 4881

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ day: d'2025-10-04T00:00:00Z', id: daily_resource_usage:[organisation:one, resource_type:seat, d'2025-10-04T00:00:00Z'], organisation: organisation:one, resource_type: resource_type:seat, total_count: 4f }, { day: d'2025-10-05T00:00:00Z', id: daily_resource_usage:[organisation:one, resource_type:seat, d'2025-10-05T00:00:00Z'], organisation: organisation:one, resource_type: resource_type:seat, total_count: 4f }]"

*/

{
    DEFINE TABLE resource_type SCHEMAFULL;
    CREATE resource_type:seat RETURN NONE;

    DEFINE TABLE resource SCHEMAFULL;
    DEFINE FIELD organisation ON resource TYPE record<organisation>;
    DEFINE FIELD resource_type ON resource TYPE record<resource_type> DEFAULT resource_type:seat;
    DEFINE FIELD entitlement_usage ON resource TYPE number DEFAULT 1;
    DEFINE FIELD created_at ON resource TYPE datetime DEFAULT time::now();

    LET $organisation = CREATE ONLY organisation:one SET name = "Some organisation";
    CREATE resource:1 SET organisation = $organisation.id, created_at = d'2025-10-04T10:20:30Z';
    CREATE resource:2 SET organisation = $organisation.id, created_at = d'2025-10-04T10:20:31Z';
    CREATE resource:3 SET organisation = $organisation.id, created_at = d'2025-10-04T10:30:30Z';
    CREATE resource:4 SET organisation = $organisation.id, created_at = d'2025-10-04T12:20:30Z';
    CREATE resource:5 SET organisation = $organisation.id, created_at = d'2025-10-05T10:20:30Z';
    CREATE resource:6 SET organisation = $organisation.id, created_at = d'2025-10-05T10:20:31Z';
    CREATE resource:7 SET organisation = $organisation.id, created_at = d'2025-10-05T10:30:30Z';
    CREATE resource:8 SET organisation = $organisation.id, created_at = d'2025-10-05T12:20:30Z';

    DEFINE TABLE daily_resource_usage AS
        SELECT
            organisation,
            resource_type,
            math::sum(<float> entitlement_usage) as total_count,
            time::group(created_at, "day") as day
        FROM resource
        GROUP BY organisation, resource_type, day;

    RETURN "OK";
};

SELECT * FROM daily_resource_usage;
