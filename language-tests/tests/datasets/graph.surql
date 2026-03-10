/**
[test]
reason = "Common graph dataset for testing graph queries, edge traversal, path traversal, recursive queries, and other graph-related features."

[[test.results]]
value = "NONE"
*/

/*
================================================================================
SCHEMA (ER Diagram)
================================================================================

  ┌────────┐  reports_to   ┌────────┐
  │ person │──────────────▶│ person │
  └────────┘               └────────┘
      │                         ▲
      │ member_of               │ mentors
      ▼                         │
  ┌────────┐    part_of    ┌────────┐
  │  team  │──────────────▶│  org   │◀──┐
  └────────┘               └────────┘   │ parent (self-ref)
      │                         └───────┘
      │ owned_by
      ▼
  ┌─────────┐  depends_on  ┌─────────┐
  │ project │─────────────▶│ project │ (includes cycle)
  └─────────┘              └─────────┘

  ┌────────┐   has_skill   ┌────────┐
  │ person │──────────────▶│ skill  │
  └────────┘               └────────┘

  ┌──────────┐             ┌──────────┐
  │ location │◀───────────▶│ location │ parent (self-ref)
  └──────────┘             └──────────┘

  ┌──────────┐             ┌──────────┐
  │ category │◀───────────▶│ category │ parent (self-ref)
  └──────────┘             └──────────┘

================================================================================
DATA HIERARCHY
================================================================================

Organizational Hierarchy (4 levels):
  org:company (root)
  └── org:engineering
      ├── org:platform
      │   └── org:infrastructure
      └── org:product

Management Chain (5 levels):
  person:ceo
  └── person:vp_eng
      ├── person:dir_platform
      │   └── person:lead_infra
      │       ├── person:alice
      │       └── person:bob
      └── person:dir_product
          └── person:lead_frontend
              ├── person:charlie
              └── person:dana

Teams (4):
  team:exec       -> org:company
  team:platform   -> org:platform
  team:infra      -> org:infrastructure
  team:frontend   -> org:product

Projects (4 with dependency cycle):
  project:api ──────▶ project:database
       ▲                    │
       │                    ▼
  project:frontend ◀── project:auth (cycle: auth depends on api)

Locations (3 levels):
  location:usa
  └── location:sf
      └── location:sf_hq
  location:uk
  └── location:london
      └── location:london_office

Categories (8 levels, for deep recursive traversal):
  category:tech
  ├── category:backend
  │   └── category:databases
  │       └── category:relational
  │           └── category:sql
  │               └── category:optimization
  │                   └── category:indexing
  │                       └── category:btree
  └── category:frontend

Skills (8):
  skill:rust, skill:typescript, skill:python, skill:go
  skill:postgresql, skill:redis, skill:react, skill:graphql

================================================================================
FEATURES EXERCISED
================================================================================

1.  Forward/backward traversal: ->reports_to->, <-reports_to<-
2.  Multi-hop with depth: person:alice.{2}->reports_to->?
3.  Recursive unbounded: org:company.{..}.children
4.  Shortest path: person:alice.{..+shortest=person:ceo}->reports_to->?
5.  Collect paths: org:infrastructure.{..+collect}.parent
6.  Edge filtering: ->(SELECT * FROM works_on WHERE hours > 20)->project
7.  Self-referential tables: org, location, category
8.  Reference fields with <~: manager on person, parent on org
9.  Bidirectional edges: knows edges in both directions
10. Edge properties for filtering: since, strength, hours, level, etc.
11. CYCLE: alice->knows->bob->knows->charlie->knows->alice
12. DIAMOND: alice->knows->bob->knows->dana AND alice->knows->charlie->knows->dana
13. DEEP NESTING (8 levels): category:tech..category:btree via parent field

*/

{
    -- ============================================================================
    -- SCHEMA DEFINITIONS
    -- ============================================================================
    DEFINE TABLE org SCHEMAFULL;
    DEFINE TABLE team SCHEMAFULL;
    DEFINE TABLE person SCHEMAFULL;
    DEFINE TABLE project SCHEMAFULL;
    DEFINE TABLE location SCHEMAFULL;
    DEFINE TABLE category SCHEMAFULL;
    DEFINE TABLE skill SCHEMAFULL;

    -- Organization units with self-referential hierarchy
    DEFINE FIELD parent ON org TYPE option<record<org>> REFERENCE;
    DEFINE FIELD children ON org TYPE array<record<org>> DEFAULT [];

    -- Teams belong to orgs
    DEFINE FIELD org ON team TYPE record<org> REFERENCE;

    -- People have managers (self-ref) and belong to teams
    DEFINE FIELD manager ON person TYPE option<record<person>> REFERENCE;
    DEFINE FIELD team ON person TYPE option<record<team>> REFERENCE;
    DEFINE FIELD location ON person TYPE option<record<location>> REFERENCE;

    -- Projects owned by teams with dependencies
    DEFINE FIELD team ON project TYPE option<record<team>> REFERENCE;

    -- Locations with self-referential hierarchy
    DEFINE FIELD name ON location TYPE string;
    DEFINE FIELD type ON location TYPE string;
    DEFINE FIELD parent ON location TYPE option<record<location>> REFERENCE;

    -- Categories with self-referential hierarchy (for recursive destructuring tests)
    DEFINE FIELD name ON category TYPE string;
    DEFINE FIELD parent ON category TYPE option<record<category>> REFERENCE;
    DEFINE FIELD children ON category TYPE array<record<category>> DEFAULT [];

    -- Skills
    DEFINE FIELD name ON skill TYPE string;
    DEFINE FIELD category ON skill TYPE string;

    -- Org additional fields
    DEFINE FIELD name ON org TYPE string;

    -- Team additional fields
    DEFINE FIELD name ON team TYPE string;

    -- Person additional fields
    DEFINE FIELD name ON person TYPE string;
    DEFINE FIELD title ON person TYPE string;
    DEFINE FIELD level ON person TYPE int;
    DEFINE FIELD hired ON person TYPE datetime;

    -- Project additional fields
    DEFINE FIELD name ON project TYPE string;
    DEFINE FIELD status ON project TYPE string;
    DEFINE FIELD priority ON project TYPE int;

    -- ============================================================================
    -- LOCATIONS (3-level hierarchy)
    -- ============================================================================

    CREATE location:usa SET name = "United States", type = "country", parent = NONE;
    CREATE location:sf SET name = "San Francisco", type = "city", parent = location:usa;
    CREATE location:sf_hq SET name = "SF Headquarters", type = "office", parent = location:sf;
    CREATE location:uk SET name = "United Kingdom", type = "country", parent = NONE;
    CREATE location:london SET name = "London", type = "city", parent = location:uk;
    CREATE location:london_office SET name = "London Office", type = "office", parent = location:london;

    -- ============================================================================
    -- CATEGORIES (8-level tree for deep recursive traversal)
    --
    --   tech (1)
    --   ├── backend (2)
    --   │   └── databases (3)
    --   │       └── relational (4)
    --   │           └── sql (5)
    --   │               └── optimization (6)
    --   │                   └── indexing (7)
    --   │                       └── btree (8)
    --   └── frontend (2)
    -- ============================================================================

    CREATE category:tech SET name = "Technology", parent = NONE, children = [category:backend, category:frontend];
    CREATE category:backend SET name = "Backend", parent = category:tech, children = [category:databases];
    CREATE category:databases SET name = "Databases", parent = category:backend, children = [category:relational];
    CREATE category:relational SET name = "Relational", parent = category:databases, children = [category:sql];
    CREATE category:sql SET name = "SQL", parent = category:relational, children = [category:optimization];
    CREATE category:optimization SET name = "Optimization", parent = category:sql, children = [category:indexing];
    CREATE category:indexing SET name = "Indexing", parent = category:optimization, children = [category:btree];
    CREATE category:btree SET name = "B-Tree", parent = category:indexing, children = [];
    CREATE category:frontend SET name = "Frontend", parent = category:tech, children = [];

    -- ============================================================================
    -- SKILLS
    -- ============================================================================

    CREATE skill:rust SET name = "Rust", category = "backend";
    CREATE skill:typescript SET name = "TypeScript", category = "frontend";
    CREATE skill:python SET name = "Python", category = "backend";
    CREATE skill:go SET name = "Go", category = "backend";
    CREATE skill:postgresql SET name = "PostgreSQL", category = "database";
    CREATE skill:redis SET name = "Redis", category = "database";
    CREATE skill:react SET name = "React", category = "frontend";
    CREATE skill:graphql SET name = "GraphQL", category = "api";

    -- ============================================================================
    -- ORGANIZATIONS (4-level hierarchy)
    -- ============================================================================

    CREATE org:company SET
        name = "Acme Corp",
        parent = NONE,
        children = [org:engineering];

    CREATE org:engineering SET
        name = "Engineering",
        parent = org:company,
        children = [org:platform, org:product];

    CREATE org:platform SET
        name = "Platform",
        parent = org:engineering,
        children = [org:infrastructure];

    CREATE org:infrastructure SET
        name = "Infrastructure",
        parent = org:platform,
        children = [];

    CREATE org:product SET
        name = "Product Engineering",
        parent = org:engineering,
        children = [];

    -- ============================================================================
    -- TEAMS
    -- ============================================================================

    CREATE team:exec SET name = "Executive", org = org:company;
    CREATE team:platform SET name = "Platform Team", org = org:platform;
    CREATE team:infra SET name = "Infrastructure Team", org = org:infrastructure;
    CREATE team:frontend SET name = "Frontend Team", org = org:product;

    -- ============================================================================
    -- PEOPLE (10 people across 5 management levels)
    -- ============================================================================

    -- Level 1: CEO
    CREATE person:ceo SET
        name = "Elena Rodriguez",
        title = "CEO",
        level = 1,
        hired = d"2015-01-15",
        manager = NONE,
        team = team:exec,
        location = location:sf_hq;

    -- Level 2: VP
    CREATE person:vp_eng SET
        name = "Marcus Chen",
        title = "VP Engineering",
        level = 2,
        hired = d"2016-03-20",
        manager = person:ceo,
        team = team:exec,
        location = location:sf_hq;

    -- Level 3: Directors
    CREATE person:dir_platform SET
        name = "Sarah Kim",
        title = "Director of Platform",
        level = 3,
        hired = d"2017-06-01",
        manager = person:vp_eng,
        team = team:platform,
        location = location:sf_hq;

    CREATE person:dir_product SET
        name = "James Wilson",
        title = "Director of Product Engineering",
        level = 3,
        hired = d"2017-09-15",
        manager = person:vp_eng,
        team = team:frontend,
        location = location:london_office;

    -- Level 4: Team Leads
    CREATE person:lead_infra SET
        name = "Priya Patel",
        title = "Infrastructure Lead",
        level = 4,
        hired = d"2018-02-01",
        manager = person:dir_platform,
        team = team:infra,
        location = location:sf_hq;

    CREATE person:lead_frontend SET
        name = "Alex Thompson",
        title = "Frontend Lead",
        level = 4,
        hired = d"2018-05-20",
        manager = person:dir_product,
        team = team:frontend,
        location = location:london_office;

    -- Level 5: Individual Contributors
    CREATE person:alice SET
        name = "Alice Zhang",
        title = "Senior Infrastructure Engineer",
        level = 5,
        hired = d"2019-01-10",
        manager = person:lead_infra,
        team = team:infra,
        location = location:sf_hq;

    CREATE person:bob SET
        name = "Bob Martinez",
        title = "Infrastructure Engineer",
        level = 5,
        hired = d"2020-03-15",
        manager = person:lead_infra,
        team = team:infra,
        location = location:sf_hq;

    CREATE person:charlie SET
        name = "Charlie Brown",
        title = "Senior Frontend Engineer",
        level = 5,
        hired = d"2019-07-22",
        manager = person:lead_frontend,
        team = team:frontend,
        location = location:london_office;

    CREATE person:dana SET
        name = "Dana Lee",
        title = "Frontend Engineer",
        level = 5,
        hired = d"2021-01-05",
        manager = person:lead_frontend,
        team = team:frontend,
        location = location:london_office;

    -- ============================================================================
    -- PROJECTS (4 projects with dependency cycle)
    -- ============================================================================

    CREATE project:database SET
        name = "Core Database",
        status = "active",
        team = team:infra,
        priority = 1;

    CREATE project:auth SET
        name = "Authentication Service",
        status = "active",
        team = team:infra,
        priority = 2;

    CREATE project:api SET
        name = "Public API",
        status = "active",
        team = team:platform,
        priority = 2;

    CREATE project:frontend SET
        name = "Web Frontend",
        status = "active",
        team = team:frontend,
        priority = 3;

    -- ============================================================================
    -- EDGE: reports_to (management hierarchy)
    --
    --                    ceo
    --                     ▲
    --                  vp_eng
    --                 ▲      ▲
    --         dir_platform  dir_product
    --              ▲             ▲
    --         lead_infra    lead_frontend
    --          ▲     ▲        ▲      ▲
    --       alice   bob   charlie   dana
    -- ============================================================================

    RELATE person:vp_eng->reports_to:vp_to_ceo->person:ceo SET since = d"2016-03-20";
    RELATE person:dir_platform->reports_to:dp_to_vp->person:vp_eng SET since = d"2017-06-01";
    RELATE person:dir_product->reports_to:dpr_to_vp->person:vp_eng SET since = d"2017-09-15";
    RELATE person:lead_infra->reports_to:li_to_dp->person:dir_platform SET since = d"2018-02-01";
    RELATE person:lead_frontend->reports_to:lf_to_dpr->person:dir_product SET since = d"2018-05-20";
    RELATE person:alice->reports_to:alice_to_li->person:lead_infra SET since = d"2019-01-10";
    RELATE person:bob->reports_to:bob_to_li->person:lead_infra SET since = d"2020-03-15";
    RELATE person:charlie->reports_to:charlie_to_lf->person:lead_frontend SET since = d"2019-07-22";
    RELATE person:dana->reports_to:dana_to_lf->person:lead_frontend SET since = d"2021-01-05";

    -- ============================================================================
    -- EDGE: knows (social connections with cycle and diamond patterns)
    --
    --   CYCLE: alice ──▶ bob ──▶ charlie ──▶ alice
    --
    --   DIAMOND: alice ──▶ bob ─────┐
    --                               ├──▶ dana
    --            alice ──▶ charlie ─┘
    --
    --   Other connections:
    --     dir_platform ◀══▶ dir_product
    --     lead_infra ◀════▶ lead_frontend
    --     alice ────▶ ceo (one-way)
    -- ============================================================================

    -- CYCLE: alice -> bob -> charlie -> alice
    RELATE person:alice->knows:alice_bob->person:bob SET since = d"2020-03-20", strength = "strong";
    RELATE person:bob->knows:bob_charlie->person:charlie SET since = d"2020-04-15", strength = "moderate";
    RELATE person:charlie->knows:charlie_alice->person:alice SET since = d"2020-06-15", strength = "moderate";

    -- DIAMOND: alice -> bob -> dana AND alice -> charlie -> dana
    -- (alice->bob already exists above, alice->charlie closes the cycle above)
    RELATE person:bob->knows:bob_dana->person:dana SET since = d"2021-02-01", strength = "weak";
    RELATE person:charlie->knows:charlie_dana->person:dana SET since = d"2021-01-10", strength = "strong";

    -- Reverse edges for bidirectional relationships
    RELATE person:bob->knows:bob_alice->person:alice SET since = d"2020-03-20", strength = "strong";
    RELATE person:dana->knows:dana_charlie->person:charlie SET since = d"2021-01-10", strength = "strong";

    -- Leadership connections
    RELATE person:lead_infra->knows:li_lf->person:lead_frontend SET since = d"2018-06-01", strength = "moderate";
    RELATE person:lead_frontend->knows:lf_li->person:lead_infra SET since = d"2018-06-01", strength = "moderate";
    RELATE person:dir_platform->knows:dp_dpr->person:dir_product SET since = d"2017-10-01", strength = "strong";
    RELATE person:dir_product->knows:dpr_dp->person:dir_platform SET since = d"2017-10-01", strength = "strong";

    -- One-directional (alice knows the CEO from a company event)
    RELATE person:alice->knows:alice_ceo->person:ceo SET since = d"2022-12-15", strength = "weak";

    -- ============================================================================
    -- EDGE: mentors (mentorship chains for recursive depth testing)
    --
    --   vp_eng ──▶ dir_platform ──▶ lead_infra ──▶ alice ──▶ bob
    --
    --                              lead_frontend ──▶ charlie ──▶ dana
    -- ============================================================================

    RELATE person:lead_infra->mentors:li_alice->person:alice SET area = "system design", started = d"2019-02-01";
    RELATE person:alice->mentors:alice_bob->person:bob SET area = "rust programming", started = d"2020-04-01";
    RELATE person:lead_frontend->mentors:lf_charlie->person:charlie SET area = "architecture", started = d"2019-08-01";
    RELATE person:charlie->mentors:charlie_dana->person:dana SET area = "react patterns", started = d"2021-02-01";
    RELATE person:dir_platform->mentors:dp_li->person:lead_infra SET area = "leadership", started = d"2018-03-01";
    RELATE person:vp_eng->mentors:vp_dp->person:dir_platform SET area = "strategy", started = d"2017-07-01";

    -- ============================================================================
    -- EDGE: works_on (person to project, many-to-many with properties)
    --
    --   lead_infra ────┬──▶ database ◀── alice ◀─┬── bob
    --                  │                         │
    --   dir_platform ──┼──▶ api ◀── charlie      │
    --                  │     ▲                   │
    --                  └─────┼───▶ auth ◀────────┘
    --                        │
    --   lead_frontend ───────┼──▶ frontend ◀── charlie ◀── dana
    -- ============================================================================

    -- Infrastructure team on database project
    RELATE person:alice->works_on:alice_db->project:database SET role = "lead", hours = 30;
    RELATE person:bob->works_on:bob_db->project:database SET role = "contributor", hours = 25;
    RELATE person:lead_infra->works_on:li_db->project:database SET role = "sponsor", hours = 5;

    -- Infrastructure team on auth project
    RELATE person:alice->works_on:alice_auth->project:auth SET role = "contributor", hours = 10;
    RELATE person:bob->works_on:bob_auth->project:auth SET role = "lead", hours = 30;

    -- Platform team on API project
    RELATE person:dir_platform->works_on:dp_api->project:api SET role = "sponsor", hours = 3;
    RELATE person:lead_infra->works_on:li_api->project:api SET role = "contributor", hours = 15;

    -- Frontend team on frontend project
    RELATE person:charlie->works_on:charlie_fe->project:frontend SET role = "lead", hours = 35;
    RELATE person:dana->works_on:dana_fe->project:frontend SET role = "contributor", hours = 30;
    RELATE person:lead_frontend->works_on:lf_fe->project:frontend SET role = "sponsor", hours = 5;

    -- Cross-team collaboration
    RELATE person:charlie->works_on:charlie_api->project:api SET role = "reviewer", hours = 5;

    -- ============================================================================
    -- EDGE: depends_on (project dependencies with cycle)
    --
    --   frontend ──hard──▶ api ──hard──▶ auth ──hard──▶ database
    --      │                │                              ▲
    --      └────soft────────┼──────────────▶───────────────┘
    --                       └────────soft──────────────────┘
    -- ============================================================================

    -- Linear dependencies
    RELATE project:auth->depends_on:auth_db->project:database SET type = "hard";
    RELATE project:api->depends_on:api_auth->project:auth SET type = "hard";
    RELATE project:frontend->depends_on:fe_api->project:api SET type = "hard";

    -- Create a cycle: api also depends on database directly
    RELATE project:api->depends_on:api_db->project:database SET type = "soft";

    -- Frontend also depends on auth for client-side auth
    RELATE project:frontend->depends_on:fe_auth->project:auth SET type = "soft";

    -- ============================================================================
    -- EDGE: has_skill (person to skill with proficiency)
    --
    --   alice ──────▶ rust, go, postgresql, redis
    --   bob ────────▶ rust, python, postgresql
    --   charlie ────▶ typescript, react, graphql
    --   dana ───────▶ typescript, react
    --   lead_infra ─▶ rust, postgresql
    --   lead_frontend ▶ typescript, react
    -- ============================================================================

    -- Alice's skills (infrastructure focus)
    RELATE person:alice->has_skill:alice_rust->skill:rust SET level = "expert", years = 5;
    RELATE person:alice->has_skill:alice_go->skill:go SET level = "advanced", years = 3;
    RELATE person:alice->has_skill:alice_pg->skill:postgresql SET level = "expert", years = 6;
    RELATE person:alice->has_skill:alice_redis->skill:redis SET level = "advanced", years = 4;

    -- Bob's skills (infrastructure focus, less experienced)
    RELATE person:bob->has_skill:bob_rust->skill:rust SET level = "intermediate", years = 2;
    RELATE person:bob->has_skill:bob_python->skill:python SET level = "advanced", years = 4;
    RELATE person:bob->has_skill:bob_pg->skill:postgresql SET level = "intermediate", years = 2;

    -- Charlie's skills (frontend focus)
    RELATE person:charlie->has_skill:charlie_ts->skill:typescript SET level = "expert", years = 6;
    RELATE person:charlie->has_skill:charlie_react->skill:react SET level = "expert", years = 5;
    RELATE person:charlie->has_skill:charlie_gql->skill:graphql SET level = "advanced", years = 3;

    -- Dana's skills (frontend, less experienced)
    RELATE person:dana->has_skill:dana_ts->skill:typescript SET level = "intermediate", years = 2;
    RELATE person:dana->has_skill:dana_react->skill:react SET level = "intermediate", years = 2;

    -- Lead skills (broader but less deep)
    RELATE person:lead_infra->has_skill:li_rust->skill:rust SET level = "advanced", years = 4;
    RELATE person:lead_infra->has_skill:li_pg->skill:postgresql SET level = "advanced", years = 5;
    RELATE person:lead_frontend->has_skill:lf_ts->skill:typescript SET level = "advanced", years = 4;
    RELATE person:lead_frontend->has_skill:lf_react->skill:react SET level = "advanced", years = 4;

    -- ============================================================================
    -- EDGE: part_of (team to org hierarchy via edges)
    --
    --   team:exec ─────────▶ org:company
    --   team:platform ─────▶ org:platform
    --   team:infra ────────▶ org:infrastructure
    --   team:frontend ─────▶ org:product
    -- ============================================================================

    RELATE team:exec->part_of:exec_company->org:company;
    RELATE team:platform->part_of:platform_org->org:platform;
    RELATE team:infra->part_of:infra_org->org:infrastructure;
    RELATE team:frontend->part_of:frontend_org->org:product;

    RETURN NONE;
}