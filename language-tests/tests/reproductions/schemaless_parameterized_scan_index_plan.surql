/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = """
Ensure the new executor correctly plans index usage on a SCHEMALESS table \
with computed fields and multiple indexes, exercised through parameterized \
OData V4-style queries. The V4 connector cannot distinguish strings from \
record IDs in plain JSON, so the WHERE clause checks both: \
type::field($field) IN [$param, type::record($param)].
"""

[[test.results]]
value = "'OK'"

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

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}\n    Filter [ctx: Db] [predicate: type::field(...) INSIDE ['scan:01kg1bsd1hbtsnq1b57zz3zm55', scan:01kg1bsd1hbtsnq1b57zz3zm55]] {rows: 2}\n        UnionIndexScan [ctx: Db] [table: finding, branches: 2] {rows: 2}\n            IndexScan [ctx: Db] [index: idx_scan, access: = 'scan:01kg1bsd1hbtsnq1b57zz3zm55', direction: Forward] {rows: 0}\n            IndexScan [ctx: Db] [index: idx_scan, access: = scan:01kg1bsd1hbtsnq1b57zz3zm55, direction: Forward] {rows: 2}\n\nTotal rows: 2"'''

[[test.results]]
value = "[{ advisories: [], annotation: NONE, category: 'crypto', classicSeverity: 'high', classicWeaknessType: 'CWE-326', confidence: 'high', cryptoVulnerabilityType: 'key-size', guidance: 'Upgrade to 4096-bit RSA or switch to ECDSA', id: finding:1, links: [], location: 'src/tls.rs:42', potentialSideEffects: NONE, quantumSeverity: 'critical', quantumWeaknessType: 'QWE-1', remediationJobs: [], remediations: [], replacementCode: NONE, replacementCodeLanguage: NONE, replacementConfidence: 'medium', ruleId: 'CRYPTO-001', scan: scan:01kg1bsd1hbtsnq1b57zz3zm55, summary: 'RSA key is below 2048 bits', title: 'Weak RSA key size' }, { advisories: ['CVE-2021-1234'], annotation: 'Known legacy requirement', category: 'transport', classicSeverity: 'medium', classicWeaknessType: 'CWE-327', confidence: 'high', cryptoVulnerabilityType: NONE, guidance: 'Enforce minimum TLS 1.2', id: finding:2, links: ['https://example.com/advisory'], location: 'src/server.rs:88', potentialSideEffects: 'May break legacy clients', quantumSeverity: 'medium', quantumWeaknessType: NONE, remediationJobs: [], remediations: [], replacementCode: 'set_min_tls_version(TLS_1_3)', replacementCodeLanguage: 'rust', replacementConfidence: 'high', ruleId: 'TLS-002', scan: scan:01kg1bsd1hbtsnq1b57zz3zm55, summary: 'TLS 1.0 is in use', title: 'Deprecated TLS version' }]"
*/

-- Schema: SCHEMALESS finding table with computed fields and a representative
-- subset of indexes, modelling a real OData V4 connector workload.
{
    DEFINE TABLE scan;
    DEFINE TABLE finding SCHEMALESS;

    -- Computed fields (known to interact with new query planner planning)
    DEFINE FIELD displayTitle ON finding VALUE string::concat(title, ' - ', category);
    DEFINE FIELD severityLabel ON finding VALUE string::concat(classicSeverity, '/', quantumSeverity);

    -- Representative subset of indexes (real schema has 18)
    DEFINE INDEX idx_scan ON finding FIELDS scan;
    DEFINE INDEX idx_title ON finding FIELDS title;
    DEFINE INDEX idx_category ON finding FIELDS category;
    DEFINE INDEX idx_ruleId ON finding FIELDS ruleId;
    DEFINE INDEX idx_classicSeverity ON finding FIELDS classicSeverity;
    DEFINE INDEX idx_confidence ON finding FIELDS confidence;

    -- Test data
    CREATE scan:01kg1bsd1hbtsnq1b57zz3zm55 SET name = "security audit";

    CREATE finding:1 SET
        title = "Weak RSA key size",
        category = "crypto",
        cryptoVulnerabilityType = "key-size",
        summary = "RSA key is below 2048 bits",
        location = "src/tls.rs:42",
        classicSeverity = "high",
        classicWeaknessType = "CWE-326",
        annotation = NONE,
        quantumSeverity = "critical",
        quantumWeaknessType = "QWE-1",
        ruleId = "CRYPTO-001",
        scan = scan:01kg1bsd1hbtsnq1b57zz3zm55,
        confidence = "high",
        advisories = [],
        links = [],
        potentialSideEffects = NONE,
        replacementConfidence = "medium",
        replacementCode = NONE,
        guidance = "Upgrade to 4096-bit RSA or switch to ECDSA",
        replacementCodeLanguage = NONE,
        remediations = [],
        remediationJobs = [];

    CREATE finding:2 SET
        title = "Deprecated TLS version",
        category = "transport",
        cryptoVulnerabilityType = NONE,
        summary = "TLS 1.0 is in use",
        location = "src/server.rs:88",
        classicSeverity = "medium",
        classicWeaknessType = "CWE-327",
        annotation = "Known legacy requirement",
        quantumSeverity = "medium",
        quantumWeaknessType = NONE,
        ruleId = "TLS-002",
        scan = scan:01kg1bsd1hbtsnq1b57zz3zm55,
        confidence = "high",
        advisories = ["CVE-2021-1234"],
        links = ["https://example.com/advisory"],
        potentialSideEffects = "May break legacy clients",
        replacementConfidence = "high",
        replacementCode = "set_min_tls_version(TLS_1_3)",
        guidance = "Enforce minimum TLS 1.2",
        replacementCodeLanguage = "rust",
        remediations = [],
        remediationJobs = [];

    CREATE finding:3 SET
        title = "Hardcoded secret",
        category = "secrets",
        cryptoVulnerabilityType = NONE,
        summary = "API key embedded in source",
        location = "src/config.rs:15",
        classicSeverity = "critical",
        classicWeaknessType = "CWE-798",
        annotation = NONE,
        quantumSeverity = "critical",
        quantumWeaknessType = NONE,
        ruleId = "SEC-010",
        scan = scan:otherscan,
        confidence = "medium",
        advisories = [],
        links = [],
        potentialSideEffects = NONE,
        replacementConfidence = NONE,
        replacementCode = NONE,
        guidance = "Use environment variables or a secrets manager",
        replacementCodeLanguage = NONE,
        remediations = [],
        remediationJobs = [];

    RETURN "OK";
};

-- OData V4 parameterized query: the connector emits parameterized field names
-- and checks both string and record-id forms since it consumes plain JSON.
LET $field1 = "scan";
LET $param1 = "scan:01kg1bsd1hbtsnq1b57zz3zm55";
LET $select0 = "id";
LET $select1 = "title";
LET $select2 = "category";
LET $select3 = "cryptoVulnerabilityType";
LET $select4 = "summary";
LET $select5 = "location";
LET $select6 = "classicSeverity";
LET $select7 = "classicWeaknessType";
LET $select8 = "annotation";
LET $select9 = "quantumSeverity";
LET $select10 = "quantumWeaknessType";
LET $select11 = "ruleId";
LET $select12 = "scan";
LET $select13 = "confidence";
LET $select14 = "advisories";
LET $select15 = "links";
LET $select16 = "potentialSideEffects";
LET $select17 = "replacementConfidence";
LET $select18 = "replacementCode";
LET $select19 = "guidance";
LET $select20 = "replacementCodeLanguage";
LET $select21 = "remediations";
LET $select22 = "remediationJobs";
LET $table = "finding";

-- Verify the new executor plans index usage correctly
EXPLAIN ANALYZE SELECT
    type::field($select0) AS `id`,
    type::field($select1) AS `title`,
    type::field($select2) AS `category`,
    type::field($select3) AS `cryptoVulnerabilityType`,
    type::field($select4) AS `summary`,
    type::field($select5) AS `location`,
    type::field($select6) AS `classicSeverity`,
    type::field($select7) AS `classicWeaknessType`,
    type::field($select8) AS `annotation`,
    type::field($select9) AS `quantumSeverity`,
    type::field($select10) AS `quantumWeaknessType`,
    type::field($select11) AS `ruleId`,
    type::field($select12) AS `scan`,
    type::field($select13) AS `confidence`,
    type::field($select14) AS `advisories`,
    type::field($select15) AS `links`,
    type::field($select16) AS `potentialSideEffects`,
    type::field($select17) AS `replacementConfidence`,
    type::field($select18) AS `replacementCode`,
    type::field($select19) AS `guidance`,
    type::field($select20) AS `replacementCodeLanguage`,
    type::field($select21) AS `remediations`,
    type::field($select22) AS `remediationJobs`
FROM type::table($table)
WHERE type::field($field1) IN [$param1, type::record($param1)];

-- Actual data query
SELECT
    type::field($select0) AS `id`,
    type::field($select1) AS `title`,
    type::field($select2) AS `category`,
    type::field($select3) AS `cryptoVulnerabilityType`,
    type::field($select4) AS `summary`,
    type::field($select5) AS `location`,
    type::field($select6) AS `classicSeverity`,
    type::field($select7) AS `classicWeaknessType`,
    type::field($select8) AS `annotation`,
    type::field($select9) AS `quantumSeverity`,
    type::field($select10) AS `quantumWeaknessType`,
    type::field($select11) AS `ruleId`,
    type::field($select12) AS `scan`,
    type::field($select13) AS `confidence`,
    type::field($select14) AS `advisories`,
    type::field($select15) AS `links`,
    type::field($select16) AS `potentialSideEffects`,
    type::field($select17) AS `replacementConfidence`,
    type::field($select18) AS `replacementCode`,
    type::field($select19) AS `guidance`,
    type::field($select20) AS `replacementCodeLanguage`,
    type::field($select21) AS `remediations`,
    type::field($select22) AS `remediationJobs`
FROM type::table($table)
WHERE type::field($field1) IN [$param1, type::record($param1)];
