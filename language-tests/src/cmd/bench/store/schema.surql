
DEFINE TABLE IF NOT EXISTS benchmark;
DEFINE FIELD IF NOT EXISTS id ON benchmark TYPE string;
DEFINE FIELD IF NOT EXISTS path ON benchmark COMPUTED id;

-- The baseline pointer table is keyed by [path, backend, transport] so an http
-- or ws run is never compared against an embedded baseline. OVERWRITE (rather
-- than IF NOT EXISTS) so existing stores migrate from the old 2-part key; legacy
-- 2-part `run` records are simply orphaned and re-established on the next --save.
DEFINE TABLE IF NOT EXISTS run;
DEFINE FIELD OVERWRITE id ON run TYPE [string, string, string];
DEFINE FIELD OVERWRITE path ON run COMPUTED id[0];
DEFINE FIELD OVERWRITE backend ON run COMPUTED id[1];
DEFINE FIELD OVERWRITE transport ON run COMPUTED id[2];
DEFINE FIELD OVERWRITE latest ON run TYPE record<measurement>;

DEFINE TABLE IF NOT EXISTS measurement;
DEFINE FIELD IF NOT EXISTS id ON measurement TYPE [string,string,datetime];
DEFINE FIELD IF NOT EXISTS path ON measurement COMPUTED id[0];
DEFINE FIELD IF NOT EXISTS backend ON measurement COMPUTED id[1];
DEFINE FIELD IF NOT EXISTS datetime ON measurement COMPUTED id[2];
DEFINE FIELD IF NOT EXISTS iterations ON measurement TYPE array<float>;
DEFINE FIELD IF NOT EXISTS times ON measurement TYPE array<float>;
DEFINE FIELD IF NOT EXISTS average_times ON measurement TYPE array<float>;
DEFINE FIELD IF NOT EXISTS labels ON measurement TYPE array<"HighMild" | "HighSevere" | "LowMild" | "LowSevere" | "NotAnOutlier">;
DEFINE FIELD IF NOT EXISTS fences ON measurement TYPE array<float,4>;
-- Run metadata (see cmd/bench/meta.rs). All nullable so existing rows read back
-- with these as NONE; `transport` defaults to "embedded" for legacy rows via the
-- update_benchmark event below.
DEFINE FIELD IF NOT EXISTS commit ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS commit_short ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS branch ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS dirty ON measurement TYPE bool | none;
DEFINE FIELD IF NOT EXISTS transport ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS endpoint ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS hostname ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS os ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS arch ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS cpu ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS cpu_cores ON measurement TYPE int | none;
-- Rows the benched statement returned. A bench whose row count moved is not
-- measuring the same work as its baseline however comparable the two timings
-- look, so the comparison reports it as a changed workload rather than as a
-- speed difference. `none` on rows written before this field existed.
DEFINE FIELD IF NOT EXISTS rows ON measurement TYPE int | none;
DEFINE FIELD IF NOT EXISTS mean ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS median ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS std_dev ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS abs_dev ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };

DEFINE EVENT OVERWRITE update_benchmark ON measurement
	WHEN $event == "CREATE"
	THEN {
		UPSERT type::record("benchmark",$after.id[0]);
		-- Legacy rows (written before transport tracking) bucket under "embedded".
		LET $transport = $after.transport ?? "embedded";
        IF run:[$after.id[0], $after.id[1], $transport].latest.datetime < $after.id[2] {
    		UPSERT run:[$after.id[0], $after.id[1], $transport]
    			SET latest = $after.id;
        };
	};


DEFINE FUNCTION OVERWRITE fn::last_measurement($path: string, $backend: string, $transport: string) {
	let $res = run:[$path,$backend,$transport].latest;
	if $res == NONE {
		return NONE
	};
	$res.*
}
