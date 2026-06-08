
DEFINE TABLE IF NOT EXISTS benchmark;
DEFINE FIELD IF NOT EXISTS id ON benchmark TYPE string;
DEFINE FIELD IF NOT EXISTS path ON benchmark COMPUTED id;

DEFINE TABLE IF NOT EXISTS run;
DEFINE FIELD IF NOT EXISTS id ON run TYPE [string, string];
DEFINE FIELD IF NOT EXISTS path ON run COMPUTED id[0];
DEFINE FIELD IF NOT EXISTS backend ON run COMPUTED id[1];
DEFINE FIELD IF NOT EXISTS latest ON run TYPE record<measurement>;

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
DEFINE FIELD IF NOT EXISTS commit ON measurement TYPE string | none;
DEFINE FIELD IF NOT EXISTS mean ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS median ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS std_dev ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };
DEFINE FIELD IF NOT EXISTS abs_dev ON measurement TYPE { point: float, standard_error: float, confidence_level: float, lower_bound: float, upper_bound: float };

DEFINE EVENT IF NOT EXISTS update_benchmark ON measurement
	WHEN $event == "CREATE"
	THEN {
		UPSERT type::record("benchmark",$after.id[0]);
        IF run:[$after.id[0], $after.id[1]].latest.datetime < $after.id[2] {
    		UPSERT run:[$after.id[0], $after.id[1]]
    			SET latest = $after.id;
        };
	};


DEFINE FUNCTION IF NOT EXISTS fn::last_measurement($path: string, $backend: string) {
	let $res = run:[$path,$backend].latest;
	if $res == NONE {
		return NONE
	};
	$res.*
}
