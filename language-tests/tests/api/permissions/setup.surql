/**
[test]
run = false  # This file is only used as an import, not run as a test
*/

// Define record access method for authentication
DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM user WHERE id = $id )
    DURATION FOR SESSION 24h;

// Create user record for testing
CREATE user:test;

// Ensure global API config starts with FULL permissions
DEFINE CONFIG API PERMISSIONS FULL;

// ============================================================================
// Method-Level Permissions Tests
// ============================================================================

// Method-level PERMISSIONS NONE
DEFINE API "/method/none"
    FOR get
        PERMISSIONS NONE
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Method-level PERMISSIONS FULL
DEFINE API "/method/full"
    FOR get
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Method-level PERMISSIONS WHERE true
DEFINE API "/method/where-true"
    FOR get
        PERMISSIONS WHERE true
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Method-level PERMISSIONS WHERE false
DEFINE API "/method/where-false"
    FOR get
        PERMISSIONS WHERE false
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// ============================================================================
// Route-Level Permissions Tests
// Route-level permissions are set using FOR ANY with PERMISSIONS
// ============================================================================

// Route-level PERMISSIONS NONE
DEFINE API "/route/none"
    FOR any
        PERMISSIONS NONE
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Route-level PERMISSIONS FULL
DEFINE API "/route/full"
    FOR any
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Route-level PERMISSIONS WHERE true
DEFINE API "/route/where-true"
    FOR any
        PERMISSIONS WHERE true
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Route-level PERMISSIONS WHERE false
DEFINE API "/route/where-false"
    FOR any
        PERMISSIONS WHERE false
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// ============================================================================
// Global API Config Permissions Tests
// ============================================================================

// Global config PERMISSIONS NONE
DEFINE CONFIG API PERMISSIONS NONE;
DEFINE API "/global/none"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Reset global config to FULL for other tests
DEFINE CONFIG API PERMISSIONS FULL;

// Global config PERMISSIONS WHERE true
DEFINE CONFIG API PERMISSIONS WHERE true;
DEFINE API "/global/where-true"
    FOR get
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Global config PERMISSIONS WHERE false
DEFINE CONFIG API PERMISSIONS WHERE false;
DEFINE API "/global/where-false"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Reset global config to FULL for remaining tests
DEFINE CONFIG API PERMISSIONS FULL;

// ============================================================================
// Permission Evaluation Order Tests
// ============================================================================

// Method-level NONE blocks even if route-level is FULL
DEFINE API "/order/method-none-route-full"
    FOR any
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: {}
            };
        }
    FOR get
        PERMISSIONS NONE
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Route-level NONE blocks even if global config is FULL
DEFINE CONFIG API PERMISSIONS FULL;
DEFINE API "/order/route-none-global-full"
    FOR any
        PERMISSIONS NONE
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Method-level WHERE false blocks even if route-level is FULL
DEFINE API "/order/method-false-route-full"
    FOR any
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: {}
            };
        }
    FOR get
        PERMISSIONS WHERE false
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// Route-level WHERE false blocks even if global config is FULL
DEFINE CONFIG API PERMISSIONS FULL;
DEFINE API "/order/route-false-global-full"
    FOR any
        PERMISSIONS WHERE false
        THEN {
            {
                status: 200,
                body: {}
            };
        };

// All levels FULL should allow
DEFINE API "/order/all-full"
    FOR any
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        }
    FOR get
        PERMISSIONS FULL
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };

// Method-level WHERE true, route-level WHERE true, global FULL should allow
DEFINE CONFIG API PERMISSIONS FULL;
DEFINE API "/order/method-true-route-true-global-full"
    FOR any
        PERMISSIONS WHERE true
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        }
    FOR get
        PERMISSIONS WHERE true
        THEN {
            {
                status: 200,
                body: { allowed: true }
            };
        };
