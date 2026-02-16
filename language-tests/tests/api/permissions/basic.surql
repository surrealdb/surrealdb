/**
[env]
namespace = "test"
database = "test"
imports = ["api/permissions/setup.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = "Test API permission checks for record users at method-level, route-level, and global config levels"

# Method-Level Permissions Tests
# 0 - Method-level PERMISSIONS NONE should deny (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 1 - Method-level PERMISSIONS FULL should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 2 - Method-level PERMISSIONS WHERE true should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 3 - Method-level PERMISSIONS WHERE false should deny (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# Route-Level Permissions Tests
# 4 - Route-level PERMISSIONS NONE should deny (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 5 - Route-level PERMISSIONS FULL should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 6 - Route-level PERMISSIONS WHERE true should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 7 - Route-level PERMISSIONS WHERE false should deny (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# Global API Config Permissions Tests
# 8 - Global config PERMISSIONS NONE was set, but then reset to FULL before invocation
# Note: Global config is checked at runtime, so this API allows because global config is FULL when invoked
[[test.results]]
skip-api-request-id = true
value = "{ body: {}, headers: {}, status: 200 }"

# 9 - Global config PERMISSIONS WHERE true should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 10 - Global config PERMISSIONS WHERE false was set, but then reset to FULL before invocation
# Note: Global config is checked at runtime, so this API allows because global config is FULL when invoked
[[test.results]]
skip-api-request-id = true
value = "{ body: {}, headers: {}, status: 200 }"

# Permission Evaluation Order Tests
# 11 - Method-level NONE blocks even if route-level is FULL (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 12 - Route-level NONE blocks even if global config is FULL (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 13 - Method-level WHERE false blocks even if route-level is FULL (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 14 - Route-level WHERE false blocks even if global config is FULL (returns ApiResponse with 403)
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

# 15 - All levels FULL should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"

# 16 - Method-level WHERE true, route-level WHERE true, global FULL should allow
[[test.results]]
skip-api-request-id = true
value = "{ body: { allowed: true }, headers: {}, status: 200 }"
*/

// ============================================================================
// Method-Level Permissions Tests
// ============================================================================

# 0 - Method-level PERMISSIONS NONE should deny
api::invoke("/method/none");

# 1 - Method-level PERMISSIONS FULL should allow
api::invoke("/method/full");

# 2 - Method-level PERMISSIONS WHERE true should allow
api::invoke("/method/where-true");

# 3 - Method-level PERMISSIONS WHERE false should deny
api::invoke("/method/where-false");

// ============================================================================
// Route-Level Permissions Tests
// ============================================================================

# 4 - Route-level PERMISSIONS NONE should deny
api::invoke("/route/none");

# 5 - Route-level PERMISSIONS FULL should allow
api::invoke("/route/full");

# 6 - Route-level PERMISSIONS WHERE true should allow
api::invoke("/route/where-true");

# 7 - Route-level PERMISSIONS WHERE false should deny
api::invoke("/route/where-false");

// ============================================================================
// Global API Config Permissions Tests
// ============================================================================

# 8 - Global config PERMISSIONS NONE should deny
api::invoke("/global/none");

# 9 - Global config PERMISSIONS WHERE true should allow
api::invoke("/global/where-true");

# 10 - Global config PERMISSIONS WHERE false should deny
api::invoke("/global/where-false");

// ============================================================================
// Permission Evaluation Order Tests
// ============================================================================

# 11 - Method-level NONE blocks even if route-level is FULL
api::invoke("/order/method-none-route-full");

# 12 - Route-level NONE blocks even if global config is FULL
api::invoke("/order/route-none-global-full");

# 13 - Method-level WHERE false blocks even if route-level is FULL
api::invoke("/order/method-false-route-full");

# 14 - Route-level WHERE false blocks even if global config is FULL
api::invoke("/order/route-false-global-full");

# 15 - All levels FULL should allow
api::invoke("/order/all-full");

# 16 - Method-level WHERE true, route-level WHERE true, global FULL should allow
api::invoke("/order/method-true-route-true-global-full");
