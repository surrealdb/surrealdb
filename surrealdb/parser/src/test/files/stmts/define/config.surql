DEFINE CONFIG API PERMISSIONS FULL MIDDLEWARE a::b(), c::d();
DEFINE CONFIG GRAPHQL NONE;
DEFINE CONFIG GRAPHQL AUTO;
DEFINE CONFIG GRAPHQL TABLES INCLUDE a,b;
DEFINE CONFIG GRAPHQL TABLES EXCLUDE a,b;
DEFINE CONFIG GRAPHQL TABLES NONE;
DEFINE CONFIG GRAPHQL TABLES AUTO;
DEFINE CONFIG GRAPHQL FUNCTIONS INCLUDE a::b,c::d;
DEFINE CONFIG GRAPHQL FUNCTIONS EXCLUDE a,b;
DEFINE CONFIG GRAPHQL FUNCTIONS NONE;
DEFINE CONFIG GRAPHQL FUNCTIONS AUTO;
DEFINE CONFIG GRAPHQL DEPTH 1;
DEFINE CONFIG GRAPHQL COMPLEXITY 1;
DEFINE CONFIG GRAPHQL INTROSPECTION AUTO;
DEFINE CONFIG GRAPHQL INTROSPECTION NONE;
DEFINE CONFIG DEFAULT NAMESPACE "a";
DEFINE CONFIG DEFAULT DATABASE "b" NAMESPACE "a";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Api
                        DefineConfigApi
                            .permission: Some
                            Permission::Full
                            .middleware: Some
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "b"
                                    .args: None
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                                    .args: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::None
                            .function_config: Some
                            FunctionConfig::None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::Auto
                            .function_config: Some
                            FunctionConfig::Auto
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::Include
                                    -Ident
                                        .text: "a"
                                    -Ident
                                        .text: "b"
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::Exclude
                                    -Ident
                                        .text: "a"
                                    -Ident
                                        .text: "b"
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::None
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: Some
                            TablesConfig::Auto
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: Some
                            FunctionConfig::Include
                                    -Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "b"
                                    -Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: Some
                            FunctionConfig::Exclude
                                    -Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                    -Path
                                        .start: Ident
                                            .text: "b"
                                        .parts: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: Some
                            FunctionConfig::None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: Some
                            FunctionConfig::Auto
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: None
                            .depth_limit: Some
                            +1
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: None
                            .table_config: None
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: Some
                            +1
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: Some
                            Auto
                            .table_config: None
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Graphql
                        DefineConfigGraphql
                            .introspection: Some
                            None
                            .table_config: None
                            .function_config: None
                            .depth_limit: None
                            .complexity_limit: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Default
                        DefineConfigDefault
                            .namespace: Some
                            Expr::String
                                StringLit
                                    .text: "a"
                            .database: None
        -TopLevelExpr::Expr
            Expr::DefineConfig
                DefineConfig
                    .kind: Create
                    .inner: DefineConfigKind::Default
                        DefineConfigDefault
                            .namespace: Some
                            Expr::String
                                StringLit
                                    .text: "a"
                            .database: Some
                            Expr::String
                                StringLit
                                    .text: "b"
*/