DEFINE API "test";
DEFINE API OVERWRITE "test";
DEFINE API "test" FOR ANY PERMISSIONS FULL THEN {};
DEFINE API "test" FOR ANY MIDDLEWARE a::b() FOR get, put MIDDLEWARE c::d() THEN { 1+1; "bla"};
DEFINE API "test" FOR ANY MIDDLEWARE a::b() FOR get, put MIDDLEWARE c::d() PERMISSIONS FULL THEN { 1+1; "bla"};
DEFINE API "test" COMMENT "comment";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Create
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: None
                    .base_permission: None
                    .fallback: None
                    .methods: DefineMethodApiActions
                        .get: None
                        .delete: None
                        .patch: None
                        .post: None
                        .put: None
                        .trace: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Overwrite
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: None
                    .base_permission: None
                    .fallback: None
                    .methods: DefineMethodApiActions
                        .get: None
                        .delete: None
                        .patch: None
                        .post: None
                        .put: None
                        .trace: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Create
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: None
                    .base_permission: Some
                    Permission::Full
                    .fallback: Some
                    Expr::Object
                        Object
                            .entries: None
                    .methods: DefineMethodApiActions
                        .get: None
                        .delete: None
                        .patch: None
                        .post: None
                        .put: None
                        .trace: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Create
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: Some
                        -ApiMiddleware
                            .path: Path
                                .start: Ident
                                    .text: "a"
                                .parts: Some
                                    -PathSegment::Ident
                                        Ident
                                            .text: "b"
                            .args: None
                    .base_permission: None
                    .fallback: None
                    .methods: DefineMethodApiActions
                        .get: Some
                        ApiAction
                            .middleware: Some
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                                    .args: None
                            .permission: None
                            .action: Expr::Block
                                Block
                                    .exprs: Some
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Integer
                                                    +1
                                                .op: Add
                                                .right: Expr::Integer
                                                    +1
                                        -Expr::String
                                            StringLit
                                                .text: "bla"
                        .delete: None
                        .patch: None
                        .post: None
                        .put: Some
                        ApiAction
                            .middleware: Some
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                                    .args: None
                            .permission: None
                            .action: Expr::Block
                                Block
                                    .exprs: Some
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Integer
                                                    +1
                                                .op: Add
                                                .right: Expr::Integer
                                                    +1
                                        -Expr::String
                                            StringLit
                                                .text: "bla"
                        .trace: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Create
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: Some
                        -ApiMiddleware
                            .path: Path
                                .start: Ident
                                    .text: "a"
                                .parts: Some
                                    -PathSegment::Ident
                                        Ident
                                            .text: "b"
                            .args: None
                    .base_permission: None
                    .fallback: None
                    .methods: DefineMethodApiActions
                        .get: Some
                        ApiAction
                            .middleware: Some
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                                    .args: None
                            .permission: Some
                            Permission::Full
                            .action: Expr::Block
                                Block
                                    .exprs: Some
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Integer
                                                    +1
                                                .op: Add
                                                .right: Expr::Integer
                                                    +1
                                        -Expr::String
                                            StringLit
                                                .text: "bla"
                        .delete: None
                        .patch: None
                        .post: None
                        .put: Some
                        ApiAction
                            .middleware: Some
                                -ApiMiddleware
                                    .path: Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "d"
                                    .args: None
                            .permission: Some
                            Permission::Full
                            .action: Expr::Block
                                Block
                                    .exprs: Some
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Integer
                                                    +1
                                                .op: Add
                                                .right: Expr::Integer
                                                    +1
                                        -Expr::String
                                            StringLit
                                                .text: "bla"
                        .trace: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineApi
                DefineApi
                    .kind: Create
                    .path: Expr::String
                        StringLit
                            .text: "test"
                    .base_middleware: None
                    .base_permission: None
                    .fallback: None
                    .methods: DefineMethodApiActions
                        .get: None
                        .delete: None
                        .patch: None
                        .post: None
                        .put: None
                        .trace: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
*/