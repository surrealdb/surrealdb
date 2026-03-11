DEFINE MODULE f"abc" as mod::bla;
DEFINE MODULE f"abc" as mod::bla COMMENT "comment";
DEFINE MODULE f"abc" as mod::bla PERMISSIONS FULL;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineModule
                DefineModule
                    .kind: Create
                    .subject: ModuleName::File
                        FileLit
                            .path: "abc"
                    .alias: Some
                    Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineModule
                DefineModule
                    .kind: Create
                    .subject: ModuleName::File
                        FileLit
                            .path: "abc"
                    .alias: Some
                    Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineModule
                DefineModule
                    .kind: Create
                    .subject: ModuleName::File
                        FileLit
                            .path: "abc"
                    .alias: Some
                    Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .comment: None
                    .permission: Some
                    Permission::Full
*/