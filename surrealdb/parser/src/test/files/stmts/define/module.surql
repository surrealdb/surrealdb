DEFINE MODULE f"abc" as mod::bla UNSIGNED;
DEFINE MODULE f"abc" as mod::bla UNSIGNED COMMENT "comment";
DEFINE MODULE f"abc" as mod::bla UNSIGNED PERMISSIONS FULL;
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
                    .unsigned: true
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
                    .unsigned: true
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
                    .unsigned: true
                    .comment: None
                    .permission: Some
                    Permission::Full
*/