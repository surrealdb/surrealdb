DEFINE MODULE mod::bla FROM f"abc" UNSIGNED;
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED COMMENT "comment";
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED PERMISSIONS FULL;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineModule
                DefineModule
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .source: Some
                    ModuleSource::File
                        FileLit
                            .path: "abc"
                    .unsigned: true
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineModule
                DefineModule
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .source: Some
                    ModuleSource::File
                        FileLit
                            .path: "abc"
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
                    .name: Path
                        .start: Ident
                            .text: "mod"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "bla"
                    .source: Some
                    ModuleSource::File
                        FileLit
                            .path: "abc"
                    .unsigned: true
                    .comment: None
                    .permission: Some
                    Permission::Full
*/