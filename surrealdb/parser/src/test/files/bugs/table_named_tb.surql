DEFINE INDEX ix ON tb FIELDS a, b;
INFO FOR TB tb;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ix"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "tb"
                            .parts: None
                    .fields: Some
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .comment: None
                    .index: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "tb"
                                .parts: None
                        .version: None
                    .structure: false
*/