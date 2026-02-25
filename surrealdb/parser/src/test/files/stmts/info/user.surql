INFO FOR USER foo;
INFO FOR USER user ON NAMESPACE;
INFO FOR USER foo ON NS;
INFO FOR USER foo ON DATABASE;
INFO FOR USER foo ON DB;
INFO FOR USER foo ON ROOT;
INFO FOR USER foo ON KV;
INFO FOR USER foo ON KV STRUCTURE;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "user"
                                .parts: None
                        .base: Some
                            Namespace
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Namespace
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Database
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Database
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Root
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Root
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .base: Some
                            Root
                    .structure: true
*/