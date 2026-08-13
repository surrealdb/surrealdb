USE NAMESPACE ns;
USE DATABASE db;
USE NAMESPACE ns DATABASE db;
USE DATABASE $db;
USE DATABASE type::string("x");
USE NAMESPACE (SELECT VALUE id FROM anything);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Namespace
                    Expr::Path
                        Path
                            .start: Ident
                                .text: "ns"
                            .parts: None
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Database
                    Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
        -TopLevelExpr::Use
            Use
                .kind: UseKind::NamespaceDatabase
                    .namespace: Expr::Path
                        Path
                            .start: Ident
                                .text: "ns"
                            .parts: None
                    .database: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Database
                    Expr::Param
                        Param
                            .text: "db"
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Database
                    Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "type"
                                    .parts: Some
                                        -PathSegment::Ident
                                            Ident
                                                .text: "string"
                            .op: IdiomOperator::Call
                                Some
                                    -Expr::String
                                        StringLit
                                            .text: "x"
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Namespace
                    Expr::Covered
                        Expr::Select
                            Select
                                .fields: Fields::Value
                                    Selector
                                        .expr: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "id"
                                                .parts: None
                                        .alias: None
                                .omit: None
                                .only: false
                                .from: 
                                    -Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "anything"
                                            .parts: None
                                .with_index: None
                                .condition: None
                                .split: None
                                .group: None
                                .order: None
                                .start: None
                                .limit: None
                                .version: None
                                .timeout: None
                                .fetch: None
                                .tempfiles: false
                                .explain: None
*/