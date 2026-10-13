CREATE context SET context = 1;
SELECT context FROM context;
LET $context = 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: false
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "context"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "context"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "context"
                                            .parts: None
                                    .alias: None
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "context"
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
                    .for_update: false
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Let
                Let
                    .param: Param
                        .text: "context"
                    .ty: None
                    .expr: Expr::Integer
                        +1
*/