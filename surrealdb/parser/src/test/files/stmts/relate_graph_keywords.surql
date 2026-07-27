RELATE a:1->edge->sleep:b;
RELATE sleep:a->sleep->sleep:b;
RELATE sleep:b<-edge<-a:1;
RELATE a:1->edge->(SELECT * FROM b);
RELATE a:1->edge->SELECT * FROM b;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "a"
                            .key: RecordIdKey::Number
                                1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "edge"
                            .parts: None
                    .to: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "sleep"
                            .key: RecordIdKey::String
                                StringLit
                                    .text: "b"
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "sleep"
                            .key: RecordIdKey::String
                                StringLit
                                    .text: "a"
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "sleep"
                            .parts: None
                    .to: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "sleep"
                            .key: RecordIdKey::String
                                StringLit
                                    .text: "b"
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "a"
                            .key: RecordIdKey::Number
                                1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "edge"
                            .parts: None
                    .to: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "sleep"
                            .key: RecordIdKey::String
                                StringLit
                                    .text: "b"
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "a"
                            .key: RecordIdKey::Number
                                1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "edge"
                            .parts: None
                    .to: Expr::Covered
                        Expr::Select
                            Select
                                .fields: Fields::List
                                        -ListSelector::All
                                .omit: None
                                .only: false
                                .from: 
                                    -Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "b"
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
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "a"
                            .key: RecordIdKey::Number
                                1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "edge"
                            .parts: None
                    .to: Expr::Select
                        Select
                            .fields: Fields::List
                                    -ListSelector::All
                            .omit: None
                            .only: false
                            .from: 
                                -Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "b"
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
                    .data: None
                    .output: None
                    .timeout: None
*/