RELATE a->b->c;
RELATE c<-b<-a;
RELATE c<-b:d<-a;
RELATE c<-$param<-a;
RELATE ONLY c<-$param<-a MERGE { a: b };
RELATE ONLY c<-$param<-a MERGE { a: b } RETURN BEFORE;
RELATE ONLY c<-$param<-a MERGE { a: b } RETURN BEFORE TIMEOUT 1s;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "b"
                            .key: RecordIdKey::String
                                StringLit
                                    .text: "d"
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Param
                        Param
                            .text: "param"
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: true
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Param
                        Param
                            .text: "param"
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: Some
                        RecordData::Merge
                            Expr::Object
                                Object
                                    .entries: Some
                                        -ObjectEntry
                                            .key: "a"
                                            .value: Expr::Path
                                                Path
                                                    .start: Ident
                                                        .text: "b"
                                                    .parts: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: true
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Param
                        Param
                            .text: "param"
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: Some
                        RecordData::Merge
                            Expr::Object
                                Object
                                    .entries: Some
                                        -ObjectEntry
                                            .key: "a"
                                            .value: Expr::Path
                                                Path
                                                    .start: Ident
                                                        .text: "b"
                                                    .parts: None
                    .output: Some
                        Output::Before
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: true
                    .from: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .through: Expr::Param
                        Param
                            .text: "param"
                    .to: Expr::Path
                        Path
                            .start: Ident
                                .text: "c"
                            .parts: None
                    .data: Some
                        RecordData::Merge
                            Expr::Object
                                Object
                                    .entries: Some
                                        -ObjectEntry
                                            .key: "a"
                                            .value: Expr::Path
                                                Path
                                                    .start: Ident
                                                        .text: "b"
                                                    .parts: None
                    .output: Some
                        Output::Before
                    .timeout: Some
                        Expr::Duration
                            1s
*/