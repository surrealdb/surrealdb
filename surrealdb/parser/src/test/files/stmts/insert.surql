INSERT a;
INSERT RELATION a;
INSERT IGNORE a;
INSERT IGNORE INTO $param a;
INSERT IGNORE INTO table a;
INSERT a ON DUPLICATE KEY UPDATE a += 1;
INSERT a RETURN NONE;
INSERT a RETURN (NONE);
INSERT a VERSION 1;
INSERT a TIMEOUT 1;
INSERT (a,b,c.d) VALUES (1,2,3),(4,5,6);
INSERT (a);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: true
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: true
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: true
                    .into: Some
                    InsertInto::Param
                        Param
                            .text: "param"
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: true
                    .into: Some
                    InsertInto::Table
                        Ident
                            .text: "table"
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: Some
                        -Assignment
                            .place: Place::Field
                                Ident
                                    .text: "a"
                            .op: AssignmentOp::Add
                            .value: Expr::Integer
                                +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: Some
                    Output::None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: Some
                    Output::Fields
                        Fields::List
                                -ListSelector::Selector
                                    Selector
                                        .expr: Expr::Covered
                                            Expr::Builtin
                                                Builtin::None
                                        .alias: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: Some
                    Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Tuples
                        InsertTuples
                            .places: 
                                -Place::Field
                                    Ident
                                        .text: "a"
                                -Place::Field
                                    Ident
                                        .text: "b"
                                -Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "c"
                                        .name: Ident
                                            .text: "d"
                            .values: 
                                -
                                    -Expr::Integer
                                        +1
                                    -Expr::Integer
                                        +2
                                    -Expr::Integer
                                        +3
                                -
                                    -Expr::Integer
                                        +4
                                    -Expr::Integer
                                        +5
                                    -Expr::Integer
                                        +6
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: None
                    .data: InsertData::Expr
                        Expr::Covered
                            Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
*/