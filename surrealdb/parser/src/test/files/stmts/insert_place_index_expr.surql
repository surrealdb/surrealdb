INSERT INTO t (a[(1)]) VALUES (1);
INSERT INTO t (id, a[(-1)]) VALUES (t:1, 1);
INSERT INTO t (a[(LET $z = 0)]) VALUES (1);
INSERT INTO t (a[(1 + 1)], b) VALUES (1, 2);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: Some
                    InsertInto::Table
                        Ident
                            .text: "t"
                    .data: InsertData::Tuples
                        InsertTuples
                            .places: 
                                -Place::Index
                                    IndexPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .index: Expr::Covered
                                            Expr::Integer
                                                +1
                            .values: 
                                -
                                    -Expr::Integer
                                        +1
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: Some
                    InsertInto::Table
                        Ident
                            .text: "t"
                    .data: InsertData::Tuples
                        InsertTuples
                            .places: 
                                -Place::Field
                                    Ident
                                        .text: "id"
                                -Place::Index
                                    IndexPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .index: Expr::Covered
                                            Expr::Prefix
                                                PrefixExpr
                                                    .op: PrefixOperator::Negate
                                                    .right: Expr::Integer
                                                        +1
                            .values: 
                                -
                                    -Expr::RecordId
                                        RecordId
                                            .name: Ident
                                                .text: "t"
                                            .key: RecordIdKey::Number
                                                1
                                    -Expr::Integer
                                        +1
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: Some
                    InsertInto::Table
                        Ident
                            .text: "t"
                    .data: InsertData::Tuples
                        InsertTuples
                            .places: 
                                -Place::Index
                                    IndexPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .index: Expr::Covered
                                            Expr::Let
                                                Let
                                                    .param: Param
                                                        .text: "z"
                                                    .ty: None
                                                    .expr: Expr::Integer
                                                        +0
                            .values: 
                                -
                                    -Expr::Integer
                                        +1
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Insert
                Insert
                    .relation: false
                    .ignore: false
                    .into: Some
                    InsertInto::Table
                        Ident
                            .text: "t"
                    .data: InsertData::Tuples
                        InsertTuples
                            .places: 
                                -Place::Index
                                    IndexPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .index: Expr::Covered
                                            Expr::Binary
                                                BinaryExpr
                                                    .left: Expr::Integer
                                                        +1
                                                    .op: BinaryOperator::Add
                                                    .right: Expr::Integer
                                                        +1
                                -Place::Field
                                    Ident
                                        .text: "b"
                            .values: 
                                -
                                    -Expr::Integer
                                        +1
                                    -Expr::Integer
                                        +2
                    .on_duplicate: None
                    .output: None
                    .version: None
                    .timeout: None
*/