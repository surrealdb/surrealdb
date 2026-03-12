CREATE a;
CREATE a, b;
CREATE ONLY a, b;
CREATE ONLY a, b SET a = 1, b.c += 2, d[1] -= 3, e +?= 4;
// TODO: Remove in CREATE?
CREATE ONLY a, b UNSET a, b.c, d[1];
CREATE ONLY a, b CONTENT { a: 1 };
CREATE ONLY a, b PATCH { a: 1 };
CREATE ONLY a, b MERGE { a: 1 };
CREATE ONLY a, b REPLACE { a: 1 };
CREATE ONLY a, b REPLACE { a: 1 } VERSION 1;
CREATE ONLY a, b REPLACE { a: 1 } VERSION 1 TIMEOUT 1;
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
                                    .text: "a"
                                .parts: None
                    .data: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: false
                    .targets: 
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
                    .data: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: None
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "b"
                                        .name: Ident
                                            .text: "c"
                                .op: AssignmentOp::Add
                                .value: Expr::Integer
                                    +2
                            -Assignment
                                .place: Place::Index
                                    IndexPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "d"
                                        .index: Expr::Integer
                                            +1
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +3
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "e"
                                .op: AssignmentOp::Extend
                                .value: Expr::Integer
                                    +4
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Unset
                            -Place::Field
                                Ident
                                    .text: "a"
                            -Place::Member
                                MemberPlace
                                    .lhs: Place::Field
                                        Ident
                                            .text: "b"
                                    .name: Ident
                                        .text: "c"
                            -Place::Index
                                IndexPlace
                                    .lhs: Place::Field
                                        Ident
                                            .text: "d"
                                    .index: Expr::Integer
                                        +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Content
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Patch
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Merge
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Replace
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Replace
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: true
                    .targets: 
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
                    .data: Some
                    RecordData::Replace
                        Expr::Object
                            Object
                                .entries: Some
                                    -ObjectEntry
                                        .key: "a"
                                        .value: Expr::Integer
                                            +1
                    .output: None
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Integer
                        +1
*/