UPSERT foo;
UPSERT ONLY foo;
UPSERT ONLY foo WITH INDEX foo;
UPSERT ONLY foo WITH NOINDEX;
UPSERT ONLY foo WITH NOINDEX SET a = 1;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN NONE;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN NULL;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN DIFF;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN BEFORE;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN AFTER;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN a AS b.c;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN a AS b.c TIMEOUT 1s;
UPSERT ONLY foo WITH NOINDEX SET a += 1, b.c -= 1 WHERE foo = bar RETURN a AS b.c TIMEOUT 1s EXPLAIN FULL;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: false
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: None
                    .data: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: None
                    .data: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "foo"
                    .data: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Null
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Diff
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Before
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::After
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Fields
                        Fields::List
                                -ListSelector::Selector
                                    Selector
                                        .expr: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "a"
                                                .parts: None
                                        .alias: Some
                                        Place::Member
                                            MemberPlace
                                                .lhs: Place::Field
                                                    Ident
                                                        .text: "b"
                                                .name: Ident
                                                    .text: "c"
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Fields
                        Fields::List
                                -ListSelector::Selector
                                    Selector
                                        .expr: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "a"
                                                .parts: None
                                        .alias: Some
                                        Place::Member
                                            MemberPlace
                                                .lhs: Place::Field
                                                    Ident
                                                        .text: "b"
                                                .name: Ident
                                                    .text: "c"
                    .timeout: Some
                    Expr::Duration
                        1s
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Upsert
                Upsert
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "a"
                                .op: AssignmentOp::Add
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
                                .op: AssignmentOp::Subtract
                                .value: Expr::Integer
                                    +1
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: Equal
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                    .output: Some
                    Output::Fields
                        Fields::List
                                -ListSelector::Selector
                                    Selector
                                        .expr: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "a"
                                                .parts: None
                                        .alias: Some
                                        Place::Member
                                            MemberPlace
                                                .lhs: Place::Field
                                                    Ident
                                                        .text: "b"
                                                .name: Ident
                                                    .text: "c"
                    .timeout: Some
                    Expr::Duration
                        1s
                    .explain: Some
                    ExplainClause::Full
*/