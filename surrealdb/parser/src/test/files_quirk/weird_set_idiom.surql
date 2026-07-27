// I dont think any of these make sense.
CREATE foo SET ->a = 1; // set field "->a" to 1
CREATE foo SET a... = [1,2]; // no field set
CREATE foo SET a[WHERE false] = 1; // no field set
CREATE foo SET a[? TRUE] = 1; // no field set
CREATE foo SET a.{b} = 1; // no field set
CREATE foo SET a.{b: 1} = 1; // no field set
CREATE foo SET a->b = 1; // sets field "a" to `{ "->b" : 1 }`
CREATE foo SET a->b->c = 1; // sets field "a" to `{ "->b" : { "->c": 1 } }`
CREATE foo SET a<~b = 1; // sets field "a" to `{ "<~b" : 1 }`
CREATE foo SET a<~? = 1; // sets field "a" to `{ "<~?" : 1 }`
CREATE foo SET a<~(select foo from bar as baz) = 1; // sets field "a" to `{ "<~(select foo from bar as baz)" : 1 }`
CREATE foo SET a.@ = 1; // no field set
CREATE foo SET a.* = 1; // no field set
CREATE foo SET a.? = 1; // no field set
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Field
                                    Ident
                                        .text: "->a"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Array
                                    Array
                                        .entries: Some
                                            -Expr::Integer
                                                +1
                                            -Expr::Integer
                                                +2
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .name: Ident
                                            .text: "->b"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Member
                                            MemberPlace
                                                .lhs: Place::Field
                                                    Ident
                                                        .text: "a"
                                                .name: Ident
                                                    .text: "->b"
                                        .name: Ident
                                            .text: "->c"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .name: Ident
                                            .text: "<~b"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .name: Ident
                                            .text: "<~?"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Member
                                    MemberPlace
                                        .lhs: Place::Field
                                            Ident
                                                .text: "a"
                                        .name: Ident
                                            .text: "<~(select foo from bar as baz)"
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
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
                                    .text: "foo"
                                .parts: None
                    .data: Some
                    RecordData::Set
                            -Assignment
                                .place: Place::Legacy
                                .op: AssignmentOp::Assign
                                .value: Expr::Integer
                                    +1
                    .output: None
                    .version: None
                    .timeout: None
*/