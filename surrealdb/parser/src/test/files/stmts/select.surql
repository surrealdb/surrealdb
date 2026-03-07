SELECT * FROM foo;
SELECT a as b, c from foo,bar;
SELECT * FROM ONLY foo;
SELECT * FROM ONLY foo WITH INDEX a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 SPLIT ON a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s TEMPFILES;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s EXPLAIN;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s EXPLAIN;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH a.b, c.d;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH type::fields(1,2);
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH type::field(1);
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH $p;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: false
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
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
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::Selector
                            Selector
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .alias: Some
                                    Place::Field
                                        Ident
                                            .text: "b"
                        -ListSelector::Selector
                            Selector
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: None
                                .alias: None
                    .only: false
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    -Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
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
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
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
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
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
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
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
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: None
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: Some
                        Expr::Integer
                            +1
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: Some
                        Expr::Integer
                            +1
                    .timeout: Some
                        Expr::Duration
                            1s
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: Some
                        Expr::Integer
                            +1
                    .timeout: Some
                        Expr::Duration
                            1s
                    .fetch: None
                    .tempfiles: true
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: Some
                        Expr::Integer
                            +1
                    .timeout: Some
                        Expr::Duration
                            1s
                    .fetch: None
                    .tempfiles: false
                    .explain: Some
                        Explain::Base
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: Some
                        Expr::Integer
                            +1
                    .timeout: Some
                        Expr::Duration
                            1s
                    .fetch: None
                    .tempfiles: false
                    .explain: Some
                        Explain::Base
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Fetch::Place
                            PresentPlace::Member
                                MemberPresentPlace
                                    .lhs: PresentPlace::Field
                                        Ident
                                            .text: "a"
                                    .name: Ident
                                        .text: "b"
                        -Fetch::Place
                            PresentPlace::Member
                                MemberPresentPlace
                                    .lhs: PresentPlace::Field
                                        Ident
                                            .text: "c"
                                    .name: Ident
                                        .text: "d"
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Fetch::TypeFields
                            FetchTypeFields
                                .args: Some
                                    -Expr::Integer
                                        +1
                                    -Expr::Integer
                                        +2
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Fetch::TypeField
                            FetchTypeField
                                .arg: Expr::Integer
                                    +1
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                        -ListSelector::All
                    .only: true
                    .from: -Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .with_index: Some
                        WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: ExactEqual
                                .right: Expr::Integer
                                    +2
                    .split: None
                    .group: Some
                        -PresentPlace::Field
                            Ident
                                .text: "a"
                        -PresentPlace::Field
                            Ident
                                .text: "b"
                    .order: Some
                        OrderBy::Places
                            -PresentPlace::Field
                                Ident
                                    .text: "a"
                    .start: Some
                        Expr::Integer
                            +1
                    .limit: Some
                        Expr::Integer
                            +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Fetch::Param
                            Param
                                .text: "p"
                    .tempfiles: false
                    .explain: None
*/