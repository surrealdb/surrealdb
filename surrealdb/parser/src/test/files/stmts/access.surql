ACCESS my_access ON DATABASE GRANT FOR USER my_user;
ACCESS my_access ON DATABASE GRANT FOR RECORD user:tobie;
ACCESS my_access ON DATABASE SHOW ALL;
ACCESS my_access ON DATABASE SHOW GRANT my_grant;
ACCESS my_access ON DATABASE SHOW WHERE revocation = NONE;
ACCESS my_access ON DATABASE REVOKE ALL;
ACCESS my_access ON DATABASE REVOKE GRANT my_grant;
ACCESS my_access ON DATABASE REVOKE WHERE expiration < time::now();
ACCESS my_access ON DATABASE PURGE EXPIRED;
ACCESS my_access ON DATABASE PURGE REVOKED;
ACCESS my_access ON DATABASE PURGE EXPIRED, REVOKED;
ACCESS my_access ON DATABASE PURGE REVOKED EXPIRED;
ACCESS my_access ON DATABASE PURGE EXPIRED, REVOKED FOR 1h;
ACCESS my_access ON NAMESPACE PURGE EXPIRED FOR 1d;
ACCESS my_access ON ROOT REVOKE ALL;
ACCESS my_access PURGE REVOKED FOR 0s;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Grant
                        AccessGrant
                            .subject: AccessSubject::User
                                Ident
                                    .text: "my_user"
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Grant
                        AccessGrant
                            .subject: AccessSubject::Subject
                                RecordId
                                    .name: Ident
                                        .text: "user"
                                    .key: RecordIdKey::String
                                        StringLit
                                            .text: "tobie"
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Show
                        AccessShow
                            .kind: AccessShowKind::All
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Show
                        AccessShow
                            .kind: AccessShowKind::Grant
                                Ident
                                    .text: "my_grant"
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Show
                        AccessShow
                            .kind: AccessShowKind::Condition
                                Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "revocation"
                                                .parts: None
                                        .op: BinaryOperator::Equal
                                        .right: Expr::Builtin
                                            Builtin::None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Revoke
                        AccessRevoke
                            .kind: AccessRevokeKind::All
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Revoke
                        AccessRevoke
                            .kind: AccessRevokeKind::Grant
                                Ident
                                    .text: "my_grant"
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Revoke
                        AccessRevoke
                            .kind: AccessRevokeKind::Condition
                                Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Path
                                            Path
                                                .start: Ident
                                                    .text: "expiration"
                                                .parts: None
                                        .op: BinaryOperator::LessThan
                                        .right: Expr::Idiom
                                            IdiomExpr
                                                .left: Expr::Path
                                                    Path
                                                        .start: Ident
                                                            .text: "time"
                                                        .parts: Some
                                                            -PathSegment::Ident
                                                                Ident
                                                                    .text: "now"
                                                .op: IdiomOperator::Call
                                                    None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Expired
                            .grace: None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Revoked
                            .grace: None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Both
                            .grace: None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Both
                            .grace: None
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Database
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Both
                            .grace: Some
                            3600s
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Namespace
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Expired
                            .grace: Some
                            86400s
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: Some
                    Root
                    .kind: AccessKind::Revoke
                        AccessRevoke
                            .kind: AccessRevokeKind::All
        -TopLevelExpr::Expr
            Expr::Access
                Access
                    .access: Ident
                        .text: "my_access"
                    .base: None
                    .kind: AccessKind::Purge
                        AccessPurge
                            .kind: PurgeKind::Revoked
                            .grace: Some
                            0ns
*/