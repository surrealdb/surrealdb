


# import "expr.capnp";
# import "expr.capnp";


# service SurrealDB {
#     rpc Query(Request) returns (stream surrealdb.value.Value);
#     rpc QueryBatched(Request) returns (Response);
#     // rpc Health(HealthParams) returns (Response);
#     // rpc Version(VersionParams) returns (Response);
#     // rpc Ping(PingParams) returns (Response);
#     // rpc Info(InfoParams) returns (Response);
#     // rpc Use(UseParams) returns (Response);
#     // rpc Signup(SignupParams) returns (Response);
#     // rpc Signin(SigninParams) returns (Response);
#     // rpc Authenticate(AuthenticateParams) returns (Response);
#     // rpc Invalidate(InvalidateParams) returns (Response);
#     // rpc Reset(ResetParams) returns (Response);
#     // rpc Kill(KillParams) returns (Response);
#     // rpc Live(LiveParams) returns (stream Response);
#     // rpc Set(SetParams) returns (Response);
#     // rpc Unset(UnsetParams) returns (Response);
#     // rpc Select(SelectParams) returns (Response);
#     // rpc Insert(InsertParams) returns (Response);
#     // rpc Upsert(UpsertParams) returns (Response);
#     // rpc Update(UpdateParams) returns (Response);
#     // rpc Merge(MergeParams) returns (Response);
#     // rpc Patch(PatchParams) returns (Response);
#     // rpc Delete(DeleteParams) returns (Response);
#     // rpc Query(QueryParams) returns (Response);
#     // rpc RawQuery(RawQueryParams) returns (ResponseValue); // TODO: This should probably be a stream.
#     // rpc Relate(RelateParams) returns (ResponseValue); // TODO: This should probably be a stream.
#     // rpc Run(RunParams) returns (ResponseValue); // TODO: This should probably be a stream.
#     // rpc GraphQL(GraphQLParams) returns (ResponseValue); // TODO: This should probably be a stream.
#     // rpc InsertRelation(InsertRelationParams) returns (ResponseValue); // TODO: This should probably be a stream.
# }

# // TODO: This should probably just be a map<string, surrealdb.value.Value> instead of a message.
# message StatementOptions {
#     oneof data {
#         surrealdb.value.Value patch = 1;
#         surrealdb.value.Value merge = 2;
#         surrealdb.value.Value replace = 3;
#         surrealdb.value.Value content = 4;
#         surrealdb.value.Value single = 5;
#     }
#     surrealdb.ast.Fields fields = 6;
#     surrealdb.ast.Output return = 7;
#     surrealdb.value.Value limit = 8;
#     surrealdb.value.Value start = 9;
#     surrealdb.value.Value cond = 10;
#     surrealdb.value.Value version = 11;
#     google.capnpbuf.Duration timeout = 12;
#     optional bool only = 13;
#     optional bool relation = 14;
#     optional bool unique = 15;
#     map<string, surrealdb.value.Value> vars = 16;
#     optional bool diff = 17;
#     surrealdb.ast.Fetchs fetchs = 18;
# }


# message Request {
#     string id = 1;
#     optional uint32 rpc_version = 2;

#     oneof command {
#         HealthParams health = 3;
#         VersionParams version = 4;
#         InfoParams info = 5;
#         UseParams use = 6;
#         SignupParams signup = 7;
#         SigninParams signin = 8;
#         AuthenticateParams authenticate = 9;
#         InvalidateParams invalidate = 10;
#         ResetParams reset = 11;
#         KillParams kill = 12;
#         LiveParams live = 13;
#         SetParams set = 14;
#         UnsetParams unset = 15;
#         SelectParams select = 16;
#         InsertParams insert = 17;
#         CreateParams create = 18;
#         UpsertParams upsert = 19;
#         UpdateParams update = 20;
#         MergeParams merge = 21;
#         PatchParams patch = 22;
#         DeleteParams delete = 23;
#         QueryParams query = 24;
#         RawQueryParams raw_query = 25;
#         RelateParams relate = 26;
#         RunParams run = 27;
#         GraphQLParams graphql = 28;
#         InsertRelationParams insert_relation = 29;
#     }
# }

# message HealthParams {}

# message VersionParams {}

# message PingParams {}

# message InfoParams {}

# message UseParams {
#     optional string namespace = 1;
#     optional string database = 2;
# }

# message RootUserCredentials {
#     string username = 1;
#     string password = 2;
# }
# message NamespaceAccessCredentials {
#     string namespace = 1;
#     string access = 2;
#     string key = 3;
# }
# message DatabaseAccessCredentials {
#     string namespace = 1;
#     string database = 2;
#     string access = 3;
#     string key = 4;
#     optional string refresh = 5;
# }
# message NamespaceUserCredentials {
#     string namespace = 1;
#     string username = 2;
#     string password = 3;
# }
# message DatabaseUserCredentials {
#     string namespace = 1;
#     string database = 2;
#     string username = 3;
#     string password = 4;
# }

# message Access {
#     oneof inner {
#         RootUserCredentials root_user = 1;
#         NamespaceAccessCredentials namespace = 2;
#         DatabaseAccessCredentials database = 3;
#         NamespaceUserCredentials namespace_user = 4;
#         DatabaseUserCredentials database_user = 5;
#     }
# }

# message SignupParams {
#     string namespace = 1;
#     string database = 2;
#     string access = 3;
#     map<string, string> access_params = 4;
# }

# message SigninParams {
#     Access access = 1;
# }

# message AuthenticateParams {
#     string token = 1;
# }

# message InvalidateParams {}

# message CreateParams {
# }

# message ResetParams {}
# message KillParams {
#     // The UUID of the live query to kill.
#     string live_uuid = 1;
    
# }
# message LiveParams {
#     string table = 1;
#     surrealdb.ast.Fields fields = 2;
#     bool diff = 3;
#     surrealdb.value.Value cond = 4;
#     surrealdb.ast.Fetchs fetchs = 5;
#     map<string, surrealdb.value.Value> vars = 6;
# }
# message SetParams {
#     string key = 1;
#     surrealdb.value.Value value = 2;
# }
# message UnsetParams {
#     string key = 1;
# }
# message SelectParams {
#     surrealdb.value.Value what = 1;
#     StatementOptions options = 2;
# }
# message InsertParams {}
# message UpsertParams {}
# message UpdateParams {}
# message MergeParams {}
# message PatchParams {}
# message DeleteParams {}
# message QueryParams {
#     map<string, surrealdb.value.Value> variables = 1;
# }
# message RawQueryParams {
#     string query = 1;
#     map<string, surrealdb.value.Value> variables = 2;
# }
# message RelateParams {}
# message RunParams {}
# message GraphQLParams {}
# message InsertRelationParams {}



# message Response {
#     string id = 1;

#     repeated QueryResult results = 2;
# }

# // This is the response for a single statement result.
# message QueryResult {
#     uint64 index = 1;
#     QueryStats stats = 2;

#     oneof result {
#         surrealdb.value.Value value = 3;
#         Error error = 4;
#     }
# }

# message QueryStats {
#     google.capnpbuf.Timestamp start_time = 1;
#     google.capnpbuf.Duration execution_duration = 2;
# }

# message Error {
#     int64 code = 1;
#     string message = 2;
# }

# Convert the above protobuf definitions to Cap'n Proto format:
# The Value type is defined in `expr.capnp` and is imported here.
# I have deleted the expr.capnp, I will be removing it from the API.

@0xcafd186b931a2877;

using import "expr.capnp".Duration;
using import "expr.capnp".Timestamp;
using import "expr.capnp".Value;
using import "expr.capnp".Fields;

struct Request {
    id @0 :Text;
    rpcVersion @1 :UInt32;

    command :union {
        health @2 :HealthParams;
        version @3 :VersionParams;
        ping @4 :PingParams;
        info @5 :InfoParams;
        use @6 :UseParams;
        signup @7 :SignupParams;
        signin @8 :SigninParams;
        authenticate @9 :AuthenticateParams;
        invalidate @10 :InvalidateParams;
        reset @11 :ResetParams;
        kill @12 :KillParams;
        live @13 :LiveParams;
        set @14 :SetParams;
        unset @15 :UnsetParams;
        select @16 :SelectParams;
        insert @17 :InsertParams;
        create @18 :CreateParams;
        upsert @19 :UpsertParams;
        update @20 :UpdateParams;
        merge @21 :MergeParams;
        patch @22 :PatchParams;
        delete @23 :DeleteParams;
        query @24 :QueryParams;
        rawQuery @25 :RawQueryParams;
        relate @26 :RelateParams;
        run @27 :RunParams;
        graphql @28 :GraphQLParams;
        insertRelation @29 :InsertRelationParams;
    }
}

struct HealthParams {
}
struct VersionParams {
}
struct PingParams {
}
struct InfoParams {
}
struct UseParams {
    namespace @0 :Text;
    database @1 :Text;
}
struct SignupParams {
    namespace @0 :Text;
    database @1 :Text;
    access @2 :Text;
    accessParams @3 :List(Text);
}
struct SigninParams {
    access @0 :Access;
}
struct AuthenticateParams {
    token @0 :Text;
}
struct InvalidateParams {
}
struct CreateParams {
}
struct ResetParams {
}
struct KillParams {
    liveUuid @0 :Text;  # The UUID of the live query to kill.
}
struct LiveParams {
    table @0 :Text;
    fields @1 :Fields;  # Assuming Fields is defined in expr.capnp
    diff @2 :Bool;
    cond @3 :Value;  # Assuming Value is defined in expr.capnp
    fetchs @4 :List(Value);  # Assuming Fetchs is defined in expr.capnp
    vars @5 :List(Value);  # Assuming Value is defined in expr.capnp
}
struct SetParams {
    key @0 :Text;
    value @1 :Value;  # Assuming Value is defined in expr.capnp
}
struct UnsetParams {
    key @0 :Text;
}
struct SelectParams {
    what @0 :Value;  # Assuming Value is defined in expr.capnp
    # options @1 :StatementOptions;  # Assuming StatementOptions is defined in expr.capnp
}
struct InsertParams {
}
struct UpsertParams {
}
struct UpdateParams {
}
struct MergeParams {
}
struct PatchParams {
}
struct DeleteParams {
}
struct QueryParams {
    variables @0 :List(Value);  # Assuming Value is defined in expr.capnp
}
struct RawQueryParams {
    query @0 :Text;
    variables @1 :List(Value);  # Assuming Value is defined in expr.capnp
}
struct RelateParams {
}
struct RunParams {
}
struct GraphQLParams {
}
struct InsertRelationParams {
}
struct Response {
    id @0 :Text;
    results @1 :List(QueryResult);
}
struct QueryResult {
    index @0 :UInt64;
    stats @1 :QueryStats;  # Assuming QueryStats is defined in expr.capnp

    result :union {
        value @2 :Value;  # Assuming Value is defined in expr.capnp
        error @3 :Error;  # Assuming Error is defined in expr.capnp
    }
}
struct QueryStats {
    startTime @0 :Timestamp;  # Assuming Timestamp is defined in expr.capnp
    executionDuration @1 :Duration;  # Assuming Duration is defined in expr.capnp
}
struct Error {
    code @0 :Int64;
    message @1 :Text;
}






struct RootUserCredentials {
    username @0 :Text;
    password @1 :Text;
}
struct NamespaceAccessCredentials {
    namespace @0 :Text;
    access @1 :Text;
    key @2 :Text;
}
struct DatabaseAccessCredentials {
    namespace @0 :Text;
    database @1 :Text;
    access @2 :Text;
    key @3 :Text;
    refresh @4 :Text;  # Optional refresh token.
}
struct NamespaceUserCredentials {
    namespace @0 :Text;
    username @1 :Text;
    password @2 :Text;
}
struct DatabaseUserCredentials {
    namespace @0 :Text;
    database @1 :Text;
    username @2 :Text;
    password @3 :Text;
}
struct Access {
    union {
        rootUser @0 :RootUserCredentials;
        namespace @1 :NamespaceAccessCredentials;
        database @2 :DatabaseAccessCredentials;
        namespaceUser @3 :NamespaceUserCredentials;
        databaseUser @4 :DatabaseUserCredentials;
    }
}
