-module(q).

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/2,

         get/2
        ]).

-export([
         from/1,
         using/1, using/2,
         with/2, with/3,
         recursive/2,
         join/2, join/3, join/4,
         where/1, where/2,
         select/1, select/2,
         set/1, set/2,
         data/1, data/2,
         group_by/1, group_by/2,
         on_conflict/2, on_conflict/3,
         order_by/1,
         limit/1, limit/2,
         offset/1, offset/2,

         distinct/0, distinct/1,
         distinct_on/1, distinct_on/2
        ]).

-type model() :: schema() | module().
-type query() :: #query{}.
-type table() :: {alias, qast:alias(), #{atom() => term()}}.
-type schema() :: #{fields => #{atom() => #{atom() => term()}}, table => binary()}.
-type data() :: [#{atom() => qast:ast_node()}].
-type select() :: #{atom() => qast:ast_node()} | qast:ast_node().
-type set() :: #{atom() => qast:ast_node()}.
-type order() :: [{qast:ast_node(), asc | desc}].
-type distinct() :: all | [atom()].
-type join_type() :: inner | left | right | full | {left, outer} | {right, outer} | {full, outer}.
-type qfun() :: fun((query()) -> query()).
-type conflict_target() :: any | [atom()].
-type conflict_action() :: nothing | set().

-export_type([query/0]).

-export_type([
         model/0,
         schema/0,
         data/0,
         select/0,
         set/0,
         order/0,
         join_type/0,
         qfun/0,
         conflict_target/0,
         conflict_action/0
        ]).

%% = Flow ======================================================================

-spec pipe(Q, [qfun()]) -> Q when Q :: query().
pipe(Query, Funs) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

-spec get(schema, query()) -> schema();
         (data, query()) -> data().
get(schema, #query{schema=Schema}) -> Schema;
get(data, #query{data=Data}) -> Data.

%% = Query builders ============================================================

-spec from(model()) -> query().
from(Info) when is_map(Info); is_atom(Info) ->
    Schema = get_schema(Info),
    {RealTable, Fields} = table_feilds(Schema),
    #query{
        schema = Schema,
        data=[Fields],
        select=Fields,
        tables=[RealTable]
    }.

using(Info) -> fun(Q) -> using(Info, Q) end.
using({alias, _AliasExp, FieldsExp}=Alias, #query{tables=[_|_]=Tables, data=Data}=Query) ->
    Query#query{
        tables = Tables ++ [Alias],
        data = Data ++ [FieldsExp]
    };
using(Info, #query{tables=[_|_]=Tables, data=Data}=Query) when is_map(Info); is_atom(Info) ->
    Schema = get_schema(Info),
    {RealTable, Fields} = table_feilds(Schema),
    Query#query{
        tables = Tables ++ [RealTable],
        data = Data ++ [Fields]
    }.

table_feilds(#{table := Table}=Schema) ->
    SchemaFields = maps:get(fields, Schema, #{}),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts) end,
        SchemaFields),
    RealTable = {real, equery_utils:wrap(Table), TRef},
    {RealTable, Fields}.


%% = Recursive =================================================================

recursive(#query{select=RFields}=BaseQuery, UnionFun) when is_map(RFields) ->
    Schema = ?MODULE:get(schema, BaseQuery),
    TRef = make_ref(),
    Fields = maps:map(
        fun(_N, Ast) -> qast:opts(Ast) end,
        RFields),
    FieldsExp = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts)
    end, Fields),
    InternalQ = #query{
        schema = (maps:with([model], Schema))#{
            fields => Fields
        },
        data = [FieldsExp],
        select = FieldsExp,
        tables = [{alias, qast:alias(TRef), FieldsExp}]
    },
    WithExpression = qast:exp([
        qast:raw("with recursive "),
        qast:alias(TRef),
        qast:raw(" as ("),
        qsql:select(BaseQuery),
        qast:raw(" union all "),
        qsql:select(call(UnionFun, [InternalQ])),
        qast:raw(") ")
    ]),
    InternalQ#query{with=WithExpression}.

-spec with(model() | query() | qast:ast_node(), fun((table()) -> qfun())) -> qfun().
with(Info, Fun) -> fun(Q) -> with(Info, Fun, Q) end.

-spec with(model() | query() | qast:ast_node(), fun((table()) -> qfun()), Q) -> Q when Q :: query().
with(Info, Fun, Q) when is_map(Info); is_atom(Info) ->
    with(from(Info), Fun, Q);
with(#query{}=Query, Fun, Q) ->
    with(qsql:select(Query), Fun, Q);
with(Ast, Fun, Q) ->
    #{type := {model, _Model, Fields}} = Opts = qast:opts(Ast),
    TRef = make_ref(),
    FieldsExp = lists:foldl(fun({N, O}, Acc) ->
        Acc#{N => qast:field(TRef, N, O)}
    end, #{}, Fields),
    Alias = qast:alias(TRef, Opts),
    WithExpression = qast:exp([
        qast:raw("with "),
        Alias,
        qast:raw(" as ("), Ast, qast:raw(") ")
    ]),
    (call(Fun, [{alias, Alias, FieldsExp}]))(Q#query{with=WithExpression}).

-spec join(model() | query() | table(), fun((data()) -> qast:ast_node())) -> qfun().
join(Info, Fun) ->
    join(inner, Info, Fun).

-spec join(join_type(), model() | query() | table(), fun((data()) -> qast:ast_node())) -> qfun().
join(JoinType, Info, Fun) ->
    fun(Q) -> join(JoinType, Info, Fun, Q) end.

-spec join(join_type(), model() | query() | table(), fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
join(JoinType, #query{}=JoinQ, Fun, #query{data=Data, joins=Joins}=Q) ->
    TRef = make_ref(),
    {FieldsData, JoinQR} = extract_data(TRef, JoinQ),
    NewData = Data ++ [FieldsData],
    JoinAst = qast:exp([
        qast:raw("("),
        qsql:select(JoinQR),
        qast:raw(") as "),
        qast:alias(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(Fun, [NewData])}|Joins]
    };
join(JoinType, {alias, TableAlias, FieldsExp}, Fun, #query{data=Data, joins=Joins}=Q) ->
    NewData = Data ++ [FieldsExp],
    Q#query{
        data=NewData,
        joins=[{JoinType, TableAlias, call(Fun, [NewData])}|Joins]
    };
join(JoinType, Info, Fun, #query{data=Data, joins=Joins}=Q) ->
    JoinSchema = get_schema(Info),
    SchemaFields = maps:get(fields, JoinSchema, #{}),
    Table = maps:get(table, JoinSchema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, O) -> qast:field(TRef, N, O) end,
        SchemaFields),
    NewData = Data ++ [Fields],
    JoinAst = qast:exp([
        qast:raw([equery_utils:wrap(Table), " as "]),
        qast:alias(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(Fun, [NewData])}|Joins]
    }.

-spec where(fun((data()) -> qast:ast_node())) -> qfun().
where(Fun) -> fun(Q) -> where(Fun, Q) end.

-spec where(fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
where(Fun, #query{data=Data, where=OldWhere}=Q) ->
    Where = call(Fun, [Data]),
    NewWhere =
        case OldWhere of
            undefined -> Where;
            _ -> pg_sql:'andalso'(OldWhere, Where)
        end,
    Q#query{where = NewWhere}.


-spec select(Fun) -> qfun() when
      Fun :: fun((data()) -> select()) |
             fun((select(), data()) -> select()).
select(Fun) -> fun(Q) -> select(Fun, Q) end.

-spec select(Fun, Q) -> Q when
      Fun :: fun((data()) -> select()) |
             fun((select(), data()) -> select()),
      Q :: query().
select(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Q#query{select=call(Fun, [Data])};
select(Fun, #query{select=PrevSelect, data=Data}=Q) when is_function(Fun, 2) ->
    Q#query{select=call(Fun, [PrevSelect, Data])}.


-spec set(Fun) -> qfun() when
      Fun :: fun((data()) -> set()) |
             fun((set(), data()) -> set()).
set(Fun) -> fun(Q) -> set(Fun, Q) end.

-spec set(Fun, Q) -> Q when
      Fun :: fun((data()) -> set()) |
             fun((set(), data()) -> set()),
      Q :: query().
set(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Set = call(Fun, [Data]),
    is_map(Set) orelse error(bad_map),
    Q#query{set=Set};
set(Fun, #query{set=PrevSet, data=Data}=Q) when is_function(Fun, 2) ->
    Set = call(Fun, [PrevSet, Data]),
    is_map(Set) orelse error(bad_map),
    Q#query{set=Set}.


-spec group_by(fun((data()) -> qast:ast_node())) -> qfun().
group_by(Fun) -> fun(Q) -> group_by(Fun, Q) end.

-spec group_by(fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
group_by(Fun, #query{data=Data}=Q) ->
    Q#query{group_by=call(Fun, [Data])}.

-spec on_conflict(conflict_target(), fun((data) -> conflict_action())) -> qfun().
on_conflict(ConflictTarget, Fun) -> fun(Q) -> on_conflict(ConflictTarget, Fun, Q) end.

-spec on_conflict(conflict_target(), fun((data) -> conflict_action()), Q) -> Q when Q :: query().
on_conflict(ConflictTarget, Fun, #query{on_conflict=OnConflict, data=Data}=Q) ->
    Schema = get(schema, Q),
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = qast:raw("EXCLUDED"),
    Fields = maps:map(fun(N, Opts) ->
        qast:exp([Table, qast:raw([".", equery_utils:field_name(N)])], Opts)
    end, SchemaFields),
    Q#query{on_conflict=maps:put(ConflictTarget, call(Fun, [Data ++ [Fields]]), OnConflict)}.

-spec order_by(fun((data()) -> order())) -> qfun().
order_by(Fun) -> fun(Q) -> order_by(Fun, Q) end.

-spec order_by(fun((data()) -> order()), Q) -> Q when Q :: query().
order_by(Fun, #query{data=Data}=Q) ->
    Q#query{order_by=call(Fun, [Data])}.

-spec limit(non_neg_integer()) -> qfun().
limit(Value) -> fun(Q) -> limit(Value, Q) end.

-spec limit(non_neg_integer(), Q) -> Q when Q :: query().
limit(Value, Q) ->
    Q#query{limit=Value}.

-spec offset(non_neg_integer()) -> qfun().
offset(Value) -> fun(Q) -> offset(Value, Q) end.

-spec offset(non_neg_integer(), Q) -> Q when Q :: query().
offset(Value, Q) ->
    Q#query{offset=Value}.


-spec data(fun((data()) -> data())) -> qfun().
data(Fun) -> fun(Q) -> data(Fun, Q) end.

-spec data(fun((data()) -> data()), Q) -> Q when Q :: query().
data(Fun, #query{data=Data}=Q) ->
    Data2 = call(Fun, [Data]),
    is_list(Data2) orelse error(bad_list),
    Q#query{data=Data2}.

-spec distinct() -> qfun().
distinct() -> fun(Q) -> distinct(Q) end.

-spec distinct(Q) -> Q when Q :: query().
distinct(#query{}=Q) -> Q#query{distinct = all}.

-spec distinct_on(fun((data()) -> [atom()])) -> qfun().
distinct_on(Fun) -> fun(Q) -> distinct_on(Fun, Q) end.

-spec distinct_on(fun((data()) -> [atom()]), Q) -> Q when Q :: query().
distinct_on(Fun, #query{data=Data}=Q) ->
    Distinct = call(Fun, [Data]),
    is_list(Distinct) orelse error(bad_list),
    Q#query{distinct = Distinct}.

%% =============================================================================
%% Internal functions
%% =============================================================================

call(Fun, Args) -> apply(equery_pt:transform_fun(Fun), Args).

get_schema(Schema) when is_map(Schema) -> Schema;
get_schema(Module) when is_atom(Module) -> (Module:schema())#{model => Module}.

field_alias(Int) ->
    equery_utils:wrap(["__field-",integer_to_list(Int)]).

gen_aliases(Fields) when is_map(Fields) ->
    {FieldAliases, _} = lists:mapfoldl(fun({K, V}, Acc) ->
        Alias = field_alias(Acc),
        {{K, {Alias, qast:opts(V)}}, Acc+1}
    end, 1, maps:to_list(Fields)),
    maps:from_list(FieldAliases).

apply_aliases(Fields, Aliases) when is_map(Fields) ->
    maps:map(fun(K, V) ->
        {Alias, _Opts} = maps:get(K, Aliases),
        qast:exp([V, qast:raw(" as "), qast:raw(Alias)])
    end, Fields).

extract_data(TRef, #query{select=Fields}=Query) ->
    FieldsAliases = gen_aliases(Fields),
    TableData = maps:map(fun(_K, {Alias, Opts}) ->
        qast:exp([qast:alias(TRef), qast:raw([".", Alias])], Opts)
    end, FieldsAliases),
    NewQuery = q:select(fun(S, _) -> apply_aliases(S, FieldsAliases) end, Query),
    {TableData, NewQuery}.
