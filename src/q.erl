-module(q).

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/2,

         get/2
        ]).

-export([
         from/1,
         join/2, join/3, join/4,
         where/1, where/2,
         select/1, select/2,
         set/1, set/2,
         data/1, data/2,
         group_by/1, group_by/2,
         order_by/1,
         limit/1, limit/2,
         offset/1, offset/2
        ]).

-type model() :: schema() | module().
-type query() :: #query{}.
-type schema() :: #{}.
-type data() :: [#{}].
-type select() :: #{atom() => qast:ast_node()} | qast:ast_node().
-type set() :: #{atom() => qast:ast_node()}.
-type order() :: [{qast:ast_node(), asc | desc}].
-type join_type() :: inner | left | right | full | {left, outer} | {right, outer} | {full, outer}.
-type qfun() :: fun((query()) -> query()).

-export_type([query/0]).

-export_type([
         model/0,
         schema/0,
         data/0,
         select/0,
         set/0,
         order/0,
         join_type/0,
         qfun/0
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
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = maps:get(table, Schema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts) end,
        SchemaFields),
    #query{
        schema = Schema,
        data=[Fields],
        select=Fields,
        tables=[{Table, TRef}]
    }.

-spec join(model() | query(), fun((data()) -> qast:ast_node())) -> qfun().
join(Info, Fun) ->
    join(inner, Info, Fun).

-spec join(join_type(), model() | query(), fun((data()) -> qast:ast_node())) -> qfun().
join(JoinType, Info, Fun) ->
    fun(Q) -> join(JoinType, Info, Fun, Q) end.

-spec join(join_type(), model() | query(), fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
join(JoinType, #query{select=Fields}=JoinQ, Fun, #query{data=Data, joins=Joins}=Q) ->
    TRef = make_ref(),
    NewData = Data ++ [Fields],
    JoinAst = qast:exp([
        qast:raw("("),
        qsql:select(JoinQ),
        qast:raw(") as "),
        qast:table(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(Fun, [NewData])}|Joins]
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
        qast:table(TRef)
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
            _ -> pg:'andalso'(OldWhere, Where)
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

%% =============================================================================
%% Internal functions
%% =============================================================================

call(Fun, Args) -> apply(equery_pt:transform_fun(Fun), Args).

get_schema(Schema) when is_map(Schema) -> Schema;
get_schema(Module) when is_atom(Module) -> Module:schema().
