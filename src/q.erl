-module(q).

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/2,

         get/2
        ]).

-export([
         from/1,
         join/2, join/3,
         where/1, where/2,
         select/1, select/2,
         set/1, set/2,
         data/1, data/2,
         group_by/1, group_by/2,
         order_by/1,
         limit/1, limit/2,
         offset/1, offset/2
        ]).

-type query() :: #query{}.
-export_type([query/0]).

%% = Flow ======================================================================

pipe(Query, Funs) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

get(schema, #query{schema=Schema}) -> Schema;
get(data, #query{data=Data}) -> Data.

%% = Query builders ============================================================

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

join(Info, Fun) ->
    fun(Q) -> join(Info, Fun, Q) end.

join(#query{select=Fields}=JoinQ, Fun, #query{data=Data, joins=Joins}=Q) ->
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
        joins=[{inner, JoinAst, call(Fun, [NewData])}|Joins]
    };
join(Info, Fun, #query{data=Data, joins=Joins}=Q) ->
    Schema = get_schema(Info),
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = maps:get(table, Schema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts) end,
        SchemaFields),
    NewData = Data ++ [Fields],
    JoinAst = qast:exp([
        qast:raw([equery_utils:wrap(Table), " as "]),
        qast:table(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{inner, JoinAst, call(Fun, [NewData])}|Joins]
    }.

where(Fun) ->
    fun(Q) -> where(Fun, Q) end.

where(Fun, #query{data=Data, where=OldWhere}=Q) ->
    Where = call(Fun, [Data]),
    NewWhere =
        case OldWhere of
            undefined -> Where;
            _ -> pg:'andalso'(OldWhere, Where)
        end,
    Q#query{where = NewWhere}.

select(Fun) ->
    fun(Q) -> select(Fun, Q) end.
select(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Q#query{select=call(Fun, [Data])};
select(Fun, #query{select=PrevSelect, data=Data}=Q) when is_function(Fun, 2) ->
    Q#query{select=call(Fun, [PrevSelect, Data])}.

set(Fun) ->
    fun(Q) -> set(Fun, Q) end.
set(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Set = call(Fun, [Data]),
    is_map(Set) orelse error(bad_map),
    Q#query{set=Set};
set(Fun, #query{set=PrevSet, data=Data}=Q) when is_function(Fun, 2) ->
    Set = call(Fun, [PrevSet, Data]),
    is_map(Set) orelse error(bad_map),
    Q#query{set=Set}.

group_by(Fun) ->
    fun(Q) -> group_by(Fun, Q) end.
group_by(Fun, #query{data=Data}=Q) ->
    Q#query{group_by=call(Fun, [Data])}.

order_by(Fun) ->
    fun(Q) -> order_by(Fun, Q) end.
order_by(Fun, #query{data=Data}=Q) ->
    Q#query{order_by=call(Fun, [Data])}.

limit(Value) ->
    fun(Q) -> limit(Value, Q) end.
limit(Value, Q) ->
    Q#query{limit=Value}.

offset(Value) ->
    fun(Q) -> offset(Value, Q) end.
offset(Value, Q) ->
    Q#query{offset=Value}.

%% @unstable
data(Fun) ->
    fun(Q) -> data(Fun, Q) end.
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
