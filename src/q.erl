-module(q).

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/1,
         pipe/2,

         schema/1
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

-export([
         'andalso'/2,
         'orelse'/2,

         '=:='/2,
         '=/='/2,
         '>'/2,
         '>='/2,
         '<'/2,
         '=<'/2,
         'not'/1,

         '+'/2,
         '-'/2,
         '*'/2,
         '/'/2
        ]).

-export([
         sum/1,
         count/1,
         min/1,
         max/1
        ]).

-export([
         min/2,
         max/2,
         row/1
        ]).

-type query() :: #query{}.
-export_type([query/0]).

%% = Flow ======================================================================

pipe([Query|Funs]) ->
    pipe(Query, Funs).

pipe(Query, Funs) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

schema(#query{schema=Schema}) -> Schema.

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
        qast:raw(["\"", Table,  "\" as "]),
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
            _ -> 'andalso'(OldWhere, Where)
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
    is_map(Set) orelse error(bad_set),
    Q#query{set=Set};
set(Fun, #query{set=PrevSet, data=Data}=Q) when is_function(Fun, 2) ->
    Set = call(Fun, [PrevSet, Data]),
    is_map(Set) orelse error(bad_set),
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
%% Sql operations
%% =============================================================================

%% = Primitive =================================================================

%% @TODO wrap values in $value before validation and add $value match
'andalso'(true, B) -> B;
'andalso'(A, true) -> A;

'andalso'(false, _) -> false;
'andalso'(_, false) -> false;

'andalso'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" and "), B, qast:raw(")")], #{type => boolean}).

'orelse'(true, _) -> true;
'orelse'(_, true) -> true;
'orelse'(false, B) -> B;
'orelse'(A, false) -> A;
'orelse'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" or "), B, qast:raw(")")], #{type => boolean}).

'not'(A) when is_boolean(A) -> not A;
'not'(A) ->
    qast:exp([qast:raw("not "), A], #{type => boolean}).

'=:='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" = "), B, qast:raw(")")], #{type => boolean}).

'=/='(A, B) -> 'not'('=:='(A,B)).

'>'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" > "), B, qast:raw(")")], #{type => boolean}).
'>='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" >= "), B, qast:raw(")")], #{type => boolean}).
'<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" < "), B, qast:raw(")")], #{type => boolean}).
'=<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" <= "), B, qast:raw(")")], #{type => boolean}).

%% @TODO type opts
'+'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" + "), B, qast:raw(")")]).
'-'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" - "), B, qast:raw(")")]).
'*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" * "), B, qast:raw(")")]).
'/'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" / "), B, qast:raw(")")]).

%% = Aggregators ===============================================================

sum(Ast) ->
    qast:exp([qast:raw("sum("), Ast, qast:raw(")")], qast:opts(Ast)).

count(Ast) ->
    qast:exp([qast:raw("count("), Ast, qast:raw(")")], #{type => integer}).

min(A) ->
    qast:exp([qast:raw("min("), A, qast:raw(")")], qast:opts(A)).

max(A) ->
    qast:exp([qast:raw("max("), A, qast:raw(")")], qast:opts(A)).

%% = Math ======================================================================

min(A, B) ->
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

max(A, B) ->
    qast:exp([qast:raw("GREATEST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

row(Fields) when is_map(Fields) ->
    FieldsList = maps:to_list(Fields),
    Type = {record, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]},
    qast:exp([
        qast:raw("row("),
        qast:join([Node || {_F, Node} <- FieldsList], qast:raw(",")),
        qast:raw(")")
    ], #{type => Type}).

%% =============================================================================
%% Internal functions
%% =============================================================================

call(Fun, Args) -> apply(equery_pt:transform_fun(Fun), Args).

get_schema(Schema) when is_map(Schema) -> Schema;
get_schema(Module) when is_atom(Module) -> Module:schema().
