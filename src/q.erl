-module(q).

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/1,
         pipe/2
        ]).

-export([
         from/1,
         join/2, join/3,
         filter/1, filter/2
        ]).

-export([
         compile/1
        ]).

pipe([Query|Funs]) ->
    pipe(Funs, Query).

pipe(Funs, Query) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

from(Schema) when is_map(Schema) ->
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = maps:get(table, Schema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, _FieldD) -> qast:field(TRef, N) end,
        SchemaFields),
    #query{
        schemas = [Schema],
        data=[Fields],
        select=Fields,
        tables=[{Table, TRef}]
    }.

join(Schema, Fun) ->
    fun(Q) -> join(Schema, Fun, Q) end.

join(Schema, Fun, #query{schemas=Schemas, data=Data, joins=Joins}=Q) ->
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = maps:get(table, Schema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, _FieldD) -> qast:field(TRef, N) end,
        SchemaFields),
    NewData = Data ++ [Fields],
    Q#query{
        schemas=[Schema|Schemas],
        data=NewData,
        joins=[{inner, {Table, TRef}, call(Fun, NewData)}|Joins]
    }.

filter(Fun) ->
    fun(Q) -> filter(Fun, Q) end.

filter(Fun, #query{data=Data, filter=OldFilter}=Q) ->
    Filter = call(Fun, Data),
    NewFilter =
        case OldFilter of
            undefined -> Filter;
            _ -> qast:'andalso'(OldFilter, Filter)
        end,
    Q#query{filter = NewFilter}.

call(Fun, Data) -> (compile(Fun))(Data).

compile(Fun) ->
    {env, Env} = erlang:fun_info(Fun, env),
    case Env of
        [{Bindings, {eval, _}, {value, _}, Ast}] ->
            Exprs = erl_syntax:revert(?func(equery:compile(Ast))),
            {value, Fun2, _} = erl_eval:expr(Exprs, Bindings),
            Fun2;
        _ -> Fun
    end.
