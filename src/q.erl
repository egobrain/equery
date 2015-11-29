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

pipe([Query|Funs]) ->
    pipe(Funs, Query).

pipe(Funs, Query) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

from(Schema) when is_map(Schema) ->
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = maps:get(table, Schema),
    TRef = make_ref(),
    %% RandT = rand_table_name(Table),
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
        joins=[{inner, {Table, TRef}, (compile_fun(Fun))(NewData)}|Joins]
    }.

filter(Fun) ->
    fun(Q) -> filter(Fun, Q) end.

filter(Fun, #query{data=Data, filter=OldFilter}=Q) ->
    Filter = (compile_fun(Fun))(Data),
    NewFilter =
        case OldFilter of
            undefined -> Filter;
            _ -> qast:'andalso'(OldFilter, Filter)
        end,
    Q#query{filter = NewFilter}.

compile_fun(Fun) ->
    {env, Env} = erlang:fun_info(Fun, env),
    case Env of
        [{Bindings, {eval, _}, {value, _}, Ast}] ->
            Exprs = erl_syntax:revert(?func(compile_filter(Ast))),
            {value, Fun2, _} = erl_eval:expr(Exprs, Bindings),
            Fun2;
        _ -> Fun
    end.

compile_filter([{clause, _Line, [Cons], [], [Exp]}]) ->
    [{clause, _Line, [Cons], [], [filter_exp(Exp)]}];
compile_filter(Ast) -> Ast.

filter_exp(Ast) ->
    {NewAst, _State} =
        traverse(
            fun({op, _L, Op, A, B} = Node, S) ->
                case erlang:function_exported(qast, Op, 2) of
                    true -> {?apply(qast, Op, [A,B]), S};
                    false -> {Node, S}
                end;
               (Node, S) -> {Node, S}
            end, undefined, Ast),
    erl_syntax:revert(NewAst).

%% =============================================================================
%% Interanl function
%% =============================================================================

traverse(Fun, State, List) when is_list(List) ->
    lists:mapfoldl(fun(L, S) -> traverse(Fun, S, L) end, State, List);
traverse(Fun, State, Tuple) when is_tuple(Tuple) ->
    L = tuple_to_list(Tuple),
    {L2, State2} = traverse(Fun, State, L),
    Tuple2 = list_to_tuple(L2),
    Fun(Tuple2, State2);
traverse(_Fun, State, Ast) ->
    {Ast, State}.
