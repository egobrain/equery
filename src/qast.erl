-module(qast).

-export([
         field/3,
         value/1, value/2,
         raw/1,
         exp/1, exp/2,
         table/1,

         is_ast/1,
         opts/1,
         set_opts/2,
         join/2
        ]).

-export([
         to_sql/1
        ]).

%% =============================================================================
%% Types
%% =============================================================================

-type opts() :: #{}.
-type raw() :: {'$raw', opts(), iodata()}.
-type value() :: {'$value', opts(), any()}.
-type table() :: {'$table', reference()}.
-type exp() :: {'$exp', opts(), [ast_node() | any()]}.

-type ast_node() :: raw() | value() | table() | exp().

-export_type([opts/0, raw/0, value/0, table/0, exp/0, ast_node/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec field(reference(), atom(), opts()) -> exp().
field(TableRef, Name, Opts) ->
    exp([table(TableRef), raw([".", equery_utils:field_name(Name)])], Opts).

-spec value(any()) -> value().
-spec value(any(), opts()) -> value().
value(V) -> value(V, #{}).
value(V, Opts) -> {'$value', Opts, V}.

-spec exp([ast_node() | any()]) -> exp().
-spec exp([ast_node() | any()], opts()) -> exp().
exp(V) -> exp(V, #{}).
exp(V, Opts) -> {'$exp', Opts, V}.

-spec raw(iodata()) -> raw().
-spec raw(iodata(), opts()) -> raw().
raw(V) -> raw(V, #{}).
raw(V, Opts) -> {'$raw', Opts, V}.

-spec table(reference()) -> table().
table(Ref) -> {'$table', Ref}.

-spec opts(ast_node()) -> opts().
opts({'$value', Opts, _}) -> Opts;
opts({'$exp', Opts, _}) -> Opts;
opts({'$raw', Opts, _}) -> Opts;
opts(_) -> #{}.

-spec set_opts(ast_node(), opts()) -> ast_node().
set_opts({'$value', _Opts, Value}, NewOpts) -> {'$value', NewOpts, Value};
set_opts({'$exp', _Opts, Exp}, NewOpts) -> {'$exp', NewOpts, Exp};
set_opts({'$raw', _Opts, Raw}, NewOpts) -> {'$raw', NewOpts, Raw};
set_opts({'$table', _Raw}=Node, _NewOpts) -> Node;
set_opts(V, NewOpts) -> value(V, NewOpts).

-spec is_ast(any()) -> boolean().
is_ast({'$value', _Opts, _Value}) -> true;
is_ast({'$exp', _Opts, _Exp}) -> true;
is_ast({'$raw', _Opts, _Raw}) -> true;
is_ast({'$table', _Raw}) -> true;
is_ast(_) -> false.

%% =============================================================================
%% Utils
%% =============================================================================

-spec join([ast_node()], ast_node()) -> exp().
join([], _Sep) -> qast:exp([]);
join([H|T], Sep) ->
    qast:exp([H|lists:foldr(fun(I, Acc) -> [Sep,I|Acc] end, [], T)]).

-record(state, {
            aliases=#{}, tables_cnt=0,
            args=[], args_cnt=0
       }).

-spec to_sql(ast_node()) -> {Sql :: binary(), Args :: [any()]}.
to_sql(Ast) ->
    {Sql, #state{args=Args}} = traverse(
        fun({'$value', _Opts, V}, #state{args=Vs, args_cnt=Cnt}=St) ->
               NewCnt = Cnt+1,
               {index(NewCnt), St#state{args=[V|Vs], args_cnt=NewCnt}};
           ({'$table', TRef}, St) ->
               get_table_alias(TRef, St);
           ({'$raw', _Opts, V}, St) ->
               {V, St}
        end, #state{}, Ast),
    {iolist_to_binary(Sql), lists:reverse(Args)}.

%% =============================================================================
%% Internal
%% =============================================================================

traverse(F, Acc, {'$exp', _Opts, List}) ->
    lists:mapfoldl(fun(E, A) -> traverse(F, A, E) end, Acc, List);
traverse(F, Acc, {'$raw', _Opts, _}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$value', _Opts, _V}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$table', _V}=Item) ->
    F(Item, Acc);
%% Other is value
traverse(F, Acc, V) ->
    F(qast:value(V), Acc).

index(N) ->
    [ $$, integer_to_binary(N) ].

table_alias(Int) ->
    equery_utils:wrap(["__table-",integer_to_list(Int)]).

get_table_alias(TRef, #state{aliases=As, tables_cnt=Cnt}=St) ->
    case maps:find(TRef, As) of
        {ok, TAlias} -> {TAlias, St};
        error ->
            TAlias = table_alias(Cnt),
            As2 = maps:put(TRef, TAlias, As),
            {TAlias, St#state{aliases=As2, tables_cnt=Cnt+1}}
    end.

%% =============================================================================
%% Tests
%% =============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

ast_utils_test_() ->
    NewOpts = #{value => a},
    Tests = [
       {"value", fun value/1, 1, true, NewOpts},
       {"exp", fun exp/1, [], true, NewOpts},
       {"raw", fun raw/1, "test", true, NewOpts},
       {"table", fun table/1, make_ref(), true, #{}},
       {"some value", fun(A) -> A end, 123, false, NewOpts}
    ],
    [
     {Name, fun() ->
         Ast = CFun(V),
         IsAst = is_ast(Ast),
         #{} = opts(Ast),
         Ast2 = set_opts(Ast, NewOpts),
         true = is_ast(Ast2),
         RNewOpts = opts(Ast2)
     end} || {Name, CFun, V, IsAst, RNewOpts} <- Tests
    ].

join_test_() ->
    Sep = qast:raw(","),
    [
     ?_assertEqual(
          {<<>>, []},
          qast:to_sql(join([], Sep))),
     ?_assertEqual(
          {<<"$1,$2">>, [1,2]},
          qast:to_sql(join([1,2], Sep)))
    ].

-endif.
