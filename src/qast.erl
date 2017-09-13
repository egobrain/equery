-module(qast).

-export([
         field/3,
         value/1, value/2,
         raw/1, raw/2,
         exp/1, exp/2,
         alias/1, alias/2,

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

-type opts() :: #{type => term(), any() => any()}.
-type raw() :: {'$raw', opts(), iodata()}.
-type value() :: {'$value', opts(), any()}.
-type alias() :: {'$alias', opts(), reference()}.
-type exp() :: {'$exp', opts(), [ast_node() | any()]}.

-type ast_node() :: raw() | value() | alias() | exp().

-export_type([opts/0, raw/0, value/0, alias/0, exp/0, ast_node/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec field(reference(), atom(), opts()) -> exp().
field(TableRef, Name, Opts) ->
    exp([alias(TableRef), raw([".", equery_utils:field_name(Name)])], Opts).

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

-spec alias(reference()) -> alias().
-spec alias(reference(), opts()) -> alias().
alias(Ref) -> alias(Ref, #{}).
alias(Ref, Opts) -> {'$alias', Opts, Ref}.

-spec opts(ast_node()) -> opts().
opts({'$value', Opts, _}) -> Opts;
opts({'$exp', Opts, _}) -> Opts;
opts({'$raw', Opts, _}) -> Opts;
opts({'$alias', Opts, _}) -> Opts;
opts(_) -> #{}.

-spec set_opts(ast_node(), opts()) -> ast_node().
set_opts({'$value', _Opts, Value}, NewOpts) -> value(Value, NewOpts);
set_opts({'$exp', _Opts, Exp}, NewOpts) -> exp(Exp, NewOpts);
set_opts({'$raw', _Opts, Raw}, NewOpts) -> raw(Raw, NewOpts);
set_opts({'$alias', _Opts, TRef}, NewOpts) -> alias(TRef, NewOpts);
set_opts(V, NewOpts) -> value(V, NewOpts).

-spec is_ast(any()) -> boolean().
is_ast({'$value', _Opts, _Value}) -> true;
is_ast({'$exp', _Opts, _Exp}) -> true;
is_ast({'$raw', _Opts, _Raw}) -> true;
is_ast({'$alias', _Opts, _Raw}) -> true;
is_ast(_) -> false.

%% =============================================================================
%% Utils
%% =============================================================================

-spec join([ast_node()], ast_node()) -> exp().
join([], _Sep) -> qast:exp([]);
join([H|T], Sep) ->
    qast:exp([H|lists:foldr(fun(I, Acc) -> [Sep,I|Acc] end, [], T)]).

-record(state, {
            aliases=#{}, aliases_cnt=0,
            args=[], args_cnt=0
       }).

-spec to_sql(ast_node()) -> {Sql :: binary(), Args :: [any()]}.
to_sql(Ast) ->
    {Sql, #state{args=Args}} = traverse(
        fun({'$value', _Opts, V}, #state{args=Vs, args_cnt=Cnt}=St) ->
               NewCnt = Cnt+1,
               {index(NewCnt), St#state{args=[V|Vs], args_cnt=NewCnt}};
           ({'$alias', _Opts, TRef}, St) ->
               get_alias(TRef, St);
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
traverse(F, Acc, {'$alias', _Opts, _V}=Item) ->
    F(Item, Acc);
%% Other is value
traverse(F, Acc, V) ->
    F(qast:value(V), Acc).

index(N) ->
    [ $$, integer_to_binary(N) ].

alias_str(Int) ->
    equery_utils:wrap(["__alias-",integer_to_list(Int)]).

get_alias(Ref, #state{aliases=Aliases, aliases_cnt=Cnt}=St) ->
    case maps:find(Ref, Aliases) of
        {ok, Alias} -> {Alias, St};
        error ->
            AliasStr = alias_str(Cnt),
            Aliases2 = maps:put(Ref, AliasStr, Aliases),
            {AliasStr, St#state{aliases=Aliases2, aliases_cnt=Cnt+1}}
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
       {"alias", fun alias/1, make_ref(), true, NewOpts},
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
