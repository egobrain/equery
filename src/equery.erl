-module(equery).

-export([
         parse_transform/2,
         compile/1
        ]).

-include("ast_helpers.hrl").

-record(state, {}).

parse_transform(Ast, _Opts) ->
    {module, _} = code:ensure_loaded(qast),
    {Ast2, _} = traverse(fun search_and_compile/2, undefined, Ast),
    %% ct:pal("~p", [pretty_print(Ast2)]),
    Ast2.

traverse(Fun, State, List) when is_list(List) ->
    lists:mapfoldl(fun(Node, St) ->
        traverse(Fun, St, Node)
    end, State, List);
traverse(Fun, State, Node) when is_tuple(Node) ->
    {Node2, State2} = Fun(Node, State),
    List = tuple_to_list(Node2),
    {Node3, State3} = traverse(Fun, State2, List),
    {list_to_tuple(Node3), State3};
traverse(_Fun, State, Node) ->
    {Node, State}.

search_and_compile({call, L1, {remote, L2, {atom, L3, q}, {atom, L4, F}}, Args}, St) when
      F =:= filter;
      F =:= join;
      F =:= compile ->
    {ArgsNode, St2} = lists:mapfoldl(
        fun({'fun', L, {clauses, Clauses}}, S) ->
                {Node2, S2} = compile(Clauses, S),
                RNode = {'fun', L, {clauses, Node2}},
                {RNode, S2};
           (N, S) -> {N, S}
        end, St, Args),
    RNode = {call, L1, {remote, L2, {atom, L3, q}, {atom, L4, F}}, ArgsNode},
    {RNode, St2};
search_and_compile(Node, St) ->
    {Node, St}.

compile(Clauses) ->
    {Clauses2, _St} = compile(Clauses, #state{}),
    Clauses2.

compile([{clause, _Line, [Cons], [], [Exp]}], St) ->
    {[{clause, _Line, [Cons], [], [filter_exp(Exp)]}], St};
compile(Ast, St) -> {Ast, St}.

filter_exp(Ast) ->
    {NewAst, _State} =
        traverse_(
            fun({op, _L, Op, A, B} = Node, S) ->
                case erlang:function_exported(qast, Op, 2) of
                    true -> {?apply(qast, Op, [A,B]), S};
                    false -> {Node, S}
                end;
               (Node, S) -> {Node, S}
            end, undefined, Ast),
    erl_syntax:revert(NewAst).

traverse_(Fun, State, List) when is_list(List) ->
    lists:mapfoldl(fun(L, S) -> traverse_(Fun, S, L) end, State, List);
traverse_(Fun, State, Tuple) when is_tuple(Tuple) ->
    L = tuple_to_list(Tuple),
    {L2, State2} = traverse_(Fun, State, L),
    Tuple2 = list_to_tuple(L2),
    Fun(Tuple2, State2);
traverse_(_Fun, State, Ast) ->
    {Ast, State}.

%% =============================================================================
%% Utils
%% =============================================================================

%% pretty_print(Forms0) ->
%%     Forms = epp:restore_typed_record_fields(revert(Forms0)),
%%     [io_lib:fwrite("~s~n",
%%                    [lists:flatten([erl_pp:form(Fm) ||
%%                                       Fm <- Forms])])].

%% revert(Tree) ->
%%     [erl_syntax:revert(T) || T <- lists:flatten(Tree)].
