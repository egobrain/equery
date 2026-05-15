-module(qast_tests).

-include_lib("eunit/include/eunit.hrl").

ast_utils_test_() ->
    NewOpts = #{value => a},
    Tests = [
       {"value", fun qast:value/1, 1, true, NewOpts},
       {"exp", fun qast:exp/1, [], true, NewOpts},
       {"raw", fun qast:raw/1, "test", true, NewOpts},
       {"alias", fun qast:alias/1, make_ref(), true, NewOpts},
       {"some value", fun(A) -> A end, 123, false, NewOpts}
    ],
    [
     {Name, fun() ->
         Ast = CFun(V),
         IsAst = qast:is_ast(Ast),
         #{} = qast:opts(Ast),
         Ast2 = qast:set_opts(Ast, NewOpts),
         true = qast:is_ast(Ast2),
         RNewOpts = qast:opts(Ast2)
     end} || {Name, CFun, V, IsAst, RNewOpts} <- Tests
    ].

join_test_() ->
    Sep = qast:raw(","),
    [
     ?_assertEqual(
          {<<>>, []},
          qast:to_sql(qast:join([], Sep))),
     ?_assertEqual(
          {<<"$1,$2">>, [1,2]},
          qast:to_sql(qast:join([1,2], Sep)))
    ].
