-module(equery_utils_tests).

-include_lib("eunit/include/eunit.hrl").

to_binary_test() ->
    ?assertEqual(<<"atom">>, equery_utils:to_binary(atom)),
    ?assertEqual(<<"123">>, equery_utils:to_binary(123)),
    ?assertEqual(<<"bin">>, equery_utils:to_binary(<<"bin">>)).
