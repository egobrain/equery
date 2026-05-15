-module(pg_sql_tests).

-include_lib("eunit/include/eunit.hrl").

type_str_test() ->
    ?assertEqual(<<"bigint">>, pg_sql:type_str(bigint)),
    ?assertEqual(<<"int[]">>, pg_sql:type_str({array, int})),
    ?assertEqual(<<"custom()">>, pg_sql:type_str({custom, []})),
    ?assertEqual(<<"custom(a,1,2.0)">>, pg_sql:type_str({custom, [<<"a">>, 1, 2.0]})).
