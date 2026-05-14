-module(qjson_tests).

-export([schema/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("equery/include/equery.hrl").

schema() ->
    #{
        fields => #{
            id => #{ type => serial, index => true },
            payload => #{type => json, required => true}
        },
        table => <<"data">>
    }.

'->_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'->'(Payload, name)
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" -> $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([name], Args),
    ?assertEqual(json, Type).

'->>_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'->>'(Payload, name)
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" ->> $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([name], Args),
    ?assertEqual(text, Type).

'#>_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'#>'(Payload, [emails, 1])
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" #> $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([[emails, 1]], Args),
    ?assertEqual(json, Type).

'#>>_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'#>>'(Payload, [emails, 1])
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" #>> $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([[emails, 1]], Args),
    ?assertEqual(text, Type).

'@>_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'@>'(Payload, <<"{\"a\":1}">>)
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" @> $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([<<"{\"a\":1}">>], Args),
    ?assertEqual(boolean, Type).

'<@_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'<@'(Payload, <<"{\"a\":1}">>)
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" <@ $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([<<"{\"a\":1}">>], Args),
    ?assertEqual(boolean, Type).

'?_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'?'(Payload, 1)
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" ? $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([1], Args),
    ?assertEqual(boolean, Type).


'?|_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'?|'(Payload, [1, 2])
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" ?| $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([[1, 2]], Args),
    ?assertEqual(boolean, Type).

'?&_test'() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(
                fun([#{payload := Payload}|_]) ->
                    qjson:'?&'(Payload, [1, 2])
                end)
        ]))),
    ?assertEqual(<<"select "
        "\"__alias-0\".\"payload\" ?& $1 "
        "from \"data\" as \"__alias-0\"">>, Sql),
    ?assertEqual([[1, 2]], Args),
    ?assertEqual(boolean, Type).

build_object_test() ->
    {Sql, Args, Type} = to_sql(
        qjson:jsonb_build_object(#{name => qast:value(<<"alice">>),
                                   count => qast:value(5)})),
    ?assertEqual(<<"jsonb_build_object($1,$2,$3,$4)">>, Sql),
    ?assertEqual([<<"count">>, 5, <<"name">>, <<"alice">>], Args),
    ?assertEqual(jsonb, Type).

build_object_binary_keys_test() ->
    A = qast:raw("a"),
    {Sql, Args, _} = to_sql(qjson:json_build_object(#{<<"key1">> => A, <<"key2">> => A})),
    ?assertEqual(<<"json_build_object($1,a,$2,a)">>, Sql),
    ?assertEqual([<<"key1">>, <<"key2">>], Args).

build_array_test() ->
    {Sql, Args, Type} = to_sql(
        qjson:jsonb_build_array([qast:value(1), qast:value(<<"x">>), qast:value(true)])),
    ?assertEqual(<<"jsonb_build_array($1,$2,$3)">>, Sql),
    ?assertEqual([1, <<"x">>, true], Args),
    ?assertEqual(jsonb, Type).

to_jsonb_test() ->
    A = qast:raw("a"),
    ?assertEqual({<<"to_jsonb(a)">>, []}, qast:to_sql(qjson:to_jsonb(A))),
    ?assertEqual({<<"to_json(a)">>, []}, qast:to_sql(qjson:to_json(A))),
    ?assertEqual({<<"row_to_json(a)">>, []}, qast:to_sql(qjson:row_to_json(A))),
    ?assertEqual({<<"array_to_json(a)">>, []}, qast:to_sql(qjson:array_to_json(A))).

jsonb_set_test() ->
    Target = qast:raw("t"),
    Value = qast:value(<<"\"dark\"">>, #{type => jsonb}),
    {Sql, Args, Type} = to_sql(qjson:jsonb_set(Target, [<<"prefs">>, <<"theme">>], Value)),
    ?assertEqual(<<"jsonb_set(t,$1,$2)">>, Sql),
    ?assertEqual([[<<"prefs">>, <<"theme">>], <<"\"dark\"">>], Args),
    ?assertEqual(jsonb, Type),
    %% with create_missing
    {Sql2, Args2, _} = to_sql(qjson:jsonb_set(Target, [<<"k">>], Value, true)),
    ?assertEqual(<<"jsonb_set(t,$1,$2,$3)">>, Sql2),
    ?assertEqual([[<<"k">>], <<"\"dark\"">>, true], Args2).

jsonb_insert_test() ->
    Target = qast:raw("t"),
    Value = qast:value(<<"1">>, #{type => jsonb}),
    {Sql, _Args, Type} = to_sql(qjson:jsonb_insert(Target, [<<"a">>], Value)),
    ?assertEqual(<<"jsonb_insert(t,$1,$2)">>, Sql),
    ?assertEqual(jsonb, Type),
    {Sql2, _, _} = to_sql(qjson:jsonb_insert(Target, [<<"a">>], Value, true)),
    ?assertEqual(<<"jsonb_insert(t,$1,$2,$3)">>, Sql2).

jsonb_strip_nulls_test() ->
    A = qast:raw("a"),
    ?assertEqual({<<"jsonb_strip_nulls(a)">>, []},
                 qast:to_sql(qjson:jsonb_strip_nulls(A))).

build_object_query_test() ->
    {Sql, Args, _Type} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(fun([#{id := Id, payload := P}]) ->
                #{obj => qjson:jsonb_build_object(#{
                    id => Id,
                    raw => P,
                    flag => Id > 10
                })}
            end)
        ]))),
    ?assertEqual(
         <<"select "
           "jsonb_build_object($1,(\"__alias-0\".\"id\" > $2),"
           "$3,\"__alias-0\".\"id\","
           "$4,\"__alias-0\".\"payload\") as \"obj\" "
           "from \"data\" as \"__alias-0\"">>,
         Sql),
    ?assertEqual([<<"flag">>, 10, <<"id">>, <<"raw">>], Args).

to_sql(QAst) ->
    {Sql, Args} = qast:to_sql(QAst),
    Type = maps:get(type, qast:opts(QAst), undefined),
    {Sql, Args, Type}.
