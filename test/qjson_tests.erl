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
    ?assertEqual(undefined, Type).

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
    ?assertEqual(undefined, Type).

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

to_sql(QAst) ->
    {Sql, Args} = qast:to_sql(QAst),
    Type = maps:get(type, qast:opts(QAst), undefined),
    {Sql, Args, Type}.
