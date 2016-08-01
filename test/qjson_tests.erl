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
        "\"__table-0\".\"payload\"->'name' "
        "from \"data\" as \"__table-0\"">>, Sql),
    ?assertEqual([], Args),
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
        "\"__table-0\".\"payload\"->>'name' "
        "from \"data\" as \"__table-0\"">>, Sql),
    ?assertEqual([], Args),
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
        "\"__table-0\".\"payload\"#>'{emails,1}' "
        "from \"data\" as \"__table-0\"">>, Sql),
    ?assertEqual([], Args),
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
        "\"__table-0\".\"payload\"#>>'{emails,1}' "
        "from \"data\" as \"__table-0\"">>, Sql),
    ?assertEqual([], Args),
    ?assertEqual(text, Type).

to_sql(QAst) ->
    {Sql, Args} = qast:to_sql(QAst),
    Type = maps:get(type, qast:opts(QAst), undefined),
    {Sql, Args, Type}.
