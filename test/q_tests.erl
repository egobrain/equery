-module(q_tests).

-export([schema/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("equery/include/equery.hrl").

-define(USER_SCHEMA, #{
        fields => #{
            id => #{ type => serial },
            name => #{type => {varchar, 60}, required => true},
            password => #{type => {varchar, 60}, required => true},
            salt => #{type => {varchar, 24}, required => true}
        },
        table => <<"users">>,
        links => #{
            comments => {has_many, ?COMMENT_SCHEMA, #{id => author}}
        }
    }).
-define(USER_FIELDS, maps:get(fields, ?USER_SCHEMA)).
-define(USER_FIELDS(L), maps:with(L, ?USER_FIELDS)).
-define(USER_FIELDS_WITHOUT(L), maps:without(L, ?USER_FIELDS)).
-define(USER_FIELDS_LIST, maps:to_list(?USER_FIELDS)).
-define(USER_FIELDS_LIST(L), maps:to_list(?USER_FIELDS(L))).
-define(USER_FIELDS_LIST_WITHOUT(L), maps:to_list(maps:without(L, ?USER_FIELDS))).

-define(COMMENT_SCHEMA, #{
        fields => #{
            id => #{type => serial},
            author => #{type => integer},
            text => #{type => text}
        },
        table => <<"comments">>,
        links => #{
           author => {belongs_to, ?MODULE, #{author => id}}
        }
    }).

-define(COMMENT_FIELDS, maps:get(fields, ?COMMENT_SCHEMA)).
-define(COMMENT_FIELDS_LIST, maps:to_list(?COMMENT_FIELDS)).
-define(COMMENT_FIELDS_LIST_WITHOUT(L), maps:to_list(maps:without(L, ?COMMENT_FIELDS))).

schema() -> ?USER_SCHEMA.

%% =============================================================================
%% tests
%% =============================================================================

schema_test() ->
    ?assertEqual(?USER_SCHEMA, q:get(schema, q:from(?USER_SCHEMA))),
    ?assertEqual(?USER_SCHEMA#{model => ?MODULE}, q:get(schema, q:from(?MODULE))).

data_test() ->
    ?assertEqual(
        maps:keys(?USER_FIELDS),
        maps:keys(hd(q:get(data, q:from(?USER_SCHEMA))))),
    ?assertEqual(
        maps:keys(?USER_FIELDS),
        maps:keys(hd(q:get(data, q:from(?MODULE))))).

q_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:where(
                fun([#{name := Name}]) ->
                    pg:'=:='(Name, <<"test1">>)
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    pg:'=:='(UserId, AuthorId)
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    pg:'=:='(Name, <<"test2">>)
                end),
            q:order_by(
                fun([#{name := Name, id := Id}|_]) ->
                    [{Name, asc}, {Id, desc}]
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\" "
           "from \"users\" as \"__table-0\" "
           "inner join \"comments\" as \"__table-1\" "
           "on (\"__table-0\".\"id\" = \"__table-1\".\"author\") "
           "where ((\"__table-0\".\"name\" = $1) and (\"__table-1\".\"text\" = $2)) "
           "order by \"__table-0\".\"name\" ASC,\"__table-0\".\"id\" DESC">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual({model, undefined, ?USER_FIELDS_LIST}, Feilds).

q_compile_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(
                fun([#{name := Name}]) ->
                    Name =:= <<"test1">>
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    UserId =:= AuthorId
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    Name =:= <<"test2">>
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\" "
           "from \"users\" as \"__table-0\" "
           "inner join \"comments\" as \"__table-1\" "
           "on (\"__table-0\".\"id\" = \"__table-1\".\"author\") "
           "where ((\"__table-0\".\"name\" = $1) and (\"__table-1\".\"text\" = $2))">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, Feilds).

q_insert_test() ->
    {Sql, _Args, ReturningFields} = to_sql(
        qsql:insert(q:set(fun(_) -> ?USER_FIELDS_WITHOUT([id]) end, q:from(?MODULE)))),
    ?assertEqual(
        <<"insert into \"users\"(\"name\",\"password\",\"salt\") "
          "values ($1,$2,$3) "
          "returning \"id\",\"name\",\"password\",\"salt\"">>,
        Sql),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

q_update_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:update(q:pipe(q:from(?MODULE), [
            q:set(fun(_) -> #{name => <<"Sam">>} end),
            q:set(fun(Set, _) -> Set#{password => <<"pass">>} end),
            q:where(fun([#{id := Id}]) -> Id =:= 3 end),
            q:select(fun(_) -> #{} end) %% return nothing
        ]))),
    ?assertEqual(
         <<"update \"users\" as \"__table-0\" set "
           "\"name\" = $1,"
           "\"password\" = $2 "
           "where (\"__table-0\".\"id\" = $3)">>,
        Sql),
    ?assertEqual([<<"Sam">>, <<"pass">>, 3], Args),
    ?assertEqual({model, ?MODULE, []}, ReturningFields).

q_delete_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:delete(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id}]) -> Id =:= 3 end)
        ]))),
    ?assertEqual(
         <<"delete from \"users\" as \"__table-0\" "
           "where (\"__table-0\".\"id\" = $1) "
           "returning \"id\",\"name\",\"password\",\"salt\"">>,
        Sql),
    ?assertEqual([3], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

q_data_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:data(fun([Tab]) -> [Tab#{f => 1}] end),
            q:select(fun([#{f := F}]) -> F end)
        ]))),
    ?assertEqual(
         <<"select $1 from \"users\" as \"__table-0\"">>,
         Sql),
    ?assertEqual([1], Args),
    ?assertEqual(undefined, ReturningFields).


q_group_by_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(
                fun([#{name := Name}]) ->
                    pg:'=:='(Name, <<"test1">>)
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    pg:'=:='(UserId, AuthorId)
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    pg:'=:='(Name, <<"test2">>)
                end),
            q:group_by(
                fun([_, #{author := AuthorId}]) ->
                    [AuthorId]
                end),
            q:select(
                fun([#{id := Id}|_]) ->
                    #{cnt => pg:count(Id)}
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "count(\"__table-0\".\"id\") "
           "from \"users\" as \"__table-0\" "
           "inner join \"comments\" as \"__table-1\" "
           "on (\"__table-0\".\"id\" = \"__table-1\".\"author\") "
           "where ((\"__table-0\".\"name\" = $1) and (\"__table-1\".\"text\" = $2)) "
           "group by \"__table-1\".\"author\"">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual({model, ?MODULE, [{cnt, #{type => integer}}]}, Feilds).

limit_offset_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:limit(10),
            q:offset(3)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\" "
           "from \"users\" as \"__table-0\" "
           "limit $1 "
           "offset $2">>, Sql),
    ?assertEqual([10, 3], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, Feilds).

complex_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(fun([#{name := Name}]) -> Name =:= <<"user">> end),
            q:join(
                q:pipe(q:from(?COMMENT_SCHEMA), [
                    q:group_by(fun([#{author := Author}]) -> [Author] end),
                    q:select(fun([#{author := Author}]) ->
                        #{author => Author, comments_cnt => pg:count(Author) }
                    end)
                ]),
                fun([#{id := Id}, #{author := AuthorId}]) -> Id =:= AuthorId end),
            q:select(fun([#{name := Name}, #{comments_cnt := Cnt}]) ->
                #{name => Name, comments_cnt => Cnt}
            end)
        ]))),
    ?assertEqual(
         <<"select "
               "count(\"__table-0\".\"author\"),"
               "\"__table-1\".\"name\" "
           "from \"users\" as \"__table-1\" "
           "inner join ("
               "select "
                   "\"__table-0\".\"author\","
                   "count(\"__table-0\".\"author\") "
               "from \"comments\" as \"__table-0\" "
               "group by \"__table-0\".\"author\""
               ") as \"__table-2\" "
           "on (\"__table-1\".\"id\" = \"__table-0\".\"author\") "
           "where (\"__table-1\".\"name\" = $1)">>,
         Sql),
    ?assertEqual([<<"user">>], Args),
    ?assertEqual({model, ?MODULE, [{comments_cnt, #{type => integer}}|?USER_FIELDS_LIST([name])]}, Feilds).

single_item_select_test() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:select(fun([#{id := Id}]) -> Id end)
        ]))),
    ?assertEqual(
         <<"select \"__table-0\".\"id\" from \"users\" as \"__table-0\"">>,
         Sql),
    ?assertEqual([], Args),
    [{id, #{type := RType}}] = ?USER_FIELDS_LIST([id]),
    ?assertEqual(RType, Type).

update_select_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(fun([#{id := Id}]) -> #{id => Id} end),
            q:select(fun(S, [#{name := Name}]) -> S#{name => Name} end)
        ]))),
    ?assertEqual(
         <<"select \"__table-0\".\"id\",\"__table-0\".\"name\" "
           "from \"users\" as \"__table-0\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST([id,name])}, Feilds).

operators_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id}]) ->
               Id < 3 andalso
               Id =< 4 orelse
               Id > 5 andalso
               Id >= 6 orelse
               Id =/= 7 andalso
               not Id * 1 + 2 - 3 / 4
            end),
            q:select(fun([T]) -> maps:with([name], T) end)
        ]))),
    ?assertEqual(
            <<"select \"__table-0\".\"name\" from \"users\" as \"__table-0\" where "
              "(((\"__table-0\".\"id\" < $1) and "
              "(\"__table-0\".\"id\" <= $2)) or "
              "(((\"__table-0\".\"id\" > $3) and "
              "(\"__table-0\".\"id\" >= $4)) or "
              "(not (\"__table-0\".\"id\" = $5) and "
              "(((not \"__table-0\".\"id\" * $6) + $7) - ($8 / $9)))))">>,
         Sql),
    ?assertEqual([3,4,5,6,7,1,2,3,4], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST([name])}, Feilds).

andalso_op_test() ->
    Node = qast:raw("a"),
    ?assertEqual(Node, pg:'andalso'(true, Node)),
    ?assertEqual(Node, pg:'andalso'(Node, true)),
    ?assertEqual(false, pg:'andalso'(false, Node)),
    ?assertEqual(false, pg:'andalso'(Node, false)),
    ?assertEqual(
         {<<"(a and a)">>, []},
         qast:to_sql(pg:'andalso'(Node, Node))).

orelse_op_test() ->
    Node = qast:raw("b"),
    ?assertEqual(true, pg:'orelse'(true, Node)),
    ?assertEqual(true, pg:'orelse'(Node, true)),
    ?assertEqual(Node, pg:'orelse'(false, Node)),
    ?assertEqual(Node, pg:'orelse'(Node, false)),
    ?assertEqual(
         {<<"(b or b)">>, []},
         qast:to_sql(pg:'orelse'(Node, Node))).

not_op_test() ->
    Node = qast:raw("c"),
    ?assertEqual(false, pg:'not'(true)),
    ?assertEqual(true, pg:'not'(false)),
    ?assertEqual(
         {<<"not c">>, []},
         qast:to_sql(pg:'not'(Node))).

aggs_test_() ->
    Tests = [
        {fun pg:max/1, "max"},
        {fun pg:min/1, "min"},
        {fun pg:count/1, "count"},
        {fun pg:sum/1, "sum"}
    ],
    [{R, fun() ->
        ?assertEqual(
            {iolist_to_binary(
             ["select ",R,"(\"__table-0\".\"id\") from \"users\" as \"__table-0\""]),
             []},
            qast:to_sql(
                qsql:select(q:pipe(q:from(?USER_SCHEMA), [
                    q:select(fun([#{id := Id}]) -> F(Id) end)
                ]))))
    end} || {F, R} <- Tests].

ops_test_() ->
    Tests = [
        {fun pg:max/2, "GREATEST"},
        {fun pg:min/2, "LEAST"}
    ],
    [{R, fun() ->
        ?assertEqual(
            {iolist_to_binary(
             ["select ",R,"(\"__table-0\".\"id\",$1) from \"users\" as \"__table-0\""]),
             [3]},
            qast:to_sql(
                qsql:select(q:pipe(q:from(?USER_SCHEMA), [
                    q:select(fun([#{id := Id}]) -> F(Id, 3) end)
                ]))))
    end} || {F, R} <- Tests].

row_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(fun([Tab]) -> pg:row(Tab) end)
        ]))),
    ?assertEqual(
         <<"select row("
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\""
           ") from \"users\" as \"__table-0\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual({record, {model, undefined, ?USER_FIELDS_LIST}}, ReturningFields).

pt_test() ->
    Path = "./test/test_m.tpl",
    {ok, Bin} = file:read_file(Path),
    {ok, M, ModuleBin} = compile_module_str(binary_to_list(Bin)),
    {module, M} = code:load_binary(M, "", ModuleBin),
    Q = q:from(M),
    ?assertEqual(
         {<<"select \"__table-0\".\"id\" from \"test\" as \"__table-0\" where "
            "(\"__table-0\".\"id\" > $1)">>,
          [3]},
         qast:to_sql(qsql:select(M:filter(3, Q)))),
    ?assertEqual(
         {<<"select \"__table-0\".\"id\" from \"test\" as \"__table-0\" where "
            "(\"__table-0\".\"id\" = $1)">>,
          [3]},
         qast:to_sql(qsql:select(M:filter(3, q:data(fun(D) -> D ++ D end, Q))))).

transform_fun_test() ->
    FunS = "fun(A) -> not (A =:= 2) end.",
    Fun = compile_fun_str(FunS),
    false = Fun(2),
    true = Fun(3),
    TFun = equery_pt:transform_fun(Fun),
    {Sql, Args} = qast:to_sql(TFun(3)),
    ?assertEqual(<<"not ($1 = $2)">>, Sql),
    ?assertEqual([3,2], Args).

join_type_test_() ->
    Q = q:from(?USER_SCHEMA),
    JFun = fun([#{id := UserId}, #{author := AuthorId}]) ->
        pg:'=:='(UserId, AuthorId)
    end,

    lists:map(fun({T, Exp}) ->
        F = fun() ->
            {Sql, []} = qast:to_sql(qsql:select(q:pipe(Q, [
                q:join(T, ?COMMENT_SCHEMA, JFun),
                q:select(fun([#{id := UId}, #{id := CId}]) ->
                   #{uid => UId, cid => CId}
                end)
            ]))),
            ?assertEqual(
                 <<"select \"__table-0\".\"id\",\"__table-1\".\"id\" "
                   "from \"users\" as \"__table-1\" ",
                   Exp/binary, " join \"comments\" as \"__table-0\" on "
                   "(\"__table-1\".\"id\" = \"__table-0\".\"author\")">>, Sql)
         end,
         {Exp, F}
    end,
    [
     {inner, <<"inner">>},
     {left, <<"left">>},
     {right, <<"right">>},
     {full, <<"full">>},
     {{left, outer}, <<"left outer">>},
     {{right, outer}, <<"right outer">>},
     {{full, outer}, <<"full outer">>}
    ]).


%% =============================================================================
%% Internal functions
%% =============================================================================

to_sql(QAst) ->
    {Sql, Args} = qast:to_sql(QAst),
    Type = maps:get(type, qast:opts(QAst), undefined),
    {Sql, Args, Type}.

compile_fun_str(FunS) ->
    {ok, Tokens, _} = erl_scan:string(FunS),
    {ok, Exprs} = erl_parse:parse_exprs(Tokens),
    {value, Fun, _Bindings} = erl_eval:exprs(Exprs, []),
    Fun.

compile_module_str(ModuleS) ->
    {ok, Tokens, _} = erl_scan:string(ModuleS),
    Splited = split_dot(Tokens),
    Forms = lists:map(fun(Ts) ->
        {ok, Form} = erl_parse:parse_form(Ts),
        Form
    end, Splited),
    Forms2 = equery_pt:parse_transform(Forms, []),
    compile:forms(Forms2).

split_dot(Tokens) ->
    split_dot(Tokens, [], []).
split_dot([{dot, _}=C|Rest], A, R) ->
    split_dot(Rest, [], [lists:reverse([C|A])|R]);
split_dot([C|Rest], A, R) ->
    split_dot(Rest, [C|A], R);
split_dot([], _, R) ->
    lists:reverse(R).
