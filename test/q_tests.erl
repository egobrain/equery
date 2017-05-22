-module(q_tests).

-export([schema/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("equery/include/equery.hrl").

-define(USER_SCHEMA, #{
        fields => #{
            id => #{ type => integer, index => true, autoincrement => true },
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

-define(TREE_FIELDS, maps:get(fields, tree_m:schema())).
-define(TREE_FIELDS_LIST, maps:to_list(?TREE_FIELDS)).

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
                    pg_sql:'=:='(Name, <<"test1">>)
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    pg_sql:'=:='(UserId, AuthorId)
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    pg_sql:'=:='(Name, <<"test2">>)
                end),
            q:order_by(
                fun([#{name := Name, id := Id}|_]) ->
                    [{Name, asc}, {Id, desc}]
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\" "
           "from \"users\" as \"__alias-0\" "
           "inner join \"comments\" as \"__alias-1\" "
           "on (\"__alias-0\".\"id\" = \"__alias-1\".\"author\") "
           "where ((\"__alias-0\".\"name\" = $1) and (\"__alias-1\".\"text\" = $2)) "
           "order by \"__alias-0\".\"name\" ASC,\"__alias-0\".\"id\" DESC">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual({model, undefined, ?USER_FIELDS_LIST}, Feilds).

q_compile_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:data(
                fun([#{name := Name}=TD]) ->
                    [TD#{filter => Name =:= <<"test1">>}]
                end),
            q:where(
                fun([#{name := Name, filter := F}]) ->
                    Name =:= <<"test2">> orelse F
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    UserId =:= AuthorId
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    Name =:= <<"test2">>
                end),
            q:select(
                fun([#{id := Id}=U|_]) ->
                    U#{'_id_gt' => Id > 3}
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "(\"__alias-0\".\"id\" > $1),"
           "(\"__alias-0\".\"name\" = $2),"
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\" "
           "from \"users\" as \"__alias-0\" "
           "inner join \"comments\" as \"__alias-1\" on "
           "(\"__alias-0\".\"id\" = \"__alias-1\".\"author\") where "
           "(((\"__alias-0\".\"name\" = $3) or "
           "(\"__alias-0\".\"name\" = $4)) and "
           "(\"__alias-1\".\"text\" = $5))">>,
         Sql),
    ?assertEqual([3,<<"test1">>,<<"test2">>,<<"test1">>,<<"test2">>], Args),
    ?assertEqual({model, ?MODULE, [
        {'_id_gt',#{type => boolean}},
        {filter,#{type => boolean}}
        | ?USER_FIELDS_LIST
    ]}, Feilds).

q_insert_test() ->
    {Sql, _Args, ReturningFields} = to_sql(
        qsql:insert(q:set(fun(_) -> ?USER_FIELDS_WITHOUT([id]) end, q:from(?MODULE)))),
    ?assertEqual(
        <<"insert into \"users\" as \"__alias-0\" (\"name\",\"password\",\"salt\") "
          "values ($1,$2,$3) "
          "returning "
          "\"__alias-0\".\"id\","
          "\"__alias-0\".\"name\","
          "\"__alias-0\".\"password\","
          "\"__alias-0\".\"salt\"">>,
        Sql),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

q_upsert_test() ->
    {Sql, _Args, ReturningFields} = to_sql(
        qsql:upsert(q:set(fun(_) -> ?USER_FIELDS end, q:from(?MODULE)))),
    ?assertEqual(
        <<"insert into \"users\" as \"__alias-0\" (\"id\",\"name\",\"password\",\"salt\") "
          "values ($1,$2,$3,$4) "
          "on conflict (\"id\") do update set "
          "\"id\" = EXCLUDED.\"id\","
          "\"name\" = EXCLUDED.\"name\","
          "\"password\" = EXCLUDED.\"password\","
          "\"salt\" = EXCLUDED.\"salt\" "
          "returning "
          "\"__alias-0\".\"id\","
          "\"__alias-0\".\"name\","
          "\"__alias-0\".\"password\","
          "\"__alias-0\".\"salt\"">>,
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
         <<"update \"users\" as \"__alias-0\" set "
           "\"name\" = $1,"
           "\"password\" = $2 "
           "where (\"__alias-0\".\"id\" = $3)">>,
        Sql),
    ?assertEqual([<<"Sam">>, <<"pass">>, 3], Args),
    ?assertEqual({model, ?MODULE, []}, ReturningFields).

q_update_using_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:update(q:pipe(q:from(?MODULE), [
            q:set(fun(_) -> #{name => <<"Sam">>} end),
            q:set(fun(Set, _) -> Set#{password => <<"pass">>} end),
            q:where(fun([#{id := Id}]) -> Id =:= 3 end),
            q:using(?COMMENT_SCHEMA),
            q:select(fun(_) -> #{} end) %% return nothing
        ]))),
    ?assertEqual(
         <<"update \"users\" as \"__alias-0\" set "
           "\"name\" = $1,"
           "\"password\" = $2 "
           "from \"comments\" as \"__alias-1\" "
           "where (\"__alias-0\".\"id\" = $3)">>,
        Sql),
    ?assertEqual([<<"Sam">>, <<"pass">>, 3], Args),
    ?assertEqual({model, ?MODULE, []}, ReturningFields).

q_delete_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:delete(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id1}]) -> Id1 =:= 3 end)
        ]))),
    ?assertEqual(
         <<"delete from \"users\" as \"__alias-0\" "
           "where (\"__alias-0\".\"id\" = $1) "
           "returning "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\"">>,
        Sql),
    ?assertEqual([3], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

q_delete_using_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:delete(q:pipe(q:from(?MODULE), [
            q:using(?MODULE),
            q:where(fun([#{id := Id1}, #{id := Id2}]) ->
                Id1 =:= Id2 andalso Id1 =:= 3
            end)
        ]))),
    ?assertEqual(
         <<"delete from \"users\" as \"__alias-0\" "
           "using \"users\" as \"__alias-1\" "
           "where ((\"__alias-0\".\"id\" = \"__alias-1\".\"id\") and (\"__alias-0\".\"id\" = $1)) "
           "returning "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\"">>,
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
         <<"select $1 from \"users\" as \"__alias-0\"">>,
         Sql),
    ?assertEqual([1], Args),
    ?assertEqual(undefined, ReturningFields).


q_group_by_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(
                fun([#{name := Name}]) ->
                    pg_sql:'=:='(Name, <<"test1">>)
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    pg_sql:'=:='(UserId, AuthorId)
                end),
            q:where(
                fun([_,#{text := Name}]) ->
                    pg_sql:'=:='(Name, <<"test2">>)
                end),
            q:group_by(
                fun([_, #{author := AuthorId}]) ->
                    [AuthorId]
                end),
            q:select(
                fun([#{id := Id}|_]) ->
                    #{cnt => pg_sql:count(Id)}
                end)
        ]))),
    ?assertEqual(
         <<"select "
           "count(\"__alias-0\".\"id\") "
           "from \"users\" as \"__alias-0\" "
           "inner join \"comments\" as \"__alias-1\" "
           "on (\"__alias-0\".\"id\" = \"__alias-1\".\"author\") "
           "where ((\"__alias-0\".\"name\" = $1) and (\"__alias-1\".\"text\" = $2)) "
           "group by \"__alias-1\".\"author\"">>,
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
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\" "
           "from \"users\" as \"__alias-0\" "
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
                        #{author => Author, comments_cnt => pg_sql:count(Author) }
                    end)
                ]),
                fun([#{id := Id}, #{author := AuthorId}]) -> Id =:= AuthorId end),
            q:select(fun([#{name := Name}, #{comments_cnt := Cnt}]) ->
                #{name => Name, comments_cnt => Cnt}
            end)
        ]))),
    ?assertEqual(
         <<"select "
               "\"__alias-0\".\"__field-2\","
               "\"__alias-1\".\"name\" "
           "from \"users\" as \"__alias-1\" "
           "inner join ("
               "select "
                   "\"__alias-2\".\"author\" as \"__field-1\","
                   "count(\"__alias-2\".\"author\") as \"__field-2\" "
               "from \"comments\" as \"__alias-2\" "
               "group by \"__alias-2\".\"author\""
               ") as \"__alias-0\" "
           "on (\"__alias-1\".\"id\" = \"__alias-0\".\"__field-1\") "
           "where (\"__alias-1\".\"name\" = $1)">>,
         Sql),
    ?assertEqual([<<"user">>], Args),
    ?assertEqual({model, ?MODULE, [{comments_cnt, #{type => integer}}|?USER_FIELDS_LIST([name])]}, Feilds).

single_item_select_test() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:select(fun([#{id := Id}]) -> Id end)
        ]))),
    ?assertEqual(
         <<"select \"__alias-0\".\"id\" from \"users\" as \"__alias-0\"">>,
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
         <<"select \"__alias-0\".\"id\",\"__alias-0\".\"name\" "
           "from \"users\" as \"__alias-0\"">>,
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
               not Id * 1 + 2 - 3 / 4 orelse
               pg_sql:in(Id, [8,9,10]) orelse
               pg_sql:like(Id, <<"11%">>) orelse
               pg_sql:ilike(Id, <<"11%">>) orelse
               pg_sql:'~'(Id, <<"a">>) orelse
               pg_sql:'~*'(Id, <<"A">>)
            end),
            q:select(fun([T]) -> maps:with([name], T) end)
        ]))),
    ?assertEqual(
            <<"select \"__alias-0\".\"name\" from \"users\" as \"__alias-0\" where "
              "(((\"__alias-0\".\"id\" < $1) and "
              "(\"__alias-0\".\"id\" <= $2)) or "
              "(((\"__alias-0\".\"id\" > $3) and "
              "(\"__alias-0\".\"id\" >= $4)) or "
              "((not (\"__alias-0\".\"id\" = $5) and "
              "(((not \"__alias-0\".\"id\" * $6) + $7) - ($8 / $9))) or "
              "(\"__alias-0\".\"id\" = ANY($10) or "
              "(\"__alias-0\".\"id\" like $11 or "
              "(\"__alias-0\".\"id\" ilike $12 or "
              "((\"__alias-0\".\"id\" ~ $13) or "
              "(\"__alias-0\".\"id\" ~* $14))))))))">>,
         Sql),
    ?assertEqual([3,4,5,6,7,1,2,3,4,[8,9,10],<<"11%">>,<<"11%">>,<<"a">>,<<"A">>], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST([name])}, Feilds).

distinct_operation_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:group_by(fun([#{name := Name}]) -> [Name] end),
            q:select(fun([#{id := Id}]) ->
                pg_sql:distinct(Id)
            end)
        ]))),
    ?assertEqual(
            <<"select distinct (\"__alias-0\".\"id\") from \"users\" as \"__alias-0\" "
              "group by \"__alias-0\".\"name\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual(integer, Feilds).

array_agg_operation_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:group_by(fun([#{name := Name}]) -> [Name] end),
            q:select(fun([#{id := Id}]) ->
                pg_sql:array_agg(Id)
            end)
        ]))),
    ?assertEqual(
            <<"select array_agg(\"__alias-0\".\"id\") from \"users\" as \"__alias-0\" "
              "group by \"__alias-0\".\"name\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual({array, integer}, Feilds).

andalso_op_test() ->
    Node = qast:raw("a"),
    ?assertEqual(Node, pg_sql:'andalso'(true, Node)),
    ?assertEqual(Node, pg_sql:'andalso'(Node, true)),
    ?assertEqual(false, pg_sql:'andalso'(false, Node)),
    ?assertEqual(false, pg_sql:'andalso'(Node, false)),
    ?assertEqual(
         {<<"(a and a)">>, []},
         qast:to_sql(pg_sql:'andalso'(Node, Node))).

orelse_op_test() ->
    Node = qast:raw("b"),
    ?assertEqual(true, pg_sql:'orelse'(true, Node)),
    ?assertEqual(true, pg_sql:'orelse'(Node, true)),
    ?assertEqual(Node, pg_sql:'orelse'(false, Node)),
    ?assertEqual(Node, pg_sql:'orelse'(Node, false)),
    ?assertEqual(
         {<<"(b or b)">>, []},
         qast:to_sql(pg_sql:'orelse'(Node, Node))).

not_op_test() ->
    Node = qast:raw("c"),
    ?assertEqual(false, pg_sql:'not'(true)),
    ?assertEqual(true, pg_sql:'not'(false)),
    ?assertEqual(
         {<<"not c">>, []},
         qast:to_sql(pg_sql:'not'(Node))).

is_null_test() ->
    Node = qast:raw("d"),
    ?assertEqual(
         {<<"d is null">>, []},
         qast:to_sql(pg_sql:'is_null'(Node))).

coalesce_test() ->
    Node1 = qast:raw("e"),
    Node2 = qast:raw("f"),
    Node3 = qast:raw("g"),
    ?assertEqual(
         {<<"coalesce(e,f,g)">>, []},
         qast:to_sql(pg_sql:'coalesce'([Node1,Node2,Node3]))).

aggs_test_() ->
    Tests = [
        {fun pg_sql:max/1, "max"},
        {fun pg_sql:min/1, "min"},
        {fun pg_sql:count/1, "count"},
        {fun pg_sql:sum/1, "sum"}
    ],
    [{R, fun() ->
        ?assertEqual(
            {iolist_to_binary(
             ["select ",R,"(\"__alias-0\".\"id\") from \"users\" as \"__alias-0\""]),
             []},
            qast:to_sql(
                qsql:select(q:pipe(q:from(?USER_SCHEMA), [
                    q:select(fun([#{id := Id}]) -> F(Id) end)
                ]))))
    end} || {F, R} <- Tests].

abs_test() ->
    Node = qast:raw("h"),
    ?assertEqual(
         {<<"abs(h)">>, []},
         qast:to_sql(pg_sql:'abs'(Node))).

ops_test_() ->
    Tests = [
        {fun pg_sql:max/2, "GREATEST"},
        {fun pg_sql:min/2, "LEAST"}
    ],
    [{R, fun() ->
        ?assertEqual(
            {iolist_to_binary(
             ["select ",R,"(\"__alias-0\".\"id\",$1) from \"users\" as \"__alias-0\""]),
             [3]},
            qast:to_sql(
                qsql:select(q:pipe(q:from(?USER_SCHEMA), [
                    q:select(fun([#{id := Id}]) -> F(Id, 3) end)
                ]))))
    end} || {F, R} <- Tests].

row_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:select(fun([Tab]) -> pg_sql:row(Tab) end)
        ]))),
    ?assertEqual(
         <<"select row("
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\""
           ") from \"users\" as \"__alias-0\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual({record, {model, undefined, ?USER_FIELDS_LIST}}, ReturningFields).

in_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id}]) ->
                pg_sql:in(Id, q:pipe(q:from(?MODULE), [
                    q:select(fun([#{id := IId}]) -> pg_sql:max(IId) end)
                ]))
            end),
            q:select(fun([T]) -> maps:with([name], T) end)
        ]))),
    ?assertEqual(
            <<"select \"__alias-0\".\"name\" from \"users\" as \"__alias-0\" where "
              "\"__alias-0\".\"id\" in ("
                  "select max(\"__alias-1\".\"id\") from \"users\" as \"__alias-1\""
              ")">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST([name])}, Feilds).

in_query_test() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id}]) -> pg_sql:in(Id, [1,2]) end)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\""
           " from \"users\" as \"__alias-0\""
           " where \"__alias-0\".\"id\" = ANY($1)">>,
         Sql),
    ?assertEqual([[1,2]], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

'@>_test'() ->
    {Sql, Args, ReturningFields} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:where(fun([#{id := Id}]) -> pg_sql:'@>'(Id, [1,2]) end)
        ]))),
    ?assertEqual(
         <<"select "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\""
           " from \"users\" as \"__alias-0\""
           " where \"__alias-0\".\"id\" @> $1">>,
         Sql),
    ?assertEqual([[1,2]], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, ReturningFields).

pt_test() ->
    Path = "./test/test_m.tpl",
    {ok, Bin} = file:read_file(Path),
    {ok, M, ModuleBin} = compile_module_str(binary_to_list(Bin)),
    {module, M} = code:load_binary(M, "", ModuleBin),
    Q = q:from(M),
    ?assertEqual(
         {<<"select \"__alias-0\".\"id\" from \"test\" as \"__alias-0\" where "
            "(\"__alias-0\".\"id\" > $1)">>,
          [3]},
         qast:to_sql(qsql:select(M:filter(3, Q)))),
    ?assertEqual(
         {<<"select \"__alias-0\".\"id\" from \"test\" as \"__alias-0\" where "
            "(\"__alias-0\".\"id\" = $1)">>,
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
        pg_sql:'=:='(UserId, AuthorId)
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
                 <<"select \"__alias-0\".\"id\",\"__alias-1\".\"id\" "
                   "from \"users\" as \"__alias-1\" ",
                   Exp/binary, " join \"comments\" as \"__alias-0\" on "
                   "(\"__alias-1\".\"id\" = \"__alias-0\".\"author\")">>, Sql)
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


as_test() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:select(fun([#{id := Id}]) -> pg_sql:as(Id, text) end)
        ]))),
    ?assertEqual(
         <<"select (\"__alias-0\".\"id\")::text from \"users\" as \"__alias-0\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual(text, Type).

set_type_test() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(q:pipe(q:from(?USER_SCHEMA), [
            q:select(fun([#{id := Id}]) -> pg_sql:set_type(Id, text) end)
        ]))),
    ?assertEqual(
         <<"select \"__alias-0\".\"id\" from \"users\" as \"__alias-0\"">>,
         Sql),
    ?assertEqual([], Args),
    ?assertEqual(text, Type).

recursive_test() ->
    {Sql, Args, Type} = to_sql(
        qsql:select(
            q:recursive(
                q:where(fun([#{id := Id}]) -> Id =:= 1 end, q:from(tree_m)),
                fun(Q) ->
                    q:select(fun([_, T]) -> T end,
                        (q:join(tree_m, fun([#{id := Id}, #{parentId := PId}]) -> Id =:= PId end))(Q))
                end))),
    ?assertEqual(
         <<"with recursive \"__alias-0\" as ("
               "select \"__alias-1\".\"id\","
                      "\"__alias-1\".\"parentId\","
                      "\"__alias-1\".\"value\" "
               "from \"tree\" as \"__alias-1\" where (\"__alias-1\".\"id\" = $1) "
               "union all "
               "select \"__alias-2\".\"id\","
                      "\"__alias-2\".\"parentId\","
                      "\"__alias-2\".\"value\" "
               "from \"__alias-0\" "
               "inner join \"tree\" as \"__alias-2\" "
               "on (\"__alias-0\".\"id\" = \"__alias-2\".\"parentId\")"
           ") select \"__alias-0\".\"id\",\"__alias-0\".\"parentId\",\"__alias-0\".\"value\" from \"__alias-0\"">>,
         Sql),
    ?assertEqual([1], Args),
    ?assertEqual({model, tree_m, ?TREE_FIELDS_LIST}, Type).

distinct_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:distinct()
        ]))),
    ?assertEqual(
         <<"select distinct "
           "\"__alias-0\".\"id\","
           "\"__alias-0\".\"name\","
           "\"__alias-0\".\"password\","
           "\"__alias-0\".\"salt\" "
           "from \"users\" as \"__alias-0\"">>, Sql),
    ?assertEqual([], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, Feilds).

distinct_on_test() ->
    {Sql, Args, Feilds} = to_sql(
        qsql:select(q:pipe(q:from(?MODULE), [
            q:distinct_on(fun(_) -> [id, name] end)
        ]))),
    ?assertEqual(
         <<"select distinct on (\"__alias-0\",\"__alias-1\") "
           "\"__alias-2\".\"id\" as \"__alias-0\","
           "\"__alias-2\".\"name\" as \"__alias-1\","
           "\"__alias-2\".\"password\","
           "\"__alias-2\".\"salt\" "
           "from \"users\" as \"__alias-2\"">>, Sql),
    ?assertEqual([], Args),
    ?assertEqual({model, ?MODULE, ?USER_FIELDS_LIST}, Feilds).

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
