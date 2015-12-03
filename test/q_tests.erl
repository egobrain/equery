-module(q_tests).

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, equery}).

-define(USER_SCHEMA, #{
        fields => #{
            id => #{ type => serial },
            name => #{type => {varchar, 60}, required => true},
            password => #{type => {varchar, 60}, required => true},
            salt => #{type => required}
        },
        table => <<"users">>
    }).

-define(COMMENT_SCHEMA, #{
        fields => #{
            id => #{type => serial},
            author => #{type => integer},
            text => #{type => text}
        },
        table => <<"comments">>
    }).

q_test() ->
    {Sql, Args, Constructor} =
        qsql:select([
            q:from(?USER_SCHEMA),
            q:filter(
                fun([#{name := Name}]) ->
                    qast:'=:='(Name, <<"test1">>)
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    qast:'=:='(UserId, AuthorId)
                end),
            q:filter(
                fun([_,#{text := Name}]) ->
                    qast:'=:='(Name, <<"test2">>)
                end)
        ]),
    ?assertEqual(
         <<"select "
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\" "
           "from \"users\" as \"__table-0\" "
           "join \"comments\" as \"__table-1\" "
           "on (\"__table-0\".\"id\" = \"__table-1\".\"author\") "
           "where ((\"__table-0\".\"name\" = $1) and (\"__table-1\".\"text\" = $2))">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual(#{id=>1, name=>2, password=>3, salt=>4}, Constructor({1,2,3,4})).

q_compile_test() ->
    {Sql, Args, Constructor} =
        qsql:select([
            q:from(?USER_SCHEMA),
            q:filter(
                fun([#{name := Name}]) ->
                    Name =:= <<"test1">>
                end),
            q:join(?COMMENT_SCHEMA,
                fun([#{id := UserId}, #{author := AuthorId}]) ->
                    UserId =:= AuthorId
                end),
            q:filter(
                fun([_,#{text := Name}]) ->
                    Name =:= <<"test2">>
                end)
        ]),
    ?assertEqual(
         <<"select "
           "\"__table-0\".\"id\","
           "\"__table-0\".\"name\","
           "\"__table-0\".\"password\","
           "\"__table-0\".\"salt\" "
           "from \"users\" as \"__table-0\" "
           "join \"comments\" as \"__table-1\" "
           "on (\"__table-0\".\"id\" = \"__table-1\".\"author\") "
           "where ((\"__table-0\".\"name\" = $1) and (\"__table-1\".\"text\" = $2))">>,
         Sql),
    ?assertEqual([<<"test1">>, <<"test2">>], Args),
    ?assertEqual(#{id=>1, name=>2, password=>3, salt=>4}, Constructor({1,2,3,4})).

    %% Userschema = ,
    %% CommentsSchema =
    %% },
    %% Q = q:pipe(
    %%         [
    %%          q:from(UserSchema),
    %%          q:filter(fun([#{name := Name}]) -> Name =:= <<"user">> end),
    %%          q:join(
    %%              q:pipe(
    %%                  [
    %%                   q:from(CommentsSchema),
    %%                   q:group_by(fun([#{author := Author}]) -> [Author] end),
    %%                   q:select(
    %%                       fun([#{author := Author}]) ->
    %%                           #{author => Author, comments_cnt => q:count(Author) }
    %%                       end)
    %%                  ]),
    %%              fun([#{id := Id}, #{author := Id}]) -> true end
    %%          ),
    %%          q:select(
    %%              fun([#{name := N}, #{comments_cnt := Cnt}]) ->
    %%                  #{name => Name, comments_cnt => q:count(P)}
    %%              end)
    %%         ]),
    %% pg_repo:select(Q).
