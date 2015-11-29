-module(qsql).

-include("query.hrl").

-export([
         select/1
        ]).

-record(state, {
            aliases=#{}, tables_cnt=0,
            args=[], args_cnt=0
       }).

select(List) when is_list(List) ->
    select(q:pipe(List));
select(#query{tables=Tables, filter=Filter, select=Fields, joins=Joins}) when Tables =/= [] ->
    State = #state{},
    {FieldsSql, State2} = fields_sql(Fields, State),
    {WhereSql, State3} = to_sql(Filter, State2),
    {TablesSql0, State4} = lists:mapfoldl(
        fun({T, TRef}, St) ->
            {TAlias, St2} = get_table_alias(TRef, St),
            Sql = [wrap(T), " as ", wrap(TAlias)],
            {Sql, St2}
        end, State3, Tables),
    TablesSql =  join(TablesSql0, ","),

    {JoinsSql, State5} = lists:mapfoldl(
        fun({inner, {Table, TRef}, Exp}, St) ->
            {TAlias, St2} = get_table_alias(TRef, St),
            {Sql, St3} = to_sql(Exp, St2),
            {[" join ", wrap(Table),  " as ", wrap(TAlias), " on ", Sql], St3}
        end, State4, Joins),
    {iolist_to_binary([
        "select ",
        FieldsSql,
        " from ",
        TablesSql,
        JoinsSql,
        case WhereSql of
            [] -> [];
            _ -> [" where ", WhereSql]
        end
    ]), lists:reverse(State5#state.args), constructor(Fields)}.

fields_sql(Fields, State) ->
    {FieldsSql, State2} =
        lists:mapfoldl(fun to_sql/2, State, maps:values(Fields)),
    {join(FieldsSql, ","), State2}.

join([], _Sep) -> [];
join([H|T], Sep) -> [H|[[Sep,E]||E<-T]].

constructor(Fields) ->
    FieldsNames = maps:keys(Fields),
    fun(TupleData) ->
        maps:from_list(lists:zip(FieldsNames, tuple_to_list(TupleData)))
    end.

to_sql(undefined, State) -> {[], State};
to_sql(Ast, State) ->
    traverse(
        fun({'$value', V}, #state{args=Vs, args_cnt=Cnt}=St) ->
               NewCnt = Cnt+1,
               {index(NewCnt), St#state{args=[V|Vs], args_cnt=NewCnt}};
           ({'$field', TRef, V}, St)->
               {TAlias, St2} = get_table_alias(TRef, St),
               {field_name(TAlias, V), St2};
           ({'$raw', V}, St) ->
               {V, St}
        end, State, Ast).

traverse(F, Acc, {T, List}) when T =:= '$exp' ->
    lists:mapfoldl(fun(E, A) -> traverse(F, A, E) end, Acc, List);
traverse(F, Acc, {'$raw', _}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$field', _F, _V}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$value', _V}=Item) ->
    F(Item, Acc);
%% Other is value
traverse(F, Acc, V) ->
    F({'$value', V}, Acc).

index(N) ->
    [ $$, integer_to_binary(N) ].

table_alias(Int) ->
    lists:concat(["__table-",Int]).

field_name(Talias, Fieldname) ->
    [wrap(Talias), $., wrap(atom_to_list(Fieldname))].

get_table_alias(TRef, #state{aliases=As, tables_cnt=Cnt}=St) ->
    case maps:find(TRef, As) of
        {ok, TAlias} -> {TAlias, St};
        error ->
            TAlias = table_alias(Cnt),
            As2 = maps:put(TRef, TAlias, As),
            {TAlias, St#state{aliases=As2, tables_cnt=Cnt+1}}
    end.

wrap(F) ->
    ["\"", F, "\""].
