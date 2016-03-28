-module(qsql).

-include("query.hrl").

-export([
         select/1,
         insert/1,
         update/1,
         delete/1
        ]).

select(#query{
            tables=Tables,
            where=Where,
            select=RFields,
            joins=Joins,
            group_by=GroupBy,
            order_by=OrderBy,
            limit=Limit,
            offset=Offset
         }) ->
    case is_map(RFields) of
        true ->
            RFieldsList = maps:to_list(RFields),
            Opts = fields_opts(RFieldsList),
            Values = maps:values(RFields);
        false ->
            Opts = qast:opts(RFields),
            Values = [RFields]
    end,
    qast:exp([
        qast:raw("select "),
        fields_exp(Values),
        tables_exp(Tables),
        joins_exp(Joins),
        where_exp(Where),
        group_by_exp(GroupBy),
        order_by_exp(OrderBy),
        limit_exp(Limit),
        offset_exp(Offset)
    ], #{type => Opts});
select(List) when is_list(List) ->
    select(q:pipe(List)).

insert(#query{tables=[{Table, _TRef}], select=RFields, set=Set}) ->
    {SetKeys, SetValues} = lists:unzip([
        {{K, qast:opts(V)}, V} || {K, V} <- maps:to_list(Set)
    ]),
    RFieldsList = maps:to_list(RFields),
    qast:exp([
        qast:raw(["insert into ", equery_utils:wrap(Table), "("]),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], Opts) || {F, Opts} <- SetKeys
        ]),
        qast:raw([") values ("]),
        fields_exp(SetValues),
        qast:raw([")"]),
        returning_exp(RFieldsList)
    ], #{type => fields_opts(RFieldsList)}).

update(List) when is_list(List) ->
    update(q:pipe(List));
update(#query{tables=[{Table, TRef}], select=RFields, where=Where, set=Set}) ->
    RFieldsList = maps:to_list(RFields),
    qast:exp([
        qast:raw(["update ", equery_utils:wrap(Table), " as "]),
        qast:table(TRef),
        qast:raw(" set "),
        qast:join([
            qast:exp([
                qast:raw([equery_utils:field_name(F), " = "]), Node
            ]) || {F, Node} <- maps:to_list(Set)
        ], qast:raw(",")),
        where_exp(Where),
        returning_exp(RFieldsList)
     ], #{type => fields_opts(RFieldsList)}).

delete(List) when is_list(List) ->
    delete(q:pipe(List));
delete(#query{tables=[{Table, TRef}], select=RFields, where=Where}) ->
    RFieldsList = maps:to_list(RFields),
    qast:exp([
        qast:raw(["delete from ", equery_utils:wrap(Table), " as "]),
        qast:table(TRef),
        where_exp(Where),
        returning_exp(RFieldsList)
    ], #{type => fields_opts(RFieldsList)}).

%% =============================================================================
%% Internal functions
%% =============================================================================

%% = Exp builders ==============================================================

fields_exp(FieldsExps) ->
    qast:join(FieldsExps, qast:raw(",")).

returning_exp([]) -> qast:raw([]);
returning_exp(Fields) ->
    qast:exp([
        qast:raw(" returning "),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], Opts)
            || {F, Opts} <- Fields
        ])
    ]).

tables_exp([]) -> qast:raw("");
tables_exp(Tables) ->
    qast:exp([
        qast:raw(" from "),
        qast:join([
            qast:exp([qast:raw([equery_utils:wrap(Table), " as "]), qast:table(TRef)])
            || {Table, TRef} <- Tables
        ], qast:raw(","))
    ]).

joins_exp(Joins) ->
    qast:exp(lists:map(
        fun({inner, JoinAst, Exp}) ->
            qast:exp([
                qast:raw([" join "]),
                JoinAst,
                qast:raw(" on "),
                Exp
            ])
        end, lists:reverse(Joins))).

where_exp(undefined) -> qast:raw([]);
where_exp(WhereExp) -> qast:exp([qast:raw(" where "), WhereExp]).

group_by_exp([]) -> qast:raw([]);
group_by_exp(GroupBy) ->
    qast:exp([
        qast:raw(" group by "),
        qast:join(GroupBy, qast:raw(","))
    ]).

order_by_exp([]) -> qast:raw([]);
order_by_exp(OrderBy) ->
    OrderExps = lists:map(fun({OrderField,Direction}) ->
        qast:exp([
            OrderField,
            qast:raw(case Direction of
                asc -> <<" ASC">>;
                desc -> <<" DESC">>
            end)
        ])
    end, OrderBy),
    qast:exp([
        qast:raw(" order by "),
        qast:join(OrderExps, qast:raw(","))
    ]).

limit_exp(undefined) -> qast:raw([]);
limit_exp(Limit) ->
    qast:exp([
        qast:raw(" limit "),
        qast:value(Limit, #{type => integer})
    ]).

offset_exp(undefined) -> qast:raw([]);
offset_exp(Offset) ->
    qast:exp([
        qast:raw(" offset "),
        qast:value(Offset, #{type => integer})
    ]).

fields_opts(FieldsList) ->
    [{F, qast:opts(Node)} || {F, Node} <- FieldsList].
