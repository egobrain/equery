-module(qsql).

-include("query.hrl").

-export([
         select/1,
         insert/1,
         update/1,
         upsert/1,
         delete/1
        ]).


-spec select(q:query()) -> qast:ast_node().
select(#query{
            tables=Tables,
            schema=Schema,
            with=WithExp,
            where=Where,
            select=RFields,
            joins=Joins,
            group_by=GroupBy,
            order_by=OrderBy,
            limit=Limit,
            offset=Offset
         }) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        maybe_exp(WithExp),
        qast:raw("select "),
        fields_exp(Fields),
        tables_exp(Tables),
        joins_exp(Joins),
        where_exp(Where),
        group_by_exp(GroupBy),
        order_by_exp(OrderBy),
        limit_exp(Limit),
        offset_exp(Offset)
    ], Opts).

-spec insert(q:query()) -> qast:ast_node().
insert(#query{schema=Schema, tables=[{real, Table, TRef}], select=RFields, set=Set}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    {SetKeys, SetValues} = lists:unzip([
        {{K, qast:opts(V)}, V} || {K, V} <- maps:to_list(Set)
    ]),
    qast:exp([
        qast:raw(["insert into ", Table, " as "]),
        qast:table(TRef),
        qast:raw(" ("),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], O) || {F, O} <- SetKeys
        ]),
        qast:raw([") values ("]),
        fields_exp(SetValues),
        qast:raw([")"]),
        returning_exp(Fields)
    ], Opts).

-spec update(q:query()) -> qast:ast_node().
update(#query{schema=Schema, tables=[{real, Table, TRef}], select=RFields, where=Where, set=Set}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        qast:raw(["update ", Table, " as "]),
        qast:table(TRef),
        qast:raw(" set "),
        qast:join([
            qast:exp([
                qast:raw([equery_utils:field_name(F), " = "]), Node
            ]) || {F, Node} <- maps:to_list(Set)
        ], qast:raw(",")),
        where_exp(Where),
        returning_exp(Fields)
     ], Opts).

-spec upsert(q:query()) -> qast:ast_node().
upsert(#query{schema=#{fields := SchemaFields}=Schema, tables=[{real, Table, TRef}], select=RFields, set=Set}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    IndexFields = maps:fold(fun(F, O, Acc) ->
        case maps:get(index, O, false) of
            true -> [{F, O}|Acc];
            false -> Acc
        end
    end, [], SchemaFields),
    {SetKeys, SetValues} = lists:unzip([
        {{K, qast:opts(V)}, V} || {K, V} <- maps:to_list(Set)
    ]),
    qast:exp([
        qast:raw(["insert into ", Table, " as "]),
        qast:table(TRef),
        qast:raw(" ("),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], O) || {F, O} <- SetKeys
        ]),
        qast:raw([") values ("]),
        fields_exp(SetValues),
        qast:raw([") on conflict ("]),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], O) || {F, O} <- IndexFields
        ]),
        qast:raw([") do update set "]),
        fields_exp([
            qast:raw([equery_utils:field_name(F), " = EXCLUDED.", equery_utils:field_name(F)])
            || {F, _Opts} <- SetKeys
        ]),
        returning_exp(Fields)
    ], Opts).

-spec delete(q:query()) -> qast:ast_node().
delete(#query{schema=Schema, tables=[{real, Table, TRef}], select=RFields, where=Where}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        qast:raw(["delete from ", Table, " as "]),
        qast:table(TRef),
        where_exp(Where),
        returning_exp(Fields)
    ], Opts).

%% =============================================================================
%% Internal functions
%% =============================================================================

%% = Exp builders ==============================================================

fields_and_opts(Schema, RFields) ->
    case is_map(RFields) of
        true ->
            RFieldsList = maps:to_list(RFields),
            Opts = #{type => type(Schema, RFieldsList)},
            Values = maps:values(RFields);
        false ->
            Opts = qast:opts(RFields),
            Values = [RFields]
    end,
    {Values, Opts}.

fields_exp(FieldsExps) ->
    qast:join(FieldsExps, qast:raw(",")).

returning_exp([]) -> qast:raw([]);
returning_exp(Fields) ->
    qast:exp([
        qast:raw(" returning "),
        fields_exp(Fields)
    ]).

tables_exp([_|_] = Tables) ->
    qast:exp([
        qast:raw(" from "),
        qast:join(
            lists:map(
                fun ({real, Table, TRef}) ->
                        qast:exp([
                            qast:raw([Table, " as "]),
                            qast:table(TRef)
                        ]);
                    ({alias, TRef}) ->
                        qast:table(TRef)
                end, Tables), qast:raw(","))
    ]).

joins_exp(Joins) ->
    qast:exp(lists:map(
        fun({JoinType, JoinAst, Exp}) ->
            qast:exp([
                qast:raw([" ", join_type(JoinType), " join "]),
                JoinAst,
                qast:raw(" on "),
                Exp
            ])
        end, lists:reverse(Joins))).

join_type(inner) -> <<"inner">>;
join_type(left) -> <<"left">>;
join_type(right) -> <<"right">>;
join_type(full) -> <<"full">>;
join_type({left, outer}) -> <<"left outer">>;
join_type({right, outer}) -> <<"right outer">>;
join_type({full, outer}) -> <<"full outer">>.

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

type(Schema, FieldsList) ->
    Model = maps:get(model, Schema, undefined),
    {model, Model, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]}.

maybe_exp(undefined) -> qast:raw("");
maybe_exp(Exp) -> Exp.
