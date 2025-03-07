-module(qsql).

-include("query.hrl").
-include("cth.hrl").

-export([
         select/1,
         insert/1,
         update/1,
         delete/1
        ]).

-spec select(q:query()) -> qast:ast_node().
select(#query{
            tables=Tables,
            schema=Schema,
            with=WithExp,
            where=Where,
            select=RFields,
            distinct=Distinct,
            joins=Joins,
            group_by=GroupBy,
            order_by=OrderBy,
            limit=Limit,
            offset=Offset,
            lock=Lock
         }) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        maybe_exp(WithExp),
        qast:raw("select "),
        distinct_exp(Distinct, RFields),
        fields_exp(Fields),
        from_exp(Tables),
        joins_exp(Joins),
        where_exp(Where),
        group_by_exp(GroupBy),
        order_by_exp(OrderBy),
        limit_exp(Limit),
        offset_exp(Offset),
        lock(Lock)
    ], Opts).

-spec insert(q:query()) -> qast:ast_node().
insert(#query{
            schema=Schema,
            with=WithExp,
            tables=[{real, Table, TRef}|Rest],
            select=RFields,
            set = #query{} = Query,
            on_conflict=OnConflict
    }) ->
    Rest =:= [] orelse error("Unsupported query using operation. See q:using/[1,2]"),
    SelectAst = select(Query),
    #{type := {model, _, SetFields}} = qast:opts(SelectAst),
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        maybe_exp(WithExp),
        qast:raw(["insert into ", Table, " as "]),
        qast:alias(TRef),
        qast:raw(" ("),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], qast:opts(V)) || {F, V} <- SetFields
        ]),
        qast:raw([") "]),
        SelectAst,
        on_conflict_exp(OnConflict),
        returning_exp(Fields)
    ], Opts);
insert(#query{
            schema=Schema,
            tables=[{real, Table, TRef}|Rest],
            select=RFields,
            set=Set,
            on_conflict=OnConflict
    }) ->
    Rest =:= [] orelse error("Unsupported query using operation. See q:using/[1,2]"),
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    {SetKeys, SetValues} = lists:unzip([
        {{K, qast:opts(V)}, V} || {K, V} <- ?MAPS_TO_LIST(Set)
    ]),
    qast:exp([
        qast:raw(["insert into ", Table, " as "]),
        qast:alias(TRef),
        qast:raw(" ("),
        fields_exp([
            qast:exp([qast:raw(equery_utils:field_name(F))], O) || {F, O} <- SetKeys
        ]),
        qast:raw([") values ("]),
        fields_exp(SetValues),
        qast:raw([")"]),
        on_conflict_exp(OnConflict),
        returning_exp(Fields)
    ], Opts).

-spec update(q:query()) -> qast:ast_node().
update(#query{schema=Schema, tables=[{real, Table, TRef}|Rest], select=RFields, where=Where, set=Set}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        qast:raw(["update ", Table, " as "]),
        qast:alias(TRef),
        qast:raw(" set "),
        qast:join([
            qast:exp([
                qast:raw([equery_utils:field_name(F), " = "]), Node
            ]) || {F, Node} <- ?MAPS_TO_LIST(Set)
        ], qast:raw(",")),
        from_exp(Rest),
        where_exp(Where),
        returning_exp(Fields)
     ], Opts).

-spec delete(q:query()) -> qast:ast_node().
delete(#query{schema=Schema, tables=[{real, Table, TRef}|Rest], select=RFields, where=Where}) ->
    {Fields, Opts} = fields_and_opts(Schema, RFields),
    qast:exp([
        qast:raw(["delete from ", Table, " as "]),
        qast:alias(TRef),
        using_exp(Rest),
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
            RFieldsList = ?MAPS_TO_LIST(RFields),
            Opts = #{type => type(Schema, RFieldsList)},
            Values = lists:map(fun({F, Ast}) ->
                as(Ast, F)
            end, RFieldsList);
        false ->
            Opts = qast:opts(RFields),
            Values = [RFields]
    end,
    {Values, Opts}.

as(Value, Name) ->
    qast:exp([
        Value, qast:raw(" as "), qast:raw(equery_utils:field_name(Name))
    ], qast:opts(Value)).

fields_exp(FieldsExps) ->
    qast:join(FieldsExps, qast:raw(",")).

returning_exp([]) -> qast:raw([]);
returning_exp(Fields) ->
    qast:exp([
        qast:raw(" returning "),
        fields_exp(Fields)
    ]).

using_exp([]) -> qast:raw([]);
using_exp([_|_]=Tables) ->
    qast:exp([qast:raw(" using "), tables_exp_(Tables)]).

from_exp([]) -> qast:raw([]);
from_exp([_|_] = Tables) ->
    qast:exp([qast:raw(" from "), tables_exp_(Tables)]).

tables_exp_([_|_]=Tables) ->
    qast:join(lists:map(fun table_exp/1, Tables), qast:raw(",")).

table_exp({real, Table, TRef}) ->
    qast:exp([
        qast:raw([Table, " as "]),
        qast:alias(TRef)
    ]);
table_exp({alias, AliasExp, _FeildsExp}) -> AliasExp.

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

distinct_exp(undefined, _RFields) -> qast:raw("");
distinct_exp(all, _RFields) -> qast:raw("distinct ");
distinct_exp([], RFields) -> distinct_exp(undefined, RFields);
distinct_exp(DistinctOn, RFields) when is_list(DistinctOn), is_map(RFields) ->
    DistinctAliases = lists:map(fun(Field) ->
        FieldAst = maps:get(Field, RFields),
        qast:raw(equery_utils:field_name(Field), qast:opts(FieldAst))
    end, DistinctOn),
    qast:exp([
        qast:raw("distinct on ("),
        qast:join(DistinctAliases, qast:raw(",")),
        qast:raw(") ")
    ]).

on_conflict_exp(Conflicts) ->
    lists:foldr(fun({ConflictTarget, ConflictAction}, Acc) ->
        qast:exp([
            Acc,
            qast:raw(" on conflict"),
            conflict_target_exp(ConflictTarget),
            qast:raw("do "),
            conflict_action_exp(ConflictAction)
        ])
    end, qast:raw(""), ?MAPS_TO_LIST(Conflicts)).

conflict_target_exp(any) -> qast:raw(" ");
conflict_target_exp(Columns) when is_list(Columns) ->
    qast:exp([
        qast:raw(" ("),
        qast:join([
            qast:raw(equery_utils:field_name(C)) || C <- Columns
        ], qast:raw(",")),
        qast:raw(") ")
    ]).

conflict_action_exp(nothing) -> qast:raw("nothing");
conflict_action_exp(Set) when is_map(Set) ->
    qast:exp([
        qast:raw("update set "),
        qast:join([
            qast:exp([
                qast:raw([equery_utils:field_name(F), " = "]), Node
            ]) || {F, Node} <- ?MAPS_TO_LIST(Set)
        ], qast:raw(","))
    ]).

lock(undefined) ->
    qast:raw("");
lock({RowLockLevel, Tables}) ->
    Aliases = lists:map(fun({real, _Table, TRef}) -> qast:alias(TRef) end, Tables),
    qast:exp([
        qast:raw(" "),
        case RowLockLevel of
            for_update -> qast:raw("for update");
            for_no_key_update -> qast:raw("for no key update");
            for_share -> qast:raw("for share");
            for_key_share -> qast:raw("for key share")
        end,
        qast:raw(" of "),
        qast:join(Aliases, qast:raw(","))
    ]).
