-module(q).
-moduledoc """
Query building DSL.

`q` is the user-facing composable query builder. Each builder takes either
a query and returns a query (eager form, arity N+1), or returns a partially
applied function `qfun()` (lazy form, arity N) that can be chained with
`pipe/2`.

```erlang
q:pipe(q:from(?USER), [
    q:where(fun([#{active := A}]) -> A =:= true end),
    q:order_by(fun([#{created := C}]) -> [{C, desc}] end),
    q:limit(10)
]).
```

Builders fall into three categories:

- **Source**: `from/1`, `using/1,2`, `with/2,3`, `recursive/2` — define
  where data comes from.
- **Refinement**: `where/1,2`, `having/1,2`, `select/1,2`, `set/1,2`,
  `data/1,2`, `group_by/1,2`, `order_by/1,2`, `limit/1,2`, `offset/1,2`,
  `distinct/0,1`, `distinct_on/1,2` — narrow or shape results.
- **Joins / locks**: `join/2,3,4`, `lateral_join/2,3,4`,
  `lock/1,2,3,4`, `for_update/0,1`.

Statement compilation (`SELECT` / `INSERT` / `UPDATE` / `DELETE`) lives
in `qsql`. SQL expression builders (operators, scalar/aggregate functions,
`CASE`, type casts) live in `pg_sql`. JSON operators and builders live in
`qjson`.
""".

-include("query.hrl").
-include("ast_helpers.hrl").

-export([
         pipe/2,

         get/2,
         lookup_tables/2
        ]).

-export([
         from/1,
         using/1, using/2,
         with/2, with/3,
         recursive/2,
         join/2, join/3, join/4,
         lateral_join/2, lateral_join/3, lateral_join/4,
         where/1, where/2,
         select/1, select/2,
         set/1, set/2,
         data/1, data/2,
         group_by/1, group_by/2,
         having/1, having/2,
         on_conflict/2, on_conflict/3,
         on_conflict_where/3, on_conflict_where/4,
         order_by/1, order_by/2,
         limit/1, limit/2,
         offset/1, offset/2,

         lock/1, lock/2, lock/3, lock/4,
         for_update/0, for_update/1,

         distinct/0, distinct/1,
         distinct_on/1, distinct_on/2
        ]).

-export([
         compile/1
        ]).

-type model() :: schema() | module().
-type query() :: #query{}.
-type table() :: {alias, qast:ast_node(), #{atom() => term()}}.
-type schema() :: #{fields => #{atom() => #{atom() => term()}}, table => binary(), atom() => any()}.
-type data() :: [#{atom() => qast:ast_node()}].
-type select() :: #{atom() => qast:ast_node()} | qast:ast_node().
-type set() :: #{atom() => qast:ast_node()} | query().
-type order_nulls() :: nulls_first | nulls_last.
-type order_item() :: {qast:ast_node(), asc | desc}
                    | {qast:ast_node(), asc | desc, order_nulls()}.
-type order() :: [order_item()].
-type distinct() :: all | [atom()].
-type join_type() :: inner | left | right | full | {left, outer} | {right, outer} | {full, outer}.
-type row_lock_level() :: for_update | for_no_key_update | for_share | for_key_share.
-type wait_policy() :: wait | nowait | skip_locked.
-type qfun() :: fun((query()) -> query()).
-type conflict_columns() :: [atom()].
-type conflict_target() :: any | conflict_columns().
-type conflict_update() :: #{atom() => qast:ast_node()}.
-type conflict_action() :: nothing | conflict_update() | {conflict_update(), qast:ast_node()}.

%% internal
-type table_id() :: binary() | {binary(), binary()}.
-type real_table() :: {real, table_id(), reference()}.
-type stored_conflict_target() :: conflict_target() | {conflict_columns(), qast:ast_node()}.

-export_type([query/0]).

-export_type([
         model/0,
         table/0,
         real_table/0,
         schema/0,
         data/0,
         select/0,
         set/0,
         order/0,
         distinct/0,
         join_type/0,
         row_lock_level/0,
         wait_policy/0,
         qfun/0,
         conflict_columns/0,
         conflict_target/0,
         stored_conflict_target/0,
         conflict_update/0,
         conflict_action/0
        ]).

%% = Flow ======================================================================

-doc(#{group => <<"Composition">>}).
-doc """
Apply a chain of partially-applied builders to a base query.

```erlang
q:pipe(q:from(?USER), [
    q:where(fun([#{active := A}]) -> A =:= true end),
    q:limit(10)
]).
```
""".
-spec pipe(Q, [qfun()]) -> Q when Q :: query().
pipe(Query, Funs) ->
    lists:foldl(fun(F, Q) -> F(Q) end, Query, Funs).

-doc(#{group => <<"Composition">>}).
-doc """
Introspect a built query.

- `get(schema, Q)` — the schema map.
- `get(data, Q)` — the list of field maps (one per joined source), as
  fed to DSL closures.
""".
-spec get(schema, query()) -> schema();
         (data, query()) -> data().
get(schema, #query{schema=Schema}) -> Schema;
get(data, #query{data=Data}) -> Data.

%% = Query builders ============================================================

-doc(#{group => <<"Source">>}).
-doc """
Start a query from a source.

Accepts:
- a schema map `#{table => ..., fields => ..., schema => ...}` —
  schema is optional, when present emits `"schema"."table"`;
- a model module that exports `schema/0`;
- another `query()` — wraps it as a subquery in `FROM`;
- a table alias from `with/3`;
- an arbitrary AST node with `{model, M, FieldsList}` opts.
""".
-spec from(model() | query() | table() | qast:ast_node()) -> query().
from(Info) when is_map(Info); is_atom(Info) ->
    Schema = get_schema(Info),
    {RealTable, Fields} = table_feilds(Schema),
    #query{
        schema = Schema,
        data=[Fields],
        select=Fields,
        tables=[RealTable]
     };
from(#query{}=Query) ->
    from(braced(qsql:select(Query)));
from({alias, _AliasExp, FieldsExp}=Alias) ->
    Fields = maps:map(
        fun(_N, Ast) -> qast:opts(Ast) end,
        FieldsExp),
    #query{
        schema = #{
            fields => Fields
        },
        tables = [Alias],
        select = FieldsExp,
        data = [FieldsExp]
    };
from(Ast) ->
    #{type := {model, Model, FieldsList}} = qast:opts(Ast),
    TRef = make_ref(),
    Fields = maps:from_list(FieldsList),
    FieldsExp = aliased_fields(TRef, Fields),
    TableAst = as(Ast, qast:alias(TRef)),
    #query{
        schema = #{
            model => Model,
            fields => Fields
        },
        data = [FieldsExp],
        select = FieldsExp,
        tables = [{alias, TableAst, FieldsExp}]
    }.

as(VAst, AsAst) ->
    qast:exp([
        VAst, qast:raw(" as "), AsAst
    ], qast:opts(VAst)).

-doc(#{group => <<"Source">>}).
-doc """
Add an additional source to `FROM` (comma form).

Used to bring extra tables into scope for `UPDATE ... FROM ...`,
`DELETE ... USING ...`, or for SELECTs joining via subsequent
`where/1,2` predicates.
""".
using(Info) -> fun(Q) -> using(Info, Q) end.
using({alias, _AliasExp, FieldsExp}=Alias, #query{tables=[_|_]=Tables, data=Data}=Query) ->
    Query#query{
        tables = Tables ++ [Alias],
        data = Data ++ [FieldsExp]
    };
using(Info, #query{tables=[_|_]=Tables, data=Data}=Query) when is_map(Info); is_atom(Info) ->
    Schema = get_schema(Info),
    {RealTable, Fields} = table_feilds(Schema),
    Query#query{
        tables = Tables ++ [RealTable],
        data = Data ++ [Fields]
     };
using(Ast, #query{tables=[_|_]=Tables, data=Data}=Query) ->
    #{type := {model, _Model, FieldsList}} = qast:opts(Ast),
    TRef = make_ref(),
    Fields = maps:from_list(FieldsList),
    FieldsExp = aliased_fields(TRef, Fields),
    TableAst = as(braced(Ast), qast:alias(TRef)),
    Query#query{
        tables = Tables ++ [{alias, TableAst, FieldsExp}],
        data = Data ++ [FieldsExp]
    }.

table_feilds(#{table := _Table}=Schema) ->
    SchemaFields = maps:get(fields, Schema, #{}),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts) end,
        SchemaFields),
    RealTable = {real, schema_table_id(Schema), TRef},
    {RealTable, Fields}.

schema_table_id(#{schema := S, table := T}) -> {S, T};
schema_table_id(#{table := T}) -> T.


%% = Recursive =================================================================

-doc(#{group => <<"Source">>}).
-doc """
Build a recursive CTE.

`BaseQuery` is the anchor; `UnionFun` receives the CTE reference and
returns the recursive query body. Emits `WITH RECURSIVE name AS
(anchor UNION ALL recursive) SELECT ...`.
""".
recursive(#query{select=RFields}=BaseQuery, UnionFun) when is_map(RFields) ->
    Schema = ?MODULE:get(schema, BaseQuery),
    TRef = make_ref(),
    Fields = maps:map(
        fun(_N, Ast) -> qast:opts(Ast) end,
        RFields),
    FieldsExp = maps:map(
        fun(N, Opts) -> qast:field(TRef, N, Opts)
    end, Fields),
    InternalQ = #query{
        schema = (maps:with([model], Schema))#{
            fields => Fields
        },
        data = [FieldsExp],
        select = FieldsExp,
        tables = [{alias, qast:alias(TRef), FieldsExp}]
    },
    WithExpression = qast:exp([
        qast:raw("with recursive "),
        qast:alias(TRef),
        qast:raw(" as ("),
        qsql:select(BaseQuery),
        qast:raw(" union all "),
        qsql:select(call(UnionFun, [InternalQ])),
        qast:raw(") ")
    ]),
    InternalQ#query{with=WithExpression}.

-doc(#{group => <<"Source">>}).
-doc """
`WITH` clause (CTE).

`Fun` receives the CTE's table reference and returns a `qfun()` that
uses it. The CTE source may be a model, a query, or arbitrary AST
with `{model, ...}` opts (e.g. an `UPDATE ... RETURNING ...` AST).
""".
-spec with(model() | query() | qast:ast_node(), fun((table()) -> qfun())) -> qfun().
with(Info, Fun) -> fun(Q) -> with(Info, Fun, Q) end.

-spec with(model() | query() | qast:ast_node(), fun((table()) -> qfun()), Q) -> Q when Q :: query().
with(Info, Fun, Q) when is_map(Info); is_atom(Info) ->
    with(from(Info), Fun, Q);
with(#query{}=Query, Fun, Q) ->
    with(qsql:select(Query), Fun, Q);
with(Ast, Fun, Q) ->
    #{type := {model, _Model, Fields}} = Opts = qast:opts(Ast),
    TRef = make_ref(),
    FieldsExp = lists:foldl(fun({N, O}, Acc) ->
        Acc#{N => qast:field(TRef, N, O)}
    end, #{}, Fields),
    Alias = qast:alias(TRef, Opts),
    WithExpression = qast:exp([
        qast:raw("with "),
        Alias,
        qast:raw(" as ("), Ast, qast:raw(") ")
    ]),
    (call(Fun, [{alias, Alias, FieldsExp}]))(Q#query{with=WithExpression}).

-doc(#{group => <<"Joins">>}).
-doc """
Inner join — short form of `join(inner, Info, Fun)`.

`Fun` receives the cumulative data (one map per joined source) and
returns the ON-condition AST.

```erlang
q:join(?POST, fun([#{id := UId}, #{author_id := AId}]) ->
    UId =:= AId
end).
```
""".
-spec join(model() | query() | table(), fun((data()) -> qast:ast_node())) -> qfun().
join(Info, Fun) ->
    join(inner, Info, Fun).

-doc(#{group => <<"Joins">>}).
-doc """
Join with explicit type.

`JoinType` is one of `inner`, `left`, `right`, `full`, `{left, outer}`,
`{right, outer}`, `{full, outer}`.
""".
-spec join(join_type(), model() | query() | table(), fun((data()) -> qast:ast_node())) -> qfun().
join(JoinType, Info, Fun) ->
    fun(Q) -> join(JoinType, Info, Fun, Q) end.

-spec join(join_type(), model() | query() | table(), fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
join(JoinType, #query{select=RFields}=JoinQ, Fun, #query{data=Data, joins=Joins}=Q) ->
    TRef = make_ref(),
    Fields = maps:map(fun(_, V) -> qast:opts(V) end, RFields),
    FieldsData = aliased_fields(TRef, Fields),
    NewData = Data ++ [FieldsData],
    JoinAst = qast:exp([
        qast:raw("("),
        qsql:select(JoinQ),
        qast:raw(") as "),
        qast:alias(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(Fun, [NewData])}|Joins]
    };
join(JoinType, {alias, TableAlias, FieldsExp}, Fun, #query{data=Data, joins=Joins}=Q) ->
    NewData = Data ++ [FieldsExp],
    Q#query{
        data=NewData,
        joins=[{JoinType, TableAlias, call(Fun, [NewData])}|Joins]
    };
join(JoinType, Info, Fun, #query{data=Data, joins=Joins}=Q) ->
    JoinSchema = get_schema(Info),
    SchemaFields = maps:get(fields, JoinSchema, #{}),
    TableId = schema_table_id(JoinSchema),
    TRef = make_ref(),
    Fields = maps:map(
        fun(N, O) -> qast:field(TRef, N, O) end,
        SchemaFields),
    NewData = Data ++ [Fields],
    JoinAst = qast:exp([
        qast:raw([equery_utils:wrap_table(TableId), " as "]),
        qast:alias(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(Fun, [NewData])}|Joins]
    }.

-doc(#{group => <<"Joins">>}).
-doc """
LATERAL join — subquery may reference outer columns. `ON` defaults to `true`.

`QFun` receives the outer `data()` and returns the subquery (which can
reference outer fields captured from the closure):

```erlang
q:lateral_join(left, fun([#{id := UId}]) ->
    q:pipe(q:from(?POST), [
        q:where(fun([#{author_id := A}]) -> A =:= UId end),
        q:limit(3)
    ])
end).
```
""".
-spec lateral_join(join_type(), fun((data()) -> query())) -> qfun().
lateral_join(JoinType, QFun) ->
    lateral_join(JoinType, QFun, fun(_) -> qast:raw(<<"true">>) end).

-doc(#{group => <<"Joins">>}).
-doc """
LATERAL join with custom `ON` condition.
""".
-spec lateral_join(join_type(), fun((data()) -> query()), fun((data()) -> qast:ast_node())) -> qfun().
lateral_join(JoinType, QFun, CondFun) ->
    fun(Q) -> lateral_join(JoinType, QFun, CondFun, Q) end.

-spec lateral_join(join_type(), fun((data()) -> query()), fun((data()) -> qast:ast_node()), Q) -> Q
    when Q :: query().
lateral_join(JoinType, QFun, CondFun, #query{data=Data, joins=Joins}=Q) ->
    #query{select=RFields}=JoinQ = call(QFun, [Data]),
    TRef = make_ref(),
    Fields = maps:map(fun(_, V) -> qast:opts(V) end, RFields),
    FieldsData = aliased_fields(TRef, Fields),
    NewData = Data ++ [FieldsData],
    JoinAst = qast:exp([
        qast:raw("lateral ("),
        qsql:select(JoinQ),
        qast:raw(") as "),
        qast:alias(TRef)
    ]),
    Q#query{
        data=NewData,
        joins=[{JoinType, JoinAst, call(CondFun, [NewData])}|Joins]
    }.

-doc(#{group => <<"Refinement">>}).
-doc """
Add a `WHERE` predicate.

Multiple `where/1,2` calls compose with `andalso`. The closure receives
the list of field maps (one per joined source) and returns an AST node
(boolean expression).

```erlang
q:where(fun([#{name := N}]) -> N =:= <<"alice">> end).
```
""".
-spec where(fun((data()) -> qast:ast_node())) -> qfun().
where(Fun) -> fun(Q) -> where(Fun, Q) end.

-spec where(fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
where(Fun, #query{data=Data, where=OldWhere}=Q) ->
    Where = call(Fun, [Data]),
    NewWhere =
        case OldWhere of
            undefined -> Where;
            _ -> pg_sql:'andalso'(OldWhere, Where)
        end,
    Q#query{where = NewWhere}.


-doc(#{group => <<"Refinement">>}).
-doc """
Project the columns.

Closure either takes only `data()` and returns the new selection map,
or takes the previous `select` map and `data()` to update it
incrementally:

```erlang
q:select(fun([#{id := Id, name := N}]) -> #{id => Id, name => N} end).
q:select(fun(S, [#{age := A}]) -> S#{age => A} end).
```

Selection may also be a single AST node (returns a scalar column).
""".
-spec select(Fun) -> qfun() when
      Fun :: fun((data()) -> select()) |
             fun((select(), data()) -> select()).
select(Fun) -> fun(Q) -> select(Fun, Q) end.

-spec select(Fun, Q) -> Q when
      Fun :: fun((data()) -> select()) |
             fun((select(), data()) -> select()),
      Q :: query().
select(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Q#query{select=call(Fun, [Data])};
select(Fun, #query{select=PrevSelect, data=Data}=Q) when is_function(Fun, 2) ->
    Q#query{select=call(Fun, [PrevSelect, Data])}.


-doc(#{group => <<"Refinement">>}).
-doc """
Set values for `INSERT` / `UPDATE`.

Accepts either a map `#{field => ast_or_value}` for direct VALUES /
SET clause, or a `query()` for `INSERT ... SELECT ...` form.

```erlang
q:set(fun(_) -> #{name => <<"alice">>, active => true} end).
```
""".
-spec set(Fun) -> qfun() when
      Fun :: fun((data()) -> set()) |
             fun((set(), data()) -> set()).
set(Fun) -> fun(Q) -> set(Fun, Q) end.

-spec set(Fun, Q) -> Q when
      Fun :: fun((data()) -> set()) |
             fun((set(), data()) -> set()),
      Q :: query().
set(Fun, #query{data=Data}=Q) when is_function(Fun, 1) ->
    Set = call(Fun, [Data]),
    check_set(Set),
    Q#query{set=Set};
set(Fun, #query{set=PrevSet, data=Data}=Q) when is_function(Fun, 2) ->
    Set = call(Fun, [PrevSet, Data]),
    check_set(Set),
    Q#query{set=Set}.

check_set(#query{}) -> ok;
check_set(Map) when is_map(Map) -> ok;
check_set(_) -> error(bad_set).

-doc(#{group => <<"Refinement">>}).
-doc """
`GROUP BY` clause. Closure returns a list of expressions to group by.
""".
-spec group_by(fun((data()) -> qast:ast_node())) -> qfun().
group_by(Fun) -> fun(Q) -> group_by(Fun, Q) end.

-spec group_by(fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
group_by(Fun, #query{data=Data}=Q) ->
    Q#query{group_by=call(Fun, [Data])}.

-doc(#{group => <<"Refinement">>}).
-doc """
`HAVING` clause — predicate over aggregated rows.

Multiple `having/1,2` calls compose with `andalso`.

```erlang
q:pipe(Q, [
    q:group_by(fun([#{name := N}]) -> [N] end),
    q:having(fun([#{id := Id}]) -> pg_sql:count(Id) > 1 end)
]).
```
""".
-spec having(fun((data()) -> qast:ast_node())) -> qfun().
having(Fun) -> fun(Q) -> having(Fun, Q) end.

-spec having(fun((data()) -> qast:ast_node()), Q) -> Q when Q :: query().
having(Fun, #query{data=Data, having=OldHaving}=Q) ->
    Having = call(Fun, [Data]),
    NewHaving =
        case OldHaving of
            undefined -> Having;
            _ -> pg_sql:'andalso'(OldHaving, Having)
        end,
    Q#query{having = NewHaving}.

-doc(#{group => <<"Upsert">>}).
-doc """
`ON CONFLICT (target) DO ...` clause.

`ConflictTarget` is either `any` (no target, matches any constraint
violation) or a list of column atoms.

`Fun` receives `data() ++ [Excluded]` — the additional `Excluded` map
provides access to the proposed-but-conflicted row values. Returns
either `nothing` (DO NOTHING), an update map (DO UPDATE SET), or
`{UpdateMap, Cond}` (DO UPDATE SET ... WHERE Cond).

```erlang
q:on_conflict([id], fun([_, Excluded]) -> Excluded end).
q:on_conflict(any, fun(_) -> nothing end).
```
""".
-spec on_conflict(conflict_target(), fun((data()) -> conflict_action())) -> qfun().
on_conflict(ConflictTarget, Fun) -> fun(Q) -> on_conflict(ConflictTarget, Fun, Q) end.

-spec on_conflict(conflict_target(), fun((data()) -> conflict_action()), Q) -> Q when Q :: query().
on_conflict(ConflictTarget, Fun, #query{on_conflict=OnConflict, data=Data}=Q) ->
    Schema = get(schema, Q),
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = qast:raw("EXCLUDED"),
    Fields = maps:map(fun(N, Opts) ->
        qast:exp([Table, qast:raw([".", equery_utils:field_name(N)])], Opts)
    end, SchemaFields),
    Q#query{on_conflict=maps:put(ConflictTarget, call(Fun, [Data ++ [Fields]]), OnConflict)}.

-doc(#{group => <<"Upsert">>}).
-doc """
`ON CONFLICT (target) WHERE filter DO ...` — partial-index upsert.

`Filter` is a predicate on the row that scopes the target index.
""".
-spec on_conflict_where(conflict_columns(), fun((data()) -> qast:ast_node()),
                        fun((data()) -> conflict_action())) -> qfun().
on_conflict_where(Columns, Filter, Fun) ->
    fun(Q) -> on_conflict_where(Columns, Filter, Fun, Q) end.

-spec on_conflict_where(conflict_columns(), fun((data()) -> qast:ast_node()),
                        fun((data()) -> conflict_action()), Q) -> Q when Q :: query().
on_conflict_where(Columns, Filter, Fun, #query{on_conflict=OnConflict, data=Data}=Q) ->
    Schema = get(schema, Q),
    SchemaFields = maps:get(fields, Schema, #{}),
    Table = qast:raw("EXCLUDED"),
    Fields = maps:map(fun(N, Opts) ->
        qast:exp([Table, qast:raw([".", equery_utils:field_name(N)])], Opts)
    end, SchemaFields),
    Target = {Columns, call(Filter, [Data])},
    Q#query{on_conflict=maps:put(Target, call(Fun, [Data ++ [Fields]]), OnConflict)}.

-doc(#{group => <<"Refinement">>}).
-doc """
`ORDER BY` clause.

Closure returns a list of `order_item()`:
- `{Field, asc | desc}` — direction only.
- `{Field, asc | desc, nulls_first | nulls_last}` — with NULL placement.

```erlang
q:order_by(fun([#{name := N, id := Id}]) ->
    [{N, asc, nulls_last}, {Id, desc}]
end).
```
""".
-spec order_by(fun((data()) -> order())) -> qfun().
order_by(Fun) -> fun(Q) -> order_by(Fun, Q) end.

-spec order_by(fun((data()) -> order()), Q) -> Q when Q :: query().
order_by(Fun, #query{data=Data}=Q) ->
    Q#query{order_by=call(Fun, [Data])}.

-doc(#{group => <<"Refinement">>}).
-doc """
`LIMIT n` — parameterized.
""".
-spec limit(non_neg_integer()) -> qfun().
limit(Value) -> fun(Q) -> limit(Value, Q) end.

-spec limit(non_neg_integer(), Q) -> Q when Q :: query().
limit(Value, Q) ->
    Q#query{limit=Value}.

-doc(#{group => <<"Refinement">>}).
-doc """
`OFFSET n` — parameterized.
""".
-spec offset(non_neg_integer()) -> qfun().
offset(Value) -> fun(Q) -> offset(Value, Q) end.

-spec offset(non_neg_integer(), Q) -> Q when Q :: query().
offset(Value, Q) ->
    Q#query{offset=Value}.

-doc(#{group => <<"Locking">>}).
-doc """
Row-level lock.

`RowLockLevel` ∈ `for_update | for_no_key_update | for_share | for_key_share`.
Defaults to `wait` policy and locks all real tables in the query.
""".
-spec lock(row_lock_level()) -> qfun().
lock(RowLockLevel) ->
    lock(RowLockLevel, wait).

-doc(#{group => <<"Locking">>}).
-doc """
Row-level lock with explicit wait policy.

`WaitPolicy` ∈ `wait | nowait | skip_locked`.
""".
-spec lock(row_lock_level(), wait_policy()) -> qfun().
lock(RowLockLevel, WaitPolicy) ->
    lock(RowLockLevel, WaitPolicy, fun(RealTables) -> RealTables end).

-doc(#{group => <<"Locking">>}).
-doc """
Lock selected tables only. `Fun` filters the list of real tables — use
`lookup_tables/2` to pick by model.

```erlang
q:lock(for_update, skip_locked,
    fun(Tables) -> q:lookup_tables(?USER, Tables) end).
```
""".
-spec lock(row_lock_level(), wait_policy(), fun(([RealTable]) -> [RealTable])) -> qfun() when
    RealTable :: real_table().
lock(RowLockLevel, WaitPolicy, Fun) ->
    fun(Q) -> lock(RowLockLevel, WaitPolicy, Fun, Q) end.

-spec lock(row_lock_level(), wait_policy(), fun(([RealTable]) -> [RealTable]), query()) -> query() when
    RealTable :: real_table().
lock(RowLockLevel, WaitPolicy, Fun, #query{tables = AllTables} = Q) ->
    RealTables = [T || {real, _Table, _TRef} = T <- AllTables],
    Q#query{lock = {RowLockLevel, Fun(RealTables), WaitPolicy}}.

-doc(#{group => <<"Locking">>}).
-doc """
Filter a list of real tables to those matching the given model(s).

Used inside `lock/3,4` to scope locking to specific tables. Throws
`{unknown_table, Model}` when a requested model is not present.
""".
-spec lookup_tables(model() | [model()], [RealTable]) -> [RealTable] when
    RealTable :: real_table().
%% @THROWS {unknown_table, model()}
lookup_tables(Models, Tables) when is_list(Models) ->
    lists:flatmap(
        fun(M) ->
            TableId = schema_table_id(get_schema(M)),
            RealTables = [T || {real, Id, _TRef} = T <- Tables, Id =:= TableId],
            case RealTables of
                [] -> error({unknown_table, M});
                _ -> RealTables
            end
        end,
        Models);
lookup_tables(Model, Tables) ->
    lookup_tables([Model], Tables).

-doc(#{group => <<"Locking">>}).
-doc """
Shorthand for `lock(for_update, wait)`.
""".
-spec for_update() -> qfun().
for_update() -> fun(Q) -> for_update(Q) end.

-spec for_update(Q) -> Q when Q :: query().
for_update(Q) ->
    lock(for_update, wait, fun(T) -> T end, Q).

-doc(#{group => <<"Refinement">>}).
-doc """
Rewrite the data context — advanced. Closure receives the list of field
maps and returns a new list. Useful for splicing computed columns into
the data passed to subsequent builders.
""".
-spec data(fun((data()) -> data())) -> qfun().
data(Fun) -> fun(Q) -> data(Fun, Q) end.

-spec data(fun((data()) -> data()), Q) -> Q when Q :: query().
data(Fun, #query{data=Data}=Q) ->
    Data2 = call(Fun, [Data]),
    is_list(Data2) orelse error(bad_list),
    Q#query{data=Data2}.

-doc(#{group => <<"Refinement">>}).
-doc """
`SELECT DISTINCT` — distinct over all selected columns.
""".
-spec distinct() -> qfun().
distinct() -> fun(Q) -> distinct(Q) end.

-spec distinct(Q) -> Q when Q :: query().
distinct(#query{}=Q) -> Q#query{distinct = all}.

-doc(#{group => <<"Refinement">>}).
-doc """
`SELECT DISTINCT ON (cols)`. Closure returns a list of column atoms.
""".
-spec distinct_on(fun((data()) -> [atom()])) -> qfun().
distinct_on(Fun) -> fun(Q) -> distinct_on(Fun, Q) end.

-spec distinct_on(fun((data()) -> [atom()]), Q) -> Q when Q :: query().
distinct_on(Fun, #query{data=Data}=Q) ->
    Distinct = call(Fun, [Data]),
    is_list(Distinct) orelse error(bad_list),
    Q#query{distinct = Distinct}.

-doc(#{group => <<"Composition">>}).
-doc """
Compile a nullary fun that returns a DSL closure into the closure itself,
applying the parse transform at AST level. Used to pre-build reusable
closures from shell-loaded code.
""".
compile(Fun) -> call(Fun, []).

%% =============================================================================
%% Internal functions
%% =============================================================================

call(Fun, Args) -> apply(equery_pt:transform_fun(Fun), Args).

get_schema(Schema) when is_map(Schema) -> Schema;
get_schema(Module) when is_atom(Module) -> (Module:schema())#{model => Module}.

aliased_fields(TRef, Fields) ->
    maps:map(fun(F, Opts) ->
        qast:exp([
            qast:alias(TRef), qast:raw([".", equery_utils:field_name(F)])
        ], Opts)
    end, Fields).

braced(QAst) ->
    qast:exp([
        qast:raw("("), QAst, qast:raw(")")
    ], qast:opts(QAst)).
