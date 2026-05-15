[![Build Status](https://github.com/egobrain/equery/actions/workflows/ci.yml/badge.svg)](https://github.com/egobrain/equery/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/egobrain/equery/badge.svg?branch=master)](https://coveralls.io/github/egobrain/equery)
[![Hex.pm](https://img.shields.io/hexpm/v/equery.svg)](https://hex.pm/packages/equery)

# equery

**Composable, type-aware PostgreSQL query builder DSL for Erlang.**

equery turns Erlang funs into SQL. You write what looks like ordinary pattern-matching code, and a parse transform rewrites operators (`=:=`, `>`, `andalso`, …) into AST builders. The result is composable queries, automatic parameter binding, and broad coverage of real PostgreSQL features — joins, CTE, recursive, LATERAL, ON CONFLICT, window-free aggregates, JSONB, arrays, and more.

```erlang
1> Schema = #{
       fields => #{id => #{type => integer}, name => #{type => text}},
       table => <<"users">>
   }.
2> Q = q:pipe(q:from(Schema), [
       q:where(fun([#{name := N, id := Id}]) -> N =:= <<"alice">> andalso Id > 3 end),
       q:order_by(fun([#{id := Id}]) -> [{Id, desc, nulls_last}] end),
       q:limit(10)
   ]).
3> qast:to_sql(qsql:select(Q)).
{<<"select \"a\".\"id\" as \"id\",\"a\".\"name\" as \"name\" "
   "from \"users\" as \"a\" "
   "where ((\"a\".\"name\" = $1) and (\"a\".\"id\" > $2)) "
   "order by \"a\".\"id\" DESC NULLS LAST "
   "limit $3">>,
 [<<"alice">>, 3, 10]}
```

---

## Install

```erlang
%% rebar.config
{deps, [{equery, "~> 0.x"}]}.
```

Then in any module that builds queries:

```erlang
-include_lib("equery/include/equery.hrl").
```

This enables the parse transform, so native Erlang operators inside `q:where`, `q:select`, etc. produce SQL expressions.

> **Works in the shell too.** Funs typed at the Erlang shell (including `remsh` to a live node) are detected at runtime — equery rewrites their AST on the fly, so `q:where(fun(...) -> Id > 3 end)` produces SQL even without compile-time parse transform. Great for ad-hoc debugging against a production node.

## Anatomy

equery is split into focused modules:

| Module | Responsibility |
|---|---|
| `q` | Query building DSL (`from`, `where`, `join`, `lateral_join`, `select`, `set`, `order_by`, `limit`, CTE, locks, …) |
| `qsql` | Statement compilation (`select/1`, `insert/1`, `update/1`, `delete/1`) |
| `qast` | AST primitives and SQL emission with parameter binding |
| `pg_sql` | SQL expression builders — operators, scalar/aggregate functions, CASE, type casts |
| `qjson` | JSON/JSONB operators, builders, and mutations |
| `equery_utils` | Identifier wrapping, order item rendering |

You compose a `#query{}` with the `q` module, then compile it with `qsql:select/insert/update/delete`. The resulting `qast` node is rendered via `qast:to_sql/1` into `{Sql, Args}` ready for `epgsql`.

---

## Quick tour

### Schemas

```erlang
-define(USER, #{
    fields => #{
        id       => #{type => integer, index => true, autoincrement => true},
        name     => #{type => {varchar, 60}, required => true},
        email    => #{type => text},
        active   => #{type => boolean},
        payload  => #{type => jsonb},
        tags     => #{type => {array, text}},
        created  => #{type => timestamptz}
    },
    table => <<"users">>
}).
```

Schema is just a map. `index` marks fields used in upsert/update WHERE clauses. Type info propagates through expressions, helping with parameter binding.

**Schema-qualified table names** — add `schema => <<"public">>` to the map and equery emits `"public"."users"` everywhere.

### CRUD

```erlang
%% SELECT
qsql:select(q:pipe(q:from(?USER), [
    q:where(fun([#{active := A}]) -> A =:= true end),
    q:order_by(fun([#{created := C}]) -> [{C, desc}] end),
    q:limit(50)
])).

%% INSERT
qsql:insert(q:set(fun(_) ->
    #{name => <<"alice">>, email => <<"a@x.io">>, active => true}
end, q:from(?USER))).

%% UPDATE
qsql:update(q:pipe(q:from(?USER), [
    q:set(fun(_) -> #{active => false} end),
    q:where(fun([#{id := Id}]) -> Id =:= 42 end)
])).

%% DELETE
qsql:delete(q:where(fun([#{id := Id}]) -> Id =:= 42 end, q:from(?USER))).
```

All write statements return updated rows via `RETURNING` automatically; control what's returned with `q:select`.

### Joins

```erlang
q:pipe(q:from(?USER), [
    q:join(?POST, fun([#{id := UId}, #{author_id := AId}]) -> UId =:= AId end),
    q:select(fun([#{name := N}, #{header := H}]) -> #{author => N, post => H} end)
]).
```

All join types supported: `inner`, `left`, `right`, `full`, `{left, outer}`, etc.

### LATERAL JOIN

For top-N-per-group, set-returning functions, parameterized subqueries:

```erlang
q:pipe(q:from(?USER), [
    q:lateral_join(left, fun([#{id := UId}]) ->
        q:pipe(q:from(?POST), [
            q:where(fun([#{author_id := A}]) -> A =:= UId end),
            q:order_by(fun([#{created := C}]) -> [{C, desc}] end),
            q:limit(3)
        ])
    end)
]).
```

→ `left join lateral (select ... where (author_id = u.id) order by created DESC limit 3) on true`

### CTE / Recursive

```erlang
q:pipe(q:from(?USER), [
    q:with(?ACTIVE_USERS, fun(CTE) -> q:join(CTE, fun(...) -> ... end) end)
]).

%% Recursive — graph traversal
q:recursive(
    q:where(fun([#{id := Id}]) -> Id =:= 1 end, q:from(?TREE)),
    fun(Q) ->
        q:select(fun([_, T]) -> T end,
            q:join(?TREE,
                fun([#{id := Id}, #{parent := P}]) -> Id =:= P end, Q))
    end).
```

### ON CONFLICT (upsert)

```erlang
q:pipe(q:from(?USER), [
    q:set(fun(_) -> ?USER_DATA end),
    q:on_conflict([id], fun([_, Excluded]) -> Excluded end),

    %% with WHERE on target
    q:on_conflict_where(
        [email],
        fun([#{active := A}]) -> A =:= true end,
        fun([_, Excluded]) -> Excluded end),

    %% multiple targets including ON CONFLICT DO NOTHING
    q:on_conflict(any, fun(_) -> nothing end)
]).
```

### Locking

`SELECT ... FOR UPDATE` and friends, with `NOWAIT` / `SKIP LOCKED`:

```erlang
q:pipe(q:from(?USER), [
    q:where(fun([#{id := Id}]) -> Id =:= 42 end),
    q:for_update()
]).

%% Granular control
q:lock(for_no_key_update, skip_locked,
    fun(Tables) -> q:lookup_tables(?USER, Tables) end).
```

Levels: `for_update`, `for_no_key_update`, `for_share`, `for_key_share`.
Wait policies: `wait`, `nowait`, `skip_locked`.

### Composition

`q:pipe/2` chains `qfun()`s — partially-applied builders:

```erlang
BaseFilter = q:where(fun([#{active := A}]) -> A =:= true end),
Recent     = q:order_by(fun([#{created := C}]) -> [{C, desc}] end),
TopN       = q:limit(10),

q:pipe(q:from(?USER), [BaseFilter, Recent, TopN]).
```

Builders are values, can be stored, composed, conditionally applied.

---

## SQL expressions

Inside any `q:where`/`q:select`/`q:join`/etc. lambda, Erlang operators are rewritten by the parse transform:

| Erlang | SQL |
|---|---|
| `=:=`, `=/=`, `>`, `>=`, `<`, `=<` | `=`, `<>`, `>`, `>=`, `<`, `<=` |
| `andalso`, `orelse`, `not` | `and`, `or`, `not` |
| `+`, `-`, `*`, `/`, `rem`, `div` | corresponding PG ops |

Anything else — explicit `pg_sql:` / `qjson:` calls.

### NULL / distinction

```erlang
pg_sql:is_null(F)
pg_sql:is_not_null(F)
pg_sql:is_distinct_from(A, B)        %% NULL-safe ≠
pg_sql:is_not_distinct_from(A, B)    %% NULL-safe =
```

### CASE WHEN

```erlang
pg_sql:case_when([
    {Id > 100, <<"big">>},
    {Id > 10,  <<"medium">>}
], <<"small">>)
```

### IN / EXISTS

```erlang
pg_sql:in(Id, [1, 2, 3])
pg_sql:in(Id, Subquery)
pg_sql:exists(Subquery)
```

### Pattern matching

```erlang
pg_sql:like(N, <<"%alice%">>)
pg_sql:ilike(N, <<"alice">>)
pg_sql:'~'(N, <<"^a">>)              %% regex
pg_sql:'~*'(N, <<"^a">>)             %% case-insensitive regex
```

### Type casts

```erlang
pg_sql:as(Id, text)                  %% (id)::text
pg_sql:as(V, {varchar, 60})          %% (v)::varchar(60)
pg_sql:as(V, {array, int})           %% (v)::int[]
```

---

## Scalar functions

### Numeric

```erlang
pg_sql:abs(X), pg_sql:round(X, 2), pg_sql:ceil(X), pg_sql:floor(X),
pg_sql:mod(X, Y), pg_sql:power(X, Y), pg_sql:sqrt(X),
pg_sql:ln(X), pg_sql:log(X), pg_sql:log(Base, X), pg_sql:exp(X),
pg_sql:sign(X), pg_sql:random(),

pg_sql:min(A, B),                    %% LEAST(a, b)
pg_sql:max(A, B),                    %% GREATEST(a, b)
pg_sql:least([A, B, C, D]),          %% N-ary LEAST
pg_sql:greatest([A, B, C, D])        %% N-ary GREATEST
```

### Strings

```erlang
pg_sql:concat([A, B, C]), pg_sql:concat(A, B),
pg_sql:'||'(A, B),                   %% NULL-propagating concat
pg_sql:length(S), pg_sql:char_length(S),
pg_sql:lower(S), pg_sql:upper(S),
pg_sql:trim(S), pg_sql:trim(S, Chars), pg_sql:ltrim(S), pg_sql:rtrim(S),
pg_sql:replace(S, From, To),
pg_sql:split_part(S, Sep, N),
pg_sql:substring(S, From), pg_sql:substring(S, From, Len),
pg_sql:strpos(Haystack, Needle),
pg_sql:starts_with(S, Prefix),
pg_sql:regexp_replace(S, Pat, Repl), pg_sql:regexp_replace(S, Pat, Repl, Flags),
pg_sql:regexp_match(S, Pat), pg_sql:regexp_match(S, Pat, Flags)
```

### Date / time

```erlang
pg_sql:now(),
pg_sql:current_timestamp(), pg_sql:current_date(), pg_sql:current_time(),

%% Field arg is a closed enum (compile-time safety):
%%   year | month | day | hour | minute | second | week | quarter |
%%   century | decade | millennium | microseconds | milliseconds |
%%   dow | doy | isodow | isoyear | julian | epoch |
%%   timezone | timezone_hour | timezone_minute
pg_sql:date_trunc(day, T),
pg_sql:extract(year, T),
pg_sql:date_part(dow, T),

pg_sql:age(T), pg_sql:age(T1, T2),
pg_sql:to_char(T, <<"YYYY-MM-DD">>),
pg_sql:to_date(<<"2024">>, <<"YYYY">>),
pg_sql:to_timestamp(EpochSec), pg_sql:to_timestamp(Text, Fmt)
```

Field argument for `extract`/`date_trunc`/`date_part` is restricted to a closed enum — invalid atoms fail with `function_clause` at build time, preventing SQL injection.

---

## Aggregates

```erlang
pg_sql:count(F), pg_sql:sum(F), pg_sql:avg(F),
pg_sql:min(F), pg_sql:max(F),
pg_sql:bool_and(F), pg_sql:bool_or(F), pg_sql:every(F),
pg_sql:array_agg(F), pg_sql:array_agg(F, [{F2, asc}]),
pg_sql:string_agg(F, <<",">>),
pg_sql:string_agg(F, <<",">>, [{Ord, asc, nulls_last}]),
pg_sql:json_agg(F), pg_sql:jsonb_agg(F),
pg_sql:json_object_agg(K, V), pg_sql:jsonb_object_agg(K, V),
pg_sql:percentile_cont(0.5, F), pg_sql:percentile_disc(0.9, F),
pg_sql:mode(F)
```

### FILTER (WHERE …)

Restricts an aggregate's input rows without affecting the rest of the query — essential for dashboard queries with multiple metrics:

```erlang
q:select(fun([#{id := Id, status := S}]) ->
    #{
        total    => pg_sql:count(Id),
        paid     => pg_sql:filter(pg_sql:count(Id), S =:= <<"paid">>),
        refunded => pg_sql:filter(pg_sql:count(Id), S =:= <<"refunded">>),
        revenue  => pg_sql:filter(pg_sql:sum(Amt),  S =:= <<"paid">>)
    }
end).
```

### ORDER BY inside aggregate

```erlang
pg_sql:string_agg(Name, <<", ">>, [{Name, asc}])
%% → string_agg(name, ', ' order by name ASC)
```

---

## JSON / JSONB

### Operators

```erlang
qjson:'->'(Field, key), qjson:'->>'(Field, key),       %% access
qjson:'#>'(Field, [a, b]), qjson:'#>>'(Field, [a, b]), %% path access
qjson:'@>'(F, Obj), qjson:'<@'(F, Obj),                %% containment
qjson:'?'(F, Key), qjson:'?|'(F, Keys), qjson:'?&'(F, Keys)
```

### Builders

```erlang
qjson:jsonb_build_object(#{                  %% Erlang map → jsonb object
    id   => Id,
    name => Name,
    flag => Id > 10
}),

qjson:jsonb_build_array([Id, Name, Age]),
qjson:to_jsonb(V),
qjson:row_to_json(pg_sql:row(#{id => Id, name => Name})),
qjson:array_to_json(Arr)
```

### Mutation

```erlang
qjson:jsonb_set(F, [<<"address">>, <<"city">>], NewCity),
qjson:jsonb_set(F, [<<"new_key">>], V, true),       %% with create_missing
qjson:jsonb_insert(F, [<<"a">>], V),
qjson:jsonb_strip_nulls(F)
```

---

## Arrays

```erlang
pg_sql:array([1, 2, 3]),                              %% ARRAY[1, 2, 3]
pg_sql:'@>'(A, B), pg_sql:'<@'(A, B), pg_sql:'&&'(A, B),

pg_sql:array_length(A), pg_sql:array_length(A, 2),
pg_sql:array_position(A, X),
pg_sql:array_append(A, X), pg_sql:array_prepend(X, A),
pg_sql:array_remove(A, X), pg_sql:array_replace(A, From, To),
pg_sql:array_cat(A, B),

pg_sql:unnest(A)                                      %% set-returning
```

`unnest` integrates with `q:from`/`q:lateral_join` — use it as a table source:

```erlang
%% Top-N tags per user via LATERAL unnest
q:pipe(q:from(?USER), [
    q:lateral_join(inner, fun([#{tags := T}]) ->
        q:from(pg_sql:unnest(T))
    end),
    q:select(fun([#{name := N}, #{unnest := Tag}]) ->
        #{user => N, tag => Tag}
    end)
]).
```

---

## Streaming & raw

For low-level needs:

```erlang
qast:raw(<<"now() AT TIME ZONE 'UTC'">>)             %% raw SQL fragment
qast:value(V, #{type => jsonb})                      %% typed param placeholder
pg_sql:call("custom_fn", [A, B], #{type => text})    %% any PG function
```

---

## Why equery

- **Composable**: queries are values, partially-applied builders are values. Build them in pieces, store them, share them.
- **Type-aware**: types propagate through expressions; type metadata is on every ast node, useful for parameter binding and inference.
- **Honest SQL**: emits clean SQL — no ORM noise, no leaky abstractions. What you write maps directly to what PG sees.
- **Broad PG coverage**: aggregates with FILTER and ORDER BY, LATERAL joins, recursive CTEs, ON CONFLICT with target WHERE, JSONB builders and mutation, array constructor and operators, set-returning functions, schema-qualified names, all row-lock variants.
- **Safe by construction**: identifiers are quoted, datetime field enums prevent injection, all values are parameterized.

equery is the foundation for [repo](https://github.com/egobrain/repo) — a data-mapper layer with schemas, lifecycle hooks, preloading, and connection pooling. Use equery directly if you want a pure query builder; use repo if you want a full Ecto-style ORM.

---

## License

MIT.
