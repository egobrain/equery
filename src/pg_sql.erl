-module(pg_sql).
-moduledoc """
SQL expression builders.

Operators, scalar functions, aggregates, CASE, type casts. These produce
`qast:ast_node()` values that go inside `q:where`/`q:select`/etc.
closures.

Inside DSL closures, native Erlang operators (`=:=`, `>`, `andalso`,
`+`, …) are rewritten by `equery_pt` to corresponding `pg_sql:`
functions, so you rarely call these directly for primitive operations.

Use the explicit `pg_sql:` form for:
- Functions (`pg_sql:count/1`, `pg_sql:lower/1`, `pg_sql:date_trunc/2`, …);
- Operators that aren't Erlang operators (`pg_sql:like/2`, `pg_sql:'~'/2`);
- Aggregates and `pg_sql:filter/2`;
- `pg_sql:case_when/1,2`;
- Type casts (`pg_sql:as/2`).

JSON/JSONB live in [`qjson`](`m:qjson`).
""".

-include("query.hrl").
-include("cth.hrl").

-export([
         'andalso'/2,
         'orelse'/2,

         '=:='/2,
         '=/='/2,
         '>'/2,
         '>='/2,
         '<'/2,
         '=<'/2,
         'not'/1,
         'is'/2,
         is_null/1,
         is_not_null/1,
         is_distinct_from/2,
         is_not_distinct_from/2,

         '+'/2,
         '-'/2,
         '*'/2,
         '/'/2,
         'div'/2,
         'rem'/2,
         'abs'/1
        ]).

-export([
         mod/2,
         round/1, round/2,
         ceil/1,
         floor/1,
         power/2,
         sqrt/1,
         ln/1,
         log/1, log/2,
         exp/1,
         sign/1,
         random/0
        ]).

-export([
         '~'/2,
         '~*'/2,
         like/2,
         ilike/2
        ]).

-export([
         call/3
        ]).

-export([
         sum/1,
         count/1,
         min/1,
         max/1,
         distinct/1,
         array_agg/1, array_agg/2,
         avg/1,
         bool_and/1,
         bool_or/1,
         every/1,
         string_agg/2, string_agg/3,
         json_agg/1, json_agg/2,
         jsonb_agg/1, jsonb_agg/2,
         json_object_agg/2,
         jsonb_object_agg/2,
         percentile_cont/2,
         percentile_disc/2,
         mode/1,
         filter/2,
         trunc/2
        ]).

-export([
         min/2,
         max/2,
         greatest/1,
         least/1,
         row/1,
         row/2
        ]).

-export([
         coalesce/1,
         case_when/1,
         case_when/2,
         in/2,
         exists/1
        ]).

%% Date/time functions
-export([
         now/0,
         current_timestamp/0,
         current_date/0,
         current_time/0,
         date_trunc/2,
         extract/2,
         date_part/2,
         age/1, age/2,
         to_char/2,
         to_date/2,
         to_timestamp/1, to_timestamp/2
        ]).

%% String functions
-export([
         concat/1, concat/2,
         length/1,
         char_length/1,
         lower/1,
         upper/1,
         trim/1, trim/2,
         ltrim/1, ltrim/2,
         rtrim/1, rtrim/2,
         replace/3,
         split_part/3,
         substring/2, substring/3,
         strpos/2,
         starts_with/2,
         regexp_replace/3, regexp_replace/4,
         regexp_match/2, regexp_match/3
        ]).

%% Array functions
-export([
         '@>'/2,
         '<@'/2,
         '&&'/2,
         array/1,
         array_length/1, array_length/2,
         array_position/2,
         array_append/2,
         array_prepend/2,
         array_remove/2,
         array_replace/3,
         array_cat/2,
         unnest/1
        ]).

%% String / array concat
-export([
         '||'/2
        ]).

%% Type function
-export([
         as/2,
         set_type/2
        ]).

%% =============================================================================
%% Sql operations
%% =============================================================================

%% DSL expression: either an AST node, or any term that gets auto-wrapped
%% as a `$value` placeholder at SQL generation time.
-type expr() :: qast:ast_node() | term().

-export_type([expr/0]).

%% = Primitive =================================================================

-doc(#{group => <<"Logical">>}).
-doc "Logical `AND`. Short-circuits on boolean literals. See [Logical Operators](https://www.postgresql.org/docs/current/functions-logical.html).".
-spec 'andalso'(V, V) -> V when V :: boolean() | qast:ast_node().
'andalso'(true, B) -> B;
'andalso'(A, true) -> A;

'andalso'(false, _) -> false;
'andalso'(_, false) -> false;

'andalso'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" and "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Logical">>}).
-doc "Logical `OR`. Short-circuits on boolean literals. See [Logical Operators](https://www.postgresql.org/docs/current/functions-logical.html).".
-spec 'orelse'(V, V) -> V when V :: boolean() | qast:ast_node().
'orelse'(true, _) -> true;
'orelse'(_, true) -> true;
'orelse'(false, B) -> B;
'orelse'(A, false) -> A;
'orelse'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" or "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Logical">>}).
-doc "Logical `NOT`. See [Logical Operators](https://www.postgresql.org/docs/current/functions-logical.html).".
-spec 'not'(V) -> V when V :: boolean() | qast:ast_node().
'not'(A) when is_boolean(A) -> not A;
'not'(A) ->
    qast:exp([qast:raw("not "), A], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A = B`. See [Comparison Operators](https://www.postgresql.org/docs/current/functions-comparison.html).".
-spec '=:='(expr(), expr()) -> qast:ast_node().
'=:='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" = "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A <> B`. See [Comparison Operators](https://www.postgresql.org/docs/current/functions-comparison.html).".
-spec '=/='(expr(), expr()) -> qast:ast_node().
'=/='(A, B) -> 'not'('=:='(A,B)).

-doc(#{group => <<"Comparison">>}).
-doc "`A > B`. See [Comparison Operators](https://www.postgresql.org/docs/current/functions-comparison.html).".
-spec '>'(expr(), expr()) -> qast:ast_node().
'>'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" > "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A >= B`.".
-spec '>='(expr(), expr()) -> qast:ast_node().
'>='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" >= "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A < B`.".
-spec '<'(expr(), expr()) -> qast:ast_node().
'<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" < "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A <= B`.".
-spec '=<'(expr(), expr()) -> qast:ast_node().
'=<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" <= "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A IS B`. Low-level building block for `IS NULL`, `IS TRUE`, etc. See [Comparison Predicates](https://www.postgresql.org/docs/current/functions-comparison.html#FUNCTIONS-COMPARISON-PRED-TABLE).".
-spec 'is'(expr(), expr()) -> qast:ast_node().
is(A, B) ->
    qast:exp([A, qast:raw(" is "), B], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A IS NULL`. See [Comparison Predicates](https://www.postgresql.org/docs/current/functions-comparison.html#FUNCTIONS-COMPARISON-PRED-TABLE).".
-spec 'is_null'(expr()) -> qast:ast_node().
is_null(A) ->
    is(A, qast:raw("null")).

-doc(#{group => <<"Comparison">>}).
-doc "`A IS NOT NULL`.".
-spec 'is_not_null'(expr()) -> qast:ast_node().
is_not_null(A) ->
    qast:exp([A, qast:raw(" is not null")], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc """
`A IS DISTINCT FROM B` — NULL-safe inequality (treats `NULL = NULL` as
equal). See [Comparison Predicates](https://www.postgresql.org/docs/current/functions-comparison.html#FUNCTIONS-COMPARISON-PRED-TABLE).
""".
-spec is_distinct_from(expr(), expr()) -> qast:ast_node().
is_distinct_from(A, B) ->
    qast:exp([A, qast:raw(" is distinct from "), B], #{type => boolean}).

-doc(#{group => <<"Comparison">>}).
-doc "`A IS NOT DISTINCT FROM B` — NULL-safe equality.".
-spec is_not_distinct_from(expr(), expr()) -> qast:ast_node().
is_not_distinct_from(A, B) ->
    qast:exp([A, qast:raw(" is not distinct from "), B], #{type => boolean}).

%% @TODO type opts
-doc(#{group => <<"Arithmetic">>}).
-doc "`A + B`. See [Mathematical Operators](https://www.postgresql.org/docs/current/functions-math.html#FUNCTIONS-MATH-OP-TABLE).".
-spec '+'(expr(), expr()) -> qast:ast_node().
'+'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" + "), B, qast:raw(")")]).

-doc(#{group => <<"Arithmetic">>}).
-doc "`A - B`.".
-spec '-'(expr(), expr()) -> qast:ast_node().
'-'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" - "), B, qast:raw(")")]).

-doc(#{group => <<"Arithmetic">>}).
-doc "`A * B`.".
-spec '*'(expr(), expr()) -> qast:ast_node().
'*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" * "), B, qast:raw(")")]).

-doc(#{group => <<"Arithmetic">>}).
-doc "`A / B` — division. For integers, truncates toward zero.".
-spec '/'(expr(), expr()) -> qast:ast_node().
'/'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" / "), B, qast:raw(")")]).

-doc(#{group => <<"Arithmetic">>}).
-doc "`abs(A)` — absolute value. See [Math Functions](https://www.postgresql.org/docs/current/functions-math.html#FUNCTIONS-MATH-FUNC-TABLE).".
-spec 'abs'(expr()) -> qast:ast_node().
abs(A) ->
    qast:exp([qast:raw("abs("), A, qast:raw(")")], qast:opts(A)).

-doc(#{group => <<"Arithmetic">>}).
-doc "`div(A, B)` — integer quotient, truncated toward zero.".
-spec 'div'(expr(), expr()) -> qast:ast_node().
'div'(A, B) ->
    call("div", [A, B], qast:opts(A)).

-doc(#{group => <<"Arithmetic">>}).
-doc "Alias for [`mod/2`](`mod/2`). Erlang's `rem` operator inside DSL closures rewrites to this.".
-spec 'rem'(expr(), expr()) -> qast:ast_node().
'rem'(A, B) -> mod(A, B).

%% = Numeric functions =========================================================

-doc(#{group => <<"Numeric">>}).
-doc "`mod(A, B)` — remainder of `A / B`. See [Math Functions](https://www.postgresql.org/docs/current/functions-math.html#FUNCTIONS-MATH-FUNC-TABLE).".
-spec mod(expr(), expr()) -> qast:ast_node().
mod(A, B) ->
    call("mod", [A, B], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`round(A)` — round to nearest integer (banker's rounding for numeric).".
-spec round(expr()) -> qast:ast_node().
round(A) ->
    call("round", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`round(A, N)` — round to `N` decimal places.".
-spec round(expr(), expr()) -> qast:ast_node().
round(A, N) ->
    call("round", [A, N], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`ceil(A)` — ceiling (smallest integer not less than `A`).".
-spec ceil(expr()) -> qast:ast_node().
ceil(A) ->
    call("ceil", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`floor(A)` — floor (largest integer not greater than `A`).".
-spec floor(expr()) -> qast:ast_node().
floor(A) ->
    call("floor", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`power(A, B)` — `A` raised to the power of `B`.".
-spec power(expr(), expr()) -> qast:ast_node().
power(A, B) ->
    call("power", [A, B], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`sqrt(A)` — square root.".
-spec sqrt(expr()) -> qast:ast_node().
sqrt(A) ->
    call("sqrt", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`ln(A)` — natural logarithm.".
-spec ln(expr()) -> qast:ast_node().
ln(A) ->
    call("ln", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`log(A)` — base-10 logarithm.".
-spec log(expr()) -> qast:ast_node().
log(A) ->
    call("log", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`log(B, A)` — logarithm of `A` to base `B`.".
-spec log(expr(), expr()) -> qast:ast_node().
log(B, A) ->
    call("log", [B, A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`exp(A)` — exponential (`e^A`).".
-spec exp(expr()) -> qast:ast_node().
exp(A) ->
    call("exp", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`sign(A)` — `-1`, `0`, or `1` according to sign.".
-spec sign(expr()) -> qast:ast_node().
sign(A) ->
    call("sign", [A], qast:opts(A)).

-doc(#{group => <<"Numeric">>}).
-doc "`random()` — pseudo-random `double precision` in `[0.0, 1.0)`.".
-spec random() -> qast:ast_node().
random() ->
    call("random", [], #{}).

%% = LIKE ======================================================================

-doc(#{group => <<"Pattern matching">>}).
-doc "POSIX regex match `A ~ B`. See [POSIX Regular Expressions](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP).".
-spec '~'(expr(), expr()) -> qast:ast_node().
'~'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" ~ "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Pattern matching">>}).
-doc "POSIX regex match (case-insensitive) `A ~* B`.".
-spec '~*'(expr(), expr()) -> qast:ast_node().
'~*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" ~* "), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Pattern matching">>}).
-doc "`A LIKE B`. See [LIKE](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE).".
like(A, B) ->
    qast:exp([A, qast:raw(" like "), B], #{type => boolean}).

-doc(#{group => <<"Pattern matching">>}).
-doc "`A ILIKE B` — case-insensitive `LIKE` (PostgreSQL extension).".
ilike(A, B) ->
    qast:exp([A, qast:raw(" ilike "), B], #{type => boolean}).

%% = Aggregators ===============================================================

-doc(#{group => <<"Aggregates">>}).
-doc "`sum(A)`. See [Aggregate Functions](https://www.postgresql.org/docs/current/functions-aggregate.html).".
-spec sum(qast:ast_node()) -> qast:ast_node().
sum(Ast) ->
    call("sum", [Ast], qast:opts(Ast)).

-doc(#{group => <<"Aggregates">>}).
-doc "`count(A)` — non-NULL row count. Returns `bigint`.".
-spec count(qast:ast_node()) -> qast:ast_node().
count(Ast) ->
    call("count", [Ast], #{type => integer}).

-doc(#{group => <<"Aggregates">>}).
-doc "`min(A)` — aggregate minimum.".
-spec 'min'(expr()) -> qast:ast_node().
min(Ast) ->
    call("min", [Ast], qast:opts(Ast)).

-doc(#{group => <<"Aggregates">>}).
-doc "`max(A)` — aggregate maximum.".
-spec 'max'(expr()) -> qast:ast_node().
max(Ast) ->
    call("max", [Ast], qast:opts(Ast)).

-doc(#{group => <<"Aggregates">>}).
-doc "`DISTINCT A` qualifier — typically used inside another aggregate, e.g. `count(distinct(x))`.".
-spec 'distinct'(expr()) -> qast:ast_node().
distinct(Ast) ->
    call("distinct ", [Ast], qast:opts(Ast)).

-doc(#{group => <<"Aggregates">>}).
-doc "`array_agg(A)` — collect values into an array. NULLs are included.".
-spec 'array_agg'(expr()) -> qast:ast_node().
array_agg(Ast) ->
    Opts = qast:opts(Ast),
    Type = maps:get(type, Opts, undefined),
    NewOpts = Opts#{type => {array, Type}},
    call("array_agg", [Ast], NewOpts).

-type agg_order_spec() :: expr()
                        | {expr(), asc | desc}
                        | {expr(), asc | desc, nulls_first | nulls_last}.
-type agg_order_specs() :: [agg_order_spec(), ...].

-doc(#{group => <<"Aggregates">>}).
-doc "`array_agg(A ORDER BY ...)` — collect into array with explicit ordering.".
-spec 'array_agg'(expr(), agg_order_specs()) -> qast:ast_node().
array_agg(Ast, OrderSpecs) ->
    Opts = qast:opts(Ast),
    Type = maps:get(type, Opts, undefined),
    NewOpts = Opts#{type => {array, Type}},
    qast:exp([
        qast:raw("array_agg("), Ast,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], NewOpts).

-doc(#{group => <<"Aggregates">>}).
-doc "`avg(A)` — arithmetic mean. Returns `numeric` or `double precision`.".
-spec avg(expr()) -> qast:ast_node().
avg(A) ->
    call("avg", [A], qast:opts(A)).

-doc(#{group => <<"Aggregates">>}).
-doc "`bool_and(A)` — true iff every non-NULL input is true.".
-spec bool_and(expr()) -> qast:ast_node().
bool_and(A) ->
    call("bool_and", [A], #{type => boolean}).

-doc(#{group => <<"Aggregates">>}).
-doc "`bool_or(A)` — true iff any non-NULL input is true.".
-spec bool_or(expr()) -> qast:ast_node().
bool_or(A) ->
    call("bool_or", [A], #{type => boolean}).

-doc(#{group => <<"Aggregates">>}).
-doc "`every(A)` — SQL standard alias for [`bool_and/1`](`bool_and/1`).".
-spec every(expr()) -> qast:ast_node().
every(A) ->
    call("every", [A], #{type => boolean}).

-doc(#{group => <<"Aggregates">>}).
-doc "`string_agg(expr, sep)` — concatenate string values with a separator. NULL inputs are ignored.".
-spec string_agg(expr(), expr()) -> qast:ast_node().
string_agg(Expr, Sep) ->
    call("string_agg", [Expr, Sep], #{type => text}).

-doc(#{group => <<"Aggregates">>}).
-doc """
`string_agg(expr, sep ORDER BY ...)` — concatenate with explicit ordering.

Without `ORDER BY`, `string_agg` is non-deterministic. Use the 3-arg form
when result order matters.
""".
-spec string_agg(expr(), expr(), agg_order_specs()) -> qast:ast_node().
string_agg(Expr, Sep, OrderSpecs) ->
    qast:exp([
        qast:raw("string_agg("), Expr, qast:raw(","), Sep,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => text}).

-doc(#{group => <<"Aggregates">>}).
-doc "`json_agg(A)` — aggregate into a JSON array. See [JSON Functions](https://www.postgresql.org/docs/current/functions-json.html).".
-spec json_agg(expr()) -> qast:ast_node().
json_agg(A) ->
    call("json_agg", [A], #{type => json}).

-doc(#{group => <<"Aggregates">>}).
-doc "`json_agg(A ORDER BY ...)`.".
-spec json_agg(expr(), agg_order_specs()) -> qast:ast_node().
json_agg(A, OrderSpecs) ->
    qast:exp([
        qast:raw("json_agg("), A,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => json}).

-doc(#{group => <<"Aggregates">>}).
-doc "`jsonb_agg(A)` — aggregate into a JSONB array.".
-spec jsonb_agg(expr()) -> qast:ast_node().
jsonb_agg(A) ->
    call("jsonb_agg", [A], #{type => jsonb}).

-doc(#{group => <<"Aggregates">>}).
-doc "`jsonb_agg(A ORDER BY ...)`.".
-spec jsonb_agg(expr(), agg_order_specs()) -> qast:ast_node().
jsonb_agg(A, OrderSpecs) ->
    qast:exp([
        qast:raw("jsonb_agg("), A,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => jsonb}).

-doc(#{group => <<"Aggregates">>}).
-doc "`json_object_agg(K, V)` — aggregate into a JSON object.".
-spec json_object_agg(expr(), expr()) -> qast:ast_node().
json_object_agg(K, V) ->
    call("json_object_agg", [K, V], #{type => json}).

-doc(#{group => <<"Aggregates">>}).
-doc "`jsonb_object_agg(K, V)` — aggregate into a JSONB object.".
-spec jsonb_object_agg(expr(), expr()) -> qast:ast_node().
jsonb_object_agg(K, V) ->
    call("jsonb_object_agg", [K, V], #{type => jsonb}).

-doc(#{group => <<"Aggregates">>}).
-doc """
`percentile_cont(Frac) WITHIN GROUP (ORDER BY x)` — interpolated continuous
percentile. `Frac` ∈ [0, 1].

See [Ordered-Set Aggregate Functions](https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE).
""".
-spec percentile_cont(expr(), expr()) -> qast:ast_node().
percentile_cont(Frac, OrderExpr) ->
    qast:exp([
        qast:raw("percentile_cont("), Frac,
        qast:raw(") within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-doc(#{group => <<"Aggregates">>}).
-doc "`percentile_disc(Frac) WITHIN GROUP (ORDER BY x)` — discrete percentile (picks one of the input values).".
-spec percentile_disc(expr(), expr()) -> qast:ast_node().
percentile_disc(Frac, OrderExpr) ->
    qast:exp([
        qast:raw("percentile_disc("), Frac,
        qast:raw(") within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-doc(#{group => <<"Aggregates">>}).
-doc "`mode() WITHIN GROUP (ORDER BY x)` — most frequent value (statistical mode).".
-spec mode(expr()) -> qast:ast_node().
mode(OrderExpr) ->
    qast:exp([
        qast:raw("mode() within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-doc(#{group => <<"Aggregates">>}).
-doc """
Wrap any aggregate with `FILTER (WHERE Cond)` — limits which rows the
aggregate sees, without affecting the rest of the query. Essential for
dashboard-style queries with multiple conditional metrics.

```erlang
#{
    total => pg_sql:count(Id),
    paid  => pg_sql:filter(pg_sql:count(Id), Status =:= <<"paid">>)
}.
```

See [Aggregate Expressions](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES).
""".
-spec filter(qast:ast_node(), expr()) -> qast:ast_node().
filter(AggAst, Cond) ->
    qast:exp([
        AggAst, qast:raw(" filter (where "), Cond, qast:raw(")")
    ], qast:opts(AggAst)).

agg_order_by(Specs) ->
    Exps = lists:map(fun agg_order_spec_exp/1, Specs),
    qast:exp([qast:raw(" order by "), qast:join(Exps, qast:raw(","))]).

agg_order_spec_exp({_F, D} = T) when D =:= asc; D =:= desc ->
    equery_utils:order_item_exp(T);
agg_order_spec_exp({_F, D, N} = T) when
        (D =:= asc orelse D =:= desc),
        (N =:= nulls_first orelse N =:= nulls_last) ->
    equery_utils:order_item_exp(T);
agg_order_spec_exp(F) -> F.

-doc(#{group => <<"Numeric">>}).
-doc "`trunc(V, N)` — truncate to `N` decimal places.".
-spec 'trunc'(expr(), qast:ast_node() | non_neg_integer()) -> qast:ast_node().
'trunc'(V, N) ->
    call("trunc", [V, N], qast:opts(V)).

%% = Math ======================================================================

-doc(#{group => <<"Conditional">>}).
-doc "`LEAST(A, B)` — minimum of two values, ignoring NULLs. See [Conditional Expressions](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-GREATEST-LEAST).".
-spec 'min'(expr(), expr()) -> qast:ast_node().
min(A, B) ->
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-doc(#{group => <<"Conditional">>}).
-doc "`GREATEST(A, B)` — maximum of two values, ignoring NULLs.".
-spec 'max'(expr(), expr()) -> qast:ast_node().
max(A, B) ->
    qast:exp([qast:raw("GREATEST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-doc(#{group => <<"Conditional">>}).
-doc "`GREATEST(A, B, C, ...)` — N-ary form. Returns max of all non-NULL inputs.".
-spec greatest([expr(), ...]) -> qast:ast_node().
greatest([H | _] = List) ->
    call("GREATEST", List, qast:opts(H)).

-doc(#{group => <<"Conditional">>}).
-doc "`LEAST(A, B, C, ...)` — N-ary form. Returns min of all non-NULL inputs.".
-spec least([expr(), ...]) -> qast:ast_node().
least([H | _] = List) ->
    call("LEAST", List, qast:opts(H)).

%% = Additional operations =====================================================

-doc(#{group => <<"Misc">>}).
-doc "`ROW(...)` constructor with anonymous record type. See [Row Constructors](https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-ROW-CONSTRUCTORS).".
-spec row(#{atom() => qast:ast_node()}) -> qast:ast_node().
row(Fields) when is_map(Fields) ->
    row(undefined, Fields).

-doc(#{group => <<"Misc">>}).
-doc "`ROW(...)` constructor tagged with a model module (for type inference in row-typed projections).".
-spec row(Model :: module(), #{atom() => qast:ast_node()}) -> qast:ast_node().
row(Model, Fields) when is_map(Fields) ->
    FieldsList = ?MAPS_TO_LIST(Fields),
    Type = {record, {model, Model, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]}},
    qast:exp([
        qast:raw("row("),
        qast:join([Node || {_F, Node} <- FieldsList], qast:raw(",")),
        qast:raw(")")
    ], #{type => Type}).

-doc(#{group => <<"Conditional">>}).
-doc """
`COALESCE(a, b, c, ...)` — returns the first non-NULL argument.

See [Conditional Expressions](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL).
""".
coalesce([H|_]=List) ->
    qast:exp([
        qast:raw("coalesce("),
        qast:join([Node || Node <- List], qast:raw(",")),
        qast:raw(")")
    ], maps:with([type], qast:opts(H))).

-doc(#{group => <<"Conditional">>}).
-doc "Searched `CASE WHEN cond THEN val ... END` without `ELSE` — `NULL` if no branch matches.".
-spec case_when([{expr(), expr()}, ...]) -> qast:ast_node().
case_when(Whens) ->
    case_when(Whens, undefined).

-doc(#{group => <<"Conditional">>}).
-doc """
`CASE WHEN cond THEN val ... ELSE default END`.

```erlang
pg_sql:case_when([
    {Id > 100, <<"big">>},
    {Id > 10,  <<"medium">>}
], <<"small">>).
```

See [Conditional Expressions](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-CASE).
""".
-spec case_when([{expr(), expr()}, ...], expr() | undefined) -> qast:ast_node().
case_when([{_, FirstThen}|_]=Whens, ElseSpec) ->
    Opts = maps:with([type], qast:opts(FirstThen)),
    WhenExps = lists:map(fun({W, T}) ->
        qast:exp([qast:raw(" when "), W, qast:raw(" then "), T])
    end, Whens),
    ElseExp = case ElseSpec of
        undefined -> qast:raw("");
        E -> qast:exp([qast:raw(" else "), E])
    end,
    qast:exp([
        qast:raw("case"),
        qast:exp(WhenExps),
        ElseExp,
        qast:raw(" end")
    ], Opts).

-doc(#{group => <<"Misc">>}).
-doc """
`A IN (...)` — membership test.

- With a `query()` argument, emits `A IN (subquery)`. See [Subquery Expressions](https://www.postgresql.org/docs/current/functions-subquery.html#FUNCTIONS-SUBQUERY-IN).
- With a list, emits `A = ANY($1)` (parameterized).
- With a single-element list, optimized to `A = item`.
""".
in(A, #query{}=Q) ->
    qast:exp([A, qast:raw(" in ("), qsql:select(Q), qast:raw(")")], #{type => boolean});
in(A, [Item]) ->
    '=:='(A, Item);
in(A, B) ->
    qast:exp([A, qast:raw(" = ANY("), B, qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Misc">>}).
-doc "`EXISTS (subquery)`. See [EXISTS](https://www.postgresql.org/docs/current/functions-subquery.html#FUNCTIONS-SUBQUERY-EXISTS).".
exists(#query{}=Q) ->
    qast:exp([qast:raw("exists ("), qsql:select(Q), qast:raw(")")], #{type => boolean}).

-doc(#{group => <<"Misc">>}).
-doc """
Build a function call AST: `FunName(Args...)`.

Used internally by all `pg_sql:` function builders. Public for custom
PG functions not yet wrapped by the library:

```erlang
pg_sql:call("my_extension_fn", [Arg1, Arg2], #{type => text}).
```

> #### Warning {: .warning}
> `FunName` is **inlined raw**. Never pass user-controlled input here;
> use a literal string.
""".
-spec call(iodata(), [expr()], qast:opts()) -> qast:ast_node().
call(FunName, Args, Opts) ->
    qast:exp([
        qast:raw([FunName, "("]),
        qast:join(Args, qast:raw(",")),
        qast:raw(")")
    ], Opts).

%% = Array oprterations ========================================================

-doc(#{group => <<"Arrays">>}).
-doc "`A @> B` — contains. See [Array Functions and Operators](https://www.postgresql.org/docs/current/functions-array.html).".
'@>'(A, B) ->
    qast:exp([A, qast:raw(" @> "), B], #{type => boolean}).

-doc(#{group => <<"Arrays">>}).
-doc "`A <@ B` — is contained by.".
-spec '<@'(expr(), expr()) -> qast:ast_node().
'<@'(A, B) ->
    qast:exp([A, qast:raw(" <@ "), B], #{type => boolean}).

-doc(#{group => <<"Arrays">>}).
-doc "`A && B` — arrays overlap (have any common element).".
-spec '&&'(expr(), expr()) -> qast:ast_node().
'&&'(A, B) ->
    qast:exp([A, qast:raw(" && "), B], #{type => boolean}).

-doc(#{group => <<"Arrays">>}).
-doc "`array_length(arr, 1)` — length of the first dimension.".
-spec array_length(expr()) -> qast:ast_node().
array_length(Arr) ->
    array_length(Arr, qast:value(1, #{type => integer})).

-doc(#{group => <<"Arrays">>}).
-doc "`array_length(arr, dim)` — length of dimension `dim`.".
-spec array_length(expr(), expr()) -> qast:ast_node().
array_length(Arr, Dim) ->
    call("array_length", [Arr, Dim], #{type => integer}).

-doc(#{group => <<"Arrays">>}).
-doc "`array_position(arr, elem)` — 1-based index of `elem` in `arr`, or NULL.".
-spec array_position(expr(), expr()) -> qast:ast_node().
array_position(Arr, Elem) ->
    call("array_position", [Arr, Elem], #{type => integer}).

-doc(#{group => <<"Arrays">>}).
-doc "`array_append(arr, elem)` — append element.".
-spec array_append(expr(), expr()) -> qast:ast_node().
array_append(Arr, Elem) ->
    call("array_append", [Arr, Elem], qast:opts(Arr)).

-doc(#{group => <<"Arrays">>}).
-doc "`array_prepend(elem, arr)` — prepend element.".
-spec array_prepend(expr(), expr()) -> qast:ast_node().
array_prepend(Elem, Arr) ->
    call("array_prepend", [Elem, Arr], qast:opts(Arr)).

-doc(#{group => <<"Arrays">>}).
-doc "`array_remove(arr, elem)` — remove all occurrences of `elem`.".
-spec array_remove(expr(), expr()) -> qast:ast_node().
array_remove(Arr, Elem) ->
    call("array_remove", [Arr, Elem], qast:opts(Arr)).

-doc(#{group => <<"Arrays">>}).
-doc "`array_replace(arr, from, to)` — replace all `from` elements with `to`.".
-spec array_replace(expr(), expr(), expr()) -> qast:ast_node().
array_replace(Arr, From, To) ->
    call("array_replace", [Arr, From, To], qast:opts(Arr)).

-doc(#{group => <<"Arrays">>}).
-doc "`array_cat(a, b)` — concatenate two arrays. Equivalent to `a || b`.".
-spec array_cat(expr(), expr()) -> qast:ast_node().
array_cat(A, B) ->
    call("array_cat", [A, B], qast:opts(A)).

-doc(#{group => <<"Arrays">>}).
-doc """
`unnest(arr)` — set-returning function that produces one row per array
element.

Integrates with `q:from/1` and `q:lateral_join/2,3,4`: the resulting
table has a single column named `unnest` (PostgreSQL default).

```erlang
q:pipe(q:from(?USER), [
    q:lateral_join(inner, fun([#{tags := T}]) ->
        q:from(pg_sql:unnest(T))
    end),
    q:select(fun([#{name := N}, #{unnest := Tag}]) ->
        #{name => N, tag => Tag}
    end)
]).
```
""".
-spec unnest(expr()) -> qast:ast_node().
unnest(Arr) ->
    ElemType =
        case maps:find(type, qast:opts(Arr)) of
            {ok, {array, T}} -> T;
            _ -> undefined
        end,
    qast:exp(
        [qast:raw("unnest("), Arr, qast:raw(")")],
        #{type => {model, undefined, [{unnest, #{type => ElemType}}]}}
    ).

-doc(#{group => <<"Arrays">>}).
-doc """
`ARRAY[v1, v2, ...]` constructor. Element type is inferred from the
first element's opts.

Empty array (`ARRAY[]`) needs an explicit cast in PG; use
[`as/2`](`as/2`): `pg_sql:as(pg_sql:array([]), {array, int})`.
""".
-spec array([expr()]) -> qast:ast_node().
array(Items) when is_list(Items) ->
    Opts = case Items of
        [] -> #{};
        [H | _] ->
            ElemType = maps:get(type, qast:opts(H), undefined),
            #{type => {array, ElemType}}
    end,
    qast:exp([
        qast:raw("ARRAY["),
        qast:join(Items, qast:raw(",")),
        qast:raw("]")
    ], Opts).

%% = String / array concat =====================================================

-doc(#{group => <<"Strings">>}).
-doc """
`A || B` — concatenation operator.

NULL-propagating: `'foo' || NULL` → `NULL`. For NULL-skipping behavior
use [`concat/2`](`concat/2`). Same operator works for arrays.

See [String Functions](https://www.postgresql.org/docs/current/functions-string.html).
""".
-spec '||'(expr(), expr()) -> qast:ast_node().
'||'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" || "), B, qast:raw(")")], qast:opts(A)).

%% = Date/time functions =======================================================

-doc(#{group => <<"Date/time">>}).
-doc "`now()` — current transaction timestamp (with timezone). PostgreSQL extension; equivalent to [`current_timestamp/0`](`current_timestamp/0`).".
-spec now() -> qast:ast_node().
now() ->
    call("now", [], #{type => timestamptz}).

-doc(#{group => <<"Date/time">>}).
-doc "`current_timestamp` — start-of-transaction timestamp with timezone. Standard SQL.".
-spec current_timestamp() -> qast:ast_node().
current_timestamp() ->
    qast:raw("current_timestamp", #{type => timestamptz}).

-doc(#{group => <<"Date/time">>}).
-doc "`current_date` — current date (date only).".
-spec current_date() -> qast:ast_node().
current_date() ->
    qast:raw("current_date", #{type => date}).

-doc(#{group => <<"Date/time">>}).
-doc "`current_time` — current time of day with timezone.".
-spec current_time() -> qast:ast_node().
current_time() ->
    qast:raw("current_time", #{type => timetz}).

-type datetime_field() ::
        century | day | decade | dow | doy | epoch | hour |
        isodow | isoyear | julian | microseconds | millennium |
        milliseconds | minute | month | quarter | second |
        timezone | timezone_hour | timezone_minute | week | year.

-spec datetime_field_str(datetime_field()) -> binary().
datetime_field_str(century) -> <<"century">>;
datetime_field_str(day) -> <<"day">>;
datetime_field_str(decade) -> <<"decade">>;
datetime_field_str(dow) -> <<"dow">>;
datetime_field_str(doy) -> <<"doy">>;
datetime_field_str(epoch) -> <<"epoch">>;
datetime_field_str(hour) -> <<"hour">>;
datetime_field_str(isodow) -> <<"isodow">>;
datetime_field_str(isoyear) -> <<"isoyear">>;
datetime_field_str(julian) -> <<"julian">>;
datetime_field_str(microseconds) -> <<"microseconds">>;
datetime_field_str(millennium) -> <<"millennium">>;
datetime_field_str(milliseconds) -> <<"milliseconds">>;
datetime_field_str(minute) -> <<"minute">>;
datetime_field_str(month) -> <<"month">>;
datetime_field_str(quarter) -> <<"quarter">>;
datetime_field_str(second) -> <<"second">>;
datetime_field_str(timezone) -> <<"timezone">>;
datetime_field_str(timezone_hour) -> <<"timezone_hour">>;
datetime_field_str(timezone_minute) -> <<"timezone_minute">>;
datetime_field_str(week) -> <<"week">>;
datetime_field_str(year) -> <<"year">>.

-doc(#{group => <<"Date/time">>}).
-doc """
`date_trunc('field', source)` — truncate a timestamp to the specified
precision. Field is a closed enum (validated at build time).

See [Date/Time Functions](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC).
""".
-spec date_trunc(datetime_field(), expr()) -> qast:ast_node().
date_trunc(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    call("date_trunc", [qast:value(FieldBin, #{type => text}), Source], qast:opts(Source)).

-doc(#{group => <<"Date/time">>}).
-doc """
`EXTRACT(field FROM source)` — get a sub-field as `numeric`.

See [EXTRACT](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT).
""".
-spec extract(datetime_field(), expr()) -> qast:ast_node().
extract(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    qast:exp([
        qast:raw("extract("),
        qast:raw(FieldBin),
        qast:raw(" from "),
        Source,
        qast:raw(")")
    ], #{type => numeric}).

-doc(#{group => <<"Date/time">>}).
-doc "`date_part('field', source)` — same as `extract`, returns `double precision`.".
-spec date_part(datetime_field(), expr()) -> qast:ast_node().
date_part(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    call("date_part", [qast:value(FieldBin, #{type => text}), Source], #{type => float8}).

-doc(#{group => <<"Date/time">>}).
-doc "`age(t)` — interval since `current_date` to `t` (or vice-versa).".
-spec age(expr()) -> qast:ast_node().
age(A) ->
    call("age", [A], #{type => interval}).

-doc(#{group => <<"Date/time">>}).
-doc "`age(t1, t2)` — symbolic interval `t1 - t2`.".
-spec age(expr(), expr()) -> qast:ast_node().
age(A, B) ->
    call("age", [A, B], #{type => interval}).

-doc(#{group => <<"Date/time">>}).
-doc "`to_char(val, fmt)` — format a date/timestamp/number as text. See [Data Type Formatting Functions](https://www.postgresql.org/docs/current/functions-formatting.html).".
-spec to_char(expr(), expr()) -> qast:ast_node().
to_char(A, Fmt) ->
    call("to_char", [A, Fmt], #{type => text}).

-doc(#{group => <<"Date/time">>}).
-doc "`to_date(text, fmt)` — parse a string into `date`.".
-spec to_date(expr(), expr()) -> qast:ast_node().
to_date(A, Fmt) ->
    call("to_date", [A, Fmt], #{type => date}).

-doc(#{group => <<"Date/time">>}).
-doc "`to_timestamp(epoch)` — Unix-epoch seconds to `timestamptz`.".
-spec to_timestamp(expr()) -> qast:ast_node().
to_timestamp(A) ->
    call("to_timestamp", [A], #{type => timestamptz}).

-doc(#{group => <<"Date/time">>}).
-doc "`to_timestamp(text, fmt)` — parse a formatted string into `timestamptz`.".
-spec to_timestamp(expr(), expr()) -> qast:ast_node().
to_timestamp(A, Fmt) ->
    call("to_timestamp", [A, Fmt], #{type => timestamptz}).

%% = String functions ==========================================================

-doc(#{group => <<"Strings">>}).
-doc "`concat(a, b, c, ...)` — concatenate. **NULLs are skipped** (unlike `||`). See [String Functions](https://www.postgresql.org/docs/current/functions-string.html).".
-spec concat([expr(), ...]) -> qast:ast_node().
concat(List) when is_list(List) ->
    call("concat", List, #{type => text}).

-doc(#{group => <<"Strings">>}).
-doc "`concat(a, b)` — 2-arg form for convenience.".
-spec concat(expr(), expr()) -> qast:ast_node().
concat(A, B) ->
    call("concat", [A, B], #{type => text}).

-doc(#{group => <<"Strings">>}).
-doc "`length(s)` — string length in characters.".
-spec length(expr()) -> qast:ast_node().
length(A) ->
    call("length", [A], #{type => integer}).

-doc(#{group => <<"Strings">>}).
-doc "`char_length(s)` — SQL-standard alias for `length`.".
-spec char_length(expr()) -> qast:ast_node().
char_length(A) ->
    call("char_length", [A], #{type => integer}).

-doc(#{group => <<"Strings">>}).
-doc "`lower(s)` — lowercase.".
-spec lower(expr()) -> qast:ast_node().
lower(A) ->
    call("lower", [A], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`upper(s)` — uppercase.".
-spec upper(expr()) -> qast:ast_node().
upper(A) ->
    call("upper", [A], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`trim(s)` — strip whitespace from both ends.".
-spec trim(expr()) -> qast:ast_node().
trim(A) ->
    call("trim", [A], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`trim(s, chars)` — strip any character listed in `chars` from both ends.".
-spec trim(expr(), expr()) -> qast:ast_node().
trim(A, Chars) ->
    call("trim", [A, Chars], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`ltrim(s)` — strip whitespace from the left.".
-spec ltrim(expr()) -> qast:ast_node().
ltrim(A) ->
    call("ltrim", [A], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`ltrim(s, chars)` — strip listed characters from the left.".
-spec ltrim(expr(), expr()) -> qast:ast_node().
ltrim(A, Chars) ->
    call("ltrim", [A, Chars], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`rtrim(s)` — strip whitespace from the right.".
-spec rtrim(expr()) -> qast:ast_node().
rtrim(A) ->
    call("rtrim", [A], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`rtrim(s, chars)` — strip listed characters from the right.".
-spec rtrim(expr(), expr()) -> qast:ast_node().
rtrim(A, Chars) ->
    call("rtrim", [A, Chars], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`replace(s, from, to)` — replace all occurrences of `from` with `to`.".
-spec replace(expr(), expr(), expr()) -> qast:ast_node().
replace(A, From, To) ->
    call("replace", [A, From, To], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`split_part(s, sep, n)` — `n`-th field after splitting `s` by `sep` (1-indexed).".
-spec split_part(expr(), expr(), expr()) -> qast:ast_node().
split_part(A, Delim, N) ->
    call("split_part", [A, Delim, N], #{type => text}).

-doc(#{group => <<"Strings">>}).
-doc "`substring(s, from)` — from the given 1-based start position to end.".
-spec substring(expr(), expr()) -> qast:ast_node().
substring(A, From) ->
    call("substring", [A, From], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`substring(s, from, len)` — `len` characters starting at `from`.".
-spec substring(expr(), expr(), expr()) -> qast:ast_node().
substring(A, From, Len) ->
    call("substring", [A, From, Len], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`strpos(haystack, needle)` — 1-based position of `needle` in `haystack`, or 0.".
-spec strpos(expr(), expr()) -> qast:ast_node().
strpos(Haystack, Needle) ->
    call("strpos", [Haystack, Needle], #{type => integer}).

-doc(#{group => <<"Strings">>}).
-doc "`starts_with(s, prefix)` — boolean prefix test. Faster than `LIKE 'prefix%'` for indexed lookup.".
-spec starts_with(expr(), expr()) -> qast:ast_node().
starts_with(A, Prefix) ->
    call("starts_with", [A, Prefix], #{type => boolean}).

-doc(#{group => <<"Strings">>}).
-doc "`regexp_replace(s, pat, repl)` — replace first match. See [POSIX Regex Match](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP).".
-spec regexp_replace(expr(), expr(), expr()) -> qast:ast_node().
regexp_replace(A, Pattern, Repl) ->
    call("regexp_replace", [A, Pattern, Repl], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`regexp_replace(s, pat, repl, flags)` — `'g'` for global replace, `'i'` case-insensitive, etc.".
-spec regexp_replace(expr(), expr(), expr(), expr()) -> qast:ast_node().
regexp_replace(A, Pattern, Repl, Flags) ->
    call("regexp_replace", [A, Pattern, Repl, Flags], qast:opts(A)).

-doc(#{group => <<"Strings">>}).
-doc "`regexp_match(s, pat)` — returns a `text[]` of capture groups, or NULL if no match.".
-spec regexp_match(expr(), expr()) -> qast:ast_node().
regexp_match(A, Pattern) ->
    call("regexp_match", [A, Pattern], #{type => {array, text}}).

-doc(#{group => <<"Strings">>}).
-doc "`regexp_match(s, pat, flags)` — with regex flags.".
-spec regexp_match(expr(), expr(), expr()) -> qast:ast_node().
regexp_match(A, Pattern, Flags) ->
    call("regexp_match", [A, Pattern, Flags], #{type => {array, text}}).

%% = Type functions ============================================================

-doc(#{group => <<"Type casts">>}).
-doc """
`(Ast)::Type` — SQL type cast.

```erlang
pg_sql:as(Id, text)              %% (id)::text
pg_sql:as(V, {varchar, 60})      %% (v)::varchar(60)
pg_sql:as(V, {array, integer})   %% (v)::integer[]
```

See [Type Casts](https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS).

> #### Warning {: .warning}
> `Type` is rendered raw. Use only with literal atoms / tuples; never
> with user-controlled values.
""".
as(Ast, Type) ->
    Opts = qast:opts(Ast),
    qast:exp([
        qast:raw("("), Ast, qast:raw(")::"),
        qast:raw(type_str(Type))
    ], Opts#{type => Type}).

-doc(#{group => <<"Type casts">>}).
-doc """
Annotate an AST node with a type, **without** emitting an SQL cast.

Useful when you know the inferred type is wrong (e.g. when wrapping a
[`call/3`](`call/3`) to a function whose return type isn't auto-inferred)
and want correct downstream type propagation without paying for a
runtime cast.
""".
set_type(Ast, Type) ->
    Opts = qast:opts(Ast),
    qast:set_opts(Ast, Opts#{type => Type}).

type_str(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
type_str({array, Atom}) when is_atom(Atom) ->
    iolist_to_binary([type_str(Atom), "[]"]);
type_str({Type, Args}) when Type =/= array ->
    iolist_to_binary([
        to_iodata(Type),
        "(", join([to_iodata(A) || A <- Args], ","), ")"
    ]).

to_iodata(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_iodata(D) when is_list(D); is_binary(D) ->
    D;
to_iodata(Int) when is_integer(Int) ->
    integer_to_list(Int);
to_iodata(Float) when is_float(Float) ->
    io_lib:format("~p", [Float]).

join([], _) -> [];
join([H|T],Sep) -> [H|[[Sep,E]||E<-T]].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

type_str_test() ->
    ?assertEqual(<<"bigint">>, type_str(bigint)),
    ?assertEqual(<<"int[]">>, type_str({array, int})),
    ?assertEqual(<<"custom()">>, type_str({custom, []})),
    ?assertEqual(<<"custom(a,1,2.0)">>, type_str({custom, [<<"a">>, 1, 2.0]})).

-endif.
