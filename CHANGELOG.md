# Changelog

## Unreleased

### Added

**Statement-level features**

- `q:having/1,2` — SQL `HAVING` clause; composes with `andalso` like `where`.
- `q:lateral_join/2,3,4` — `LATERAL` join; subquery closure receives outer data, supports custom `ON` condition.
- `q:order_by/2` — `NULLS FIRST` / `NULLS LAST` via 3-element tuple `{Field, asc|desc, nulls_first|nulls_last}`.
- Schema-qualified table names via optional `schema => binary()` field in schema map → emits `"schema"."table"`.

**Expression-level operators and predicates**

- `pg_sql:is_not_null/1`, `pg_sql:is_distinct_from/2`, `pg_sql:is_not_distinct_from/2`.
- `pg_sql:case_when/1,2` — `CASE WHEN ... THEN ... [ELSE ...] END`.
- `pg_sql:'||'/2` — NULL-propagating string/array concatenation.

**Numeric functions**

- `pg_sql:mod/2`, `pg_sql:div/2`, `pg_sql:rem/2` (Erlang `rem` operator works inside DSL closures).
- `pg_sql:round/1,2`, `pg_sql:ceil/1`, `pg_sql:floor/1`.
- `pg_sql:power/2`, `pg_sql:sqrt/1`, `pg_sql:ln/1`, `pg_sql:log/1,2`, `pg_sql:exp/1`, `pg_sql:sign/1`, `pg_sql:random/0`.
- `pg_sql:greatest/1`, `pg_sql:least/1` — N-ary forms.

**String functions**

- `pg_sql:concat/1,2`, `pg_sql:length/1`, `pg_sql:char_length/1`.
- `pg_sql:lower/1`, `pg_sql:upper/1`.
- `pg_sql:trim/1,2`, `pg_sql:ltrim/1,2`, `pg_sql:rtrim/1,2`.
- `pg_sql:replace/3`, `pg_sql:split_part/3`, `pg_sql:substring/2,3`.
- `pg_sql:strpos/2`, `pg_sql:starts_with/2`.
- `pg_sql:regexp_replace/3,4`, `pg_sql:regexp_match/2,3`.

**Date/time functions**

- `pg_sql:now/0`, `pg_sql:current_timestamp/0`, `pg_sql:current_date/0`, `pg_sql:current_time/0`.
- `pg_sql:date_trunc/2`, `pg_sql:extract/2`, `pg_sql:date_part/2` — field argument restricted to a closed `datetime_field()` enum (prevents SQL injection via field names).
- `pg_sql:age/1,2`, `pg_sql:to_char/2`, `pg_sql:to_date/2`, `pg_sql:to_timestamp/1,2`.

**Aggregates**

- `pg_sql:avg/1`, `pg_sql:bool_and/1`, `pg_sql:bool_or/1`, `pg_sql:every/1`.
- `pg_sql:string_agg/2,3`, `pg_sql:array_agg/2`.
- `pg_sql:json_agg/1,2`, `pg_sql:jsonb_agg/1,2`, `pg_sql:json_object_agg/2`, `pg_sql:jsonb_object_agg/2`.
- `pg_sql:percentile_cont/2`, `pg_sql:percentile_disc/2`, `pg_sql:mode/1`.
- `pg_sql:filter/2` — wraps any aggregate with `FILTER (WHERE ...)`.
- ORDER BY inside `string_agg`/`array_agg`/`json_agg`/`jsonb_agg` via `agg_order_spec()`.

**Arrays**

- `pg_sql:array/1` — `ARRAY[...]` constructor.
- `pg_sql:'<@'/2`, `pg_sql:'&&'/2` — containment and overlap.
- `pg_sql:array_length/1,2`, `pg_sql:array_position/2`.
- `pg_sql:array_append/2`, `pg_sql:array_prepend/2`, `pg_sql:array_remove/2`, `pg_sql:array_replace/3`, `pg_sql:array_cat/2`.
- `pg_sql:unnest/1` — set-returning function; integrates with `q:from/1` (uses `unnest` as default column name to match PG convention).

**JSON / JSONB**

- `qjson:json_build_object/1`, `qjson:jsonb_build_object/1` — accept Erlang map.
- `qjson:json_build_array/1`, `qjson:jsonb_build_array/1`.
- `qjson:to_json/1`, `qjson:to_jsonb/1`, `qjson:row_to_json/1`, `qjson:array_to_json/1`.
- `qjson:jsonb_set/3,4`, `qjson:jsonb_insert/3,4`, `qjson:jsonb_strip_nulls/1`.

### Changed

- Atom → binary conversions now use UTF-8 instead of latin1 in `equery_utils:to_binary/1` and `pg_sql` type-name rendering. Atoms with non-ASCII characters no longer lose data.
- `pg_sql:value()` type renamed to `pg_sql:expr()` to disambiguate from `qast:value()` (the AST node).
- `equery_utils:order_item_exp/1` — shared helper used by both top-level `ORDER BY` and aggregate-internal ordering.

### Fixed

- `real_table()` type updated from `{real, iolist(), reference()}` to `{real, binary() | {binary(), binary()}, reference()}` to match actual usage (schema-qualified names broke the old type).
- Type literals in `pg_sql:array_length/1` and `qjson:jsonb_set/4`/`jsonb_insert/4` wrapped with `qast:value/2` to satisfy success typing.
