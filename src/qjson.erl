-module(qjson).
-moduledoc """
JSON / JSONB operators, builders, and mutation.

Three families:

- **Access operators** — `->`, `->>`, `#>`, `#>>` (PG's JSON path
  operators).
- **Predicates** — `@>`, `<@`, `?`, `?|`, `?&` (containment and key
  existence; same operator atoms as their array siblings in
  [`pg_sql`](`m:pg_sql`)).
- **Builders & mutation** — `jsonb_build_object/1` (takes an Erlang
  map), `jsonb_build_array/1`, `to_jsonb/1`, `row_to_json/1`,
  `array_to_json/1`, `jsonb_set/3,4`, `jsonb_insert/3,4`,
  `jsonb_strip_nulls/1`.

```erlang
q:select(fun([#{id := Id, name := N}]) ->
    #{obj => qjson:jsonb_build_object(#{
        id   => Id,
        name => N,
        flag => Id > 10
    })}
end).
```
""".

-include("cth.hrl").

-export([
        '->'/2,
        '->>'/2,
        '#>'/2,
        '#>>'/2,

        '@>'/2,
        '<@'/2,
        '?'/2,
        '?|'/2,
        '?&'/2
       ]).

%% Builders
-export([
         json_build_object/1,
         jsonb_build_object/1,
         json_build_array/1,
         jsonb_build_array/1,
         to_json/1,
         to_jsonb/1,
         row_to_json/1,
         array_to_json/1
        ]).

%% Mutation
-export([
         jsonb_set/3, jsonb_set/4,
         jsonb_insert/3, jsonb_insert/4,
         jsonb_strip_nulls/1
        ]).

-doc(#{group => <<"Access">>}).
-doc "`json -> key` — get JSON field as `json`/`jsonb`. See [JSON Operators](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE).".
'->'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" -> "), Name
    ], #{type => json}).

-doc(#{group => <<"Access">>}).
-doc "`json ->> key` — get JSON field as `text`.".
'->>'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" ->> "), Name
    ], #{type => text}).

-doc(#{group => <<"Access">>}).
-doc "`json #> path` — extract by path (list of keys/indexes), as `json`/`jsonb`.".
'#>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field, qast:raw(" #> "), Path
    ], #{type => json}).

-doc(#{group => <<"Access">>}).
-doc "`json #>> path` — extract by path, as `text`.".
'#>>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field, qast:raw(" #>> "), Path
    ], #{type => text}).

-doc(#{group => <<"Predicates">>}).
-doc "`jsonb @> obj` — contains. See [JSONB Containment](https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT).".
'@>'(Field, Obj) ->
    qast:exp([
        Field, qast:raw(" @> "), Obj
    ], #{type => boolean}).

-doc(#{group => <<"Predicates">>}).
-doc "`jsonb <@ obj` — is contained by.".
'<@'(Field, Obj) ->
    qast:exp([
        Field, qast:raw(" <@ "), Obj
    ], #{type => boolean}).

-doc(#{group => <<"Predicates">>}).
-doc "`jsonb ? key` — does the object/array contain the key (or text element)?".
'?'(Field, Key) ->
    qast:exp([
        Field, qast:raw(" ? "), Key
    ], #{type => boolean}).

-doc(#{group => <<"Predicates">>}).
-doc "`jsonb ?| keys` — does it contain **any** of the given keys?".
'?|'(Field, Keys) when is_list(Keys) ->
    qast:exp([
        Field, qast:raw(" ?| "), Keys
    ], #{type => boolean}).

-doc(#{group => <<"Predicates">>}).
-doc "`jsonb ?& keys` — does it contain **all** of the given keys?".
'?&'(Field, Keys) ->
    qast:exp([
        Field, qast:raw(" ?& "), Keys
    ], #{type => boolean}).

%% = Builders ==================================================================

-doc(#{group => <<"Builders">>}).
-doc """
`json_build_object(k1, v1, k2, v2, ...)` from an Erlang map.

Keys (atoms or binaries) become text literals; values are arbitrary
AST. See [Builder Functions](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-CREATION-TABLE).

```erlang
qjson:json_build_object(#{
    id => Id,
    name => Name,
    flag => Id > 10
}).
```
""".
-spec json_build_object(map()) -> qast:ast_node().
json_build_object(Map) when is_map(Map) ->
    build_object("json_build_object", Map, json).

-doc(#{group => <<"Builders">>}).
-doc "`jsonb_build_object(...)` — binary JSON variant of [`json_build_object/1`](`json_build_object/1`).".
-spec jsonb_build_object(map()) -> qast:ast_node().
jsonb_build_object(Map) when is_map(Map) ->
    build_object("jsonb_build_object", Map, jsonb).

build_object(FnName, Map, ResultType) ->
    Args = lists:append([[key_node(K), V] || {K, V} <- ?MAPS_TO_LIST(Map)]),
    pg_sql:call(FnName, Args, #{type => ResultType}).

key_node(K) when is_atom(K) ->
    qast:value(atom_to_binary(K, utf8), #{type => text});
key_node(K) when is_binary(K) ->
    qast:value(K, #{type => text}).

-doc(#{group => <<"Builders">>}).
-doc "`json_build_array(v1, v2, ...)` — build a JSON array from a list of values.".
-spec json_build_array(list()) -> qast:ast_node().
json_build_array(Items) when is_list(Items) ->
    build_array("json_build_array", Items, json).

-doc(#{group => <<"Builders">>}).
-doc "`jsonb_build_array(...)`.".
-spec jsonb_build_array(list()) -> qast:ast_node().
jsonb_build_array(Items) when is_list(Items) ->
    build_array("jsonb_build_array", Items, jsonb).

build_array(FnName, Items, ResultType) ->
    pg_sql:call(FnName, Items, #{type => ResultType}).

-doc(#{group => <<"Builders">>}).
-doc "`to_json(value)` — convert any SQL value to `json`. Records become objects, arrays become JSON arrays.".
-spec to_json(qast:ast_node() | any()) -> qast:ast_node().
to_json(V) ->
    pg_sql:call("to_json", [V], #{type => json}).

-doc(#{group => <<"Builders">>}).
-doc "`to_jsonb(value)` — same as `to_json`, returns `jsonb`.".
-spec to_jsonb(qast:ast_node() | any()) -> qast:ast_node().
to_jsonb(V) ->
    pg_sql:call("to_jsonb", [V], #{type => jsonb}).

-doc(#{group => <<"Builders">>}).
-doc "`row_to_json(record)` — convert a row/record to a JSON object with column names as keys.".
-spec row_to_json(qast:ast_node()) -> qast:ast_node().
row_to_json(V) ->
    pg_sql:call("row_to_json", [V], #{type => json}).

-doc(#{group => <<"Builders">>}).
-doc "`array_to_json(arr)` — convert an SQL array to a JSON array.".
-spec array_to_json(qast:ast_node()) -> qast:ast_node().
array_to_json(V) ->
    pg_sql:call("array_to_json", [V], #{type => json}).

%% = Mutation ==================================================================

-doc(#{group => <<"Mutation">>}).
-doc """
`jsonb_set(target, path, new_value)` — set value at path. Path is an
Erlang list of binary keys (auto-converted to `text[]`).

```erlang
qjson:jsonb_set(Data, [<<"prefs">>, <<"theme">>], NewTheme).
```

See [JSON Processing Functions](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING-TABLE).
""".
-spec jsonb_set(qast:ast_node(), [binary()], qast:ast_node() | any()) -> qast:ast_node().
jsonb_set(Target, Path, Value) ->
    set_call("jsonb_set", Target, Path, Value, []).

-doc(#{group => <<"Mutation">>}).
-doc "`jsonb_set(target, path, new_value, create_missing)` — when `create_missing` is `true`, creates the path if it does not exist.".
-spec jsonb_set(qast:ast_node(), [binary()], qast:ast_node() | any(), boolean())
    -> qast:ast_node().
jsonb_set(Target, Path, Value, CreateMissing) ->
    set_call("jsonb_set", Target, Path, Value,
             [qast:value(CreateMissing, #{type => boolean})]).

-doc(#{group => <<"Mutation">>}).
-doc "`jsonb_insert(target, path, new_value)` — insert before the position pointed to by `path`.".
-spec jsonb_insert(qast:ast_node(), [binary()], qast:ast_node() | any())
    -> qast:ast_node().
jsonb_insert(Target, Path, Value) ->
    set_call("jsonb_insert", Target, Path, Value, []).

-doc(#{group => <<"Mutation">>}).
-doc "`jsonb_insert(target, path, new_value, insert_after)` — when `insert_after` is `true`, insert **after** the path.".
-spec jsonb_insert(qast:ast_node(), [binary()], qast:ast_node() | any(), boolean())
    -> qast:ast_node().
jsonb_insert(Target, Path, Value, InsertAfter) ->
    set_call("jsonb_insert", Target, Path, Value,
             [qast:value(InsertAfter, #{type => boolean})]).

set_call(FnName, Target, Path, Value, Extra) ->
    PathAst = qast:value(Path, #{type => {array, text}}),
    Args = [Target, PathAst, Value | Extra],
    pg_sql:call(FnName, Args, #{type => jsonb}).

-doc(#{group => <<"Mutation">>}).
-doc "`jsonb_strip_nulls(target)` — recursively remove object fields with `null` values.".
-spec jsonb_strip_nulls(qast:ast_node()) -> qast:ast_node().
jsonb_strip_nulls(V) ->
    pg_sql:call("jsonb_strip_nulls", [V], #{type => jsonb}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
