-module(qjson).

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

'->'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" -> "), Name
    ], #{type => json}).

'->>'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" ->> "), Name
    ], #{type => text}).

'#>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field, qast:raw(" #> "), Path
    ], #{type => json}).

'#>>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field, qast:raw(" #>> "), Path
    ], #{type => text}).

'@>'(Field, Obj) ->
    qast:exp([
        Field, qast:raw(" @> "), Obj
    ], #{type => boolean}).

'<@'(Field, Obj) ->
    qast:exp([
        Field, qast:raw(" <@ "), Obj
    ], #{type => boolean}).

'?'(Field, Key) ->
    qast:exp([
        Field, qast:raw(" ? "), Key
    ], #{type => boolean}).

'?|'(Field, Keys) when is_list(Keys) ->
    qast:exp([
        Field, qast:raw(" ?| "), Keys
    ], #{type => boolean}).

'?&'(Field, Keys) ->
    qast:exp([
        Field, qast:raw(" ?& "), Keys
    ], #{type => boolean}).

%% = Builders ==================================================================

-spec json_build_object(map()) -> qast:ast_node().
json_build_object(Map) when is_map(Map) ->
    build_object("json_build_object", Map, json).

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

-spec json_build_array(list()) -> qast:ast_node().
json_build_array(Items) when is_list(Items) ->
    build_array("json_build_array", Items, json).

-spec jsonb_build_array(list()) -> qast:ast_node().
jsonb_build_array(Items) when is_list(Items) ->
    build_array("jsonb_build_array", Items, jsonb).

build_array(FnName, Items, ResultType) ->
    pg_sql:call(FnName, Items, #{type => ResultType}).

-spec to_json(qast:ast_node() | any()) -> qast:ast_node().
to_json(V) ->
    pg_sql:call("to_json", [V], #{type => json}).

-spec to_jsonb(qast:ast_node() | any()) -> qast:ast_node().
to_jsonb(V) ->
    pg_sql:call("to_jsonb", [V], #{type => jsonb}).

-spec row_to_json(qast:ast_node()) -> qast:ast_node().
row_to_json(V) ->
    pg_sql:call("row_to_json", [V], #{type => json}).

-spec array_to_json(qast:ast_node()) -> qast:ast_node().
array_to_json(V) ->
    pg_sql:call("array_to_json", [V], #{type => json}).

%% = Mutation ==================================================================

-spec jsonb_set(qast:ast_node(), [binary()], qast:ast_node() | any()) -> qast:ast_node().
jsonb_set(Target, Path, Value) ->
    set_call("jsonb_set", Target, Path, Value, []).

-spec jsonb_set(qast:ast_node(), [binary()], qast:ast_node() | any(), boolean())
    -> qast:ast_node().
jsonb_set(Target, Path, Value, CreateMissing) ->
    set_call("jsonb_set", Target, Path, Value, [CreateMissing]).

-spec jsonb_insert(qast:ast_node(), [binary()], qast:ast_node() | any())
    -> qast:ast_node().
jsonb_insert(Target, Path, Value) ->
    set_call("jsonb_insert", Target, Path, Value, []).

-spec jsonb_insert(qast:ast_node(), [binary()], qast:ast_node() | any(), boolean())
    -> qast:ast_node().
jsonb_insert(Target, Path, Value, InsertAfter) ->
    set_call("jsonb_insert", Target, Path, Value, [InsertAfter]).

set_call(FnName, Target, Path, Value, Extra) ->
    PathAst = qast:value(Path, #{type => {array, text}}),
    Args = [Target, PathAst, Value | Extra],
    pg_sql:call(FnName, Args, #{type => jsonb}).

-spec jsonb_strip_nulls(qast:ast_node()) -> qast:ast_node().
jsonb_strip_nulls(V) ->
    pg_sql:call("jsonb_strip_nulls", [V], #{type => jsonb}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
