-module(qjson).

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

'->'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" -> "), Name
    ], #{}).

'->>'(Field, Name) ->
    qast:exp([
        Field, qast:raw(" ->> "), Name
    ], #{type => text}).

'#>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field, qast:raw(" #> "), Path
    ], #{}).

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
