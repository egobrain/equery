-module(qjson).

-export([
        '->'/2,
        '->>'/2,
        '#>'/2,
        '#>>'/2
       ]).

'->'(Field, Name) when is_binary(Name); is_atom(Name) ->
    qast:exp([
        Field,
        qast:raw("->"),
        qast:raw(
            equery_utils:string_wrap(
                equery_utils:to_binary(Name)))
    ], #{}).

'->>'(Field, Name) when is_binary(Name); is_atom(Name) ->
    qast:exp([
        Field,
        qast:raw("->>"),
        qast:raw(
            equery_utils:string_wrap(
                equery_utils:to_binary(Name)))
    ], #{type => text}).

'#>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field,
        qast:raw("#>"),
            braces_string_wrap(
                qast:join([
                    qast:raw(equery_utils:to_binary(P)) || P <- Path
                ], qast:raw(",")))
    ], #{}).

'#>>'(Field, Path) when is_list(Path) ->
    qast:exp([
        Field,
        qast:raw("#>>"),
            braces_string_wrap(
                qast:join([
                    qast:raw(equery_utils:to_binary(P)) || P <- Path
                ], qast:raw(",")))
    ], #{type => text}).

braces_string_wrap(Ast) ->
    qast:exp([
        qast:raw("'{"),
        Ast,
        qast:raw("}'")
    ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
