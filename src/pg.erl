-module(pg).

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

         '+'/2,
         '-'/2,
         '*'/2,
         '/'/2
        ]).

-export([
         sum/1,
         count/1,
         min/1,
         max/1
        ]).

-export([
         min/2,
         max/2,
         row/1
        ]).

%% =============================================================================
%% Sql operations
%% =============================================================================

%% = Primitive =================================================================

%% @TODO wrap values in $value before validation and add $value match
'andalso'(true, B) -> B;
'andalso'(A, true) -> A;

'andalso'(false, _) -> false;
'andalso'(_, false) -> false;

'andalso'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" and "), B, qast:raw(")")], #{type => boolean}).

'orelse'(true, _) -> true;
'orelse'(_, true) -> true;
'orelse'(false, B) -> B;
'orelse'(A, false) -> A;
'orelse'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" or "), B, qast:raw(")")], #{type => boolean}).

'not'(A) when is_boolean(A) -> not A;
'not'(A) ->
    qast:exp([qast:raw("not "), A], #{type => boolean}).

'=:='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" = "), B, qast:raw(")")], #{type => boolean}).

'=/='(A, B) -> 'not'('=:='(A,B)).

'>'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" > "), B, qast:raw(")")], #{type => boolean}).
'>='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" >= "), B, qast:raw(")")], #{type => boolean}).
'<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" < "), B, qast:raw(")")], #{type => boolean}).
'=<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" <= "), B, qast:raw(")")], #{type => boolean}).

%% @TODO type opts
'+'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" + "), B, qast:raw(")")]).
'-'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" - "), B, qast:raw(")")]).
'*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" * "), B, qast:raw(")")]).
'/'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" / "), B, qast:raw(")")]).

%% = Aggregators ===============================================================

sum(Ast) ->
    qast:exp([qast:raw("sum("), Ast, qast:raw(")")], qast:opts(Ast)).

count(Ast) ->
    qast:exp([qast:raw("count("), Ast, qast:raw(")")], #{type => integer}).

min(A) ->
    qast:exp([qast:raw("min("), A, qast:raw(")")], qast:opts(A)).

max(A) ->
    qast:exp([qast:raw("max("), A, qast:raw(")")], qast:opts(A)).

%% = Math ======================================================================

min(A, B) ->
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

max(A, B) ->
    qast:exp([qast:raw("GREATEST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

row(Fields) when is_map(Fields) ->
    FieldsList = maps:to_list(Fields),
    Type = {record, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]},
    qast:exp([
        qast:raw("row("),
        qast:join([Node || {_F, Node} <- FieldsList], qast:raw(",")),
        qast:raw(")")
    ], #{type => Type}).
