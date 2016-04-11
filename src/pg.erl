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

-type value() :: qast:ast_node() | any().

%% = Primitive =================================================================

%% @TODO wrap values in $value before validation and add $value match
-spec 'andalso'(V, V) -> V when V :: boolean() | qast:ast_node().
'andalso'(true, B) -> B;
'andalso'(A, true) -> A;

'andalso'(false, _) -> false;
'andalso'(_, false) -> false;

'andalso'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" and "), B, qast:raw(")")], #{type => boolean}).

-spec 'orelse'(V, V) -> V when V :: boolean() | qast:ast_node().
'orelse'(true, _) -> true;
'orelse'(_, true) -> true;
'orelse'(false, B) -> B;
'orelse'(A, false) -> A;
'orelse'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" or "), B, qast:raw(")")], #{type => boolean}).

-spec 'not'(V) -> V when V :: boolean() | qast:ast_node().
'not'(A) when is_boolean(A) -> not A;
'not'(A) ->
    qast:exp([qast:raw("not "), A], #{type => boolean}).

-spec '=:='(value(), value()) -> qast:ast_node().
'=:='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" = "), B, qast:raw(")")], #{type => boolean}).

-spec '=/='(value(), value()) -> qast:ast_node().
'=/='(A, B) -> 'not'('=:='(A,B)).

-spec '>'(value(), value()) -> qast:ast_node().
'>'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" > "), B, qast:raw(")")], #{type => boolean}).
-spec '>='(value(), value()) -> qast:ast_node().
'>='(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" >= "), B, qast:raw(")")], #{type => boolean}).
-spec '<'(value(), value()) -> qast:ast_node().
'<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" < "), B, qast:raw(")")], #{type => boolean}).
-spec '=<'(value(), value()) -> qast:ast_node().
'=<'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" <= "), B, qast:raw(")")], #{type => boolean}).

%% @TODO type opts
-spec '+'(value(), value()) -> qast:ast_node().
'+'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" + "), B, qast:raw(")")]).
-spec '-'(value(), value()) -> qast:ast_node().
'-'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" - "), B, qast:raw(")")]).
-spec '*'(value(), value()) -> qast:ast_node().
'*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" * "), B, qast:raw(")")]).
-spec '/'(value(), value()) -> qast:ast_node().
'/'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" / "), B, qast:raw(")")]).

%% = Aggregators ===============================================================

-spec sum(qast:ast_node()) -> qast:ast_node().
sum(Ast) ->
    qast:exp([qast:raw("sum("), Ast, qast:raw(")")], qast:opts(Ast)).

-spec count(qast:ast_node()) -> qast:ast_node().
count(Ast) ->
    qast:exp([qast:raw("count("), Ast, qast:raw(")")], #{type => integer}).

-spec 'min'(value()) -> qast:ast_node().
min(A) ->
    qast:exp([qast:raw("min("), A, qast:raw(")")], qast:opts(A)).

-spec 'max'(value()) -> qast:ast_node().
max(A) ->
    qast:exp([qast:raw("max("), A, qast:raw(")")], qast:opts(A)).

%% = Math ======================================================================

-spec 'min'(value(), value()) -> qast:ast_node().
min(A, B) ->
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-spec 'max'(value(), value()) -> qast:ast_node().
max(A, B) ->
    qast:exp([qast:raw("GREATEST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-spec row(#{atom() => qast:ast_node()}) -> qast:ast_node().
row(Fields) when is_map(Fields) ->
    FieldsList = maps:to_list(Fields),
    Type = {record, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]},
    qast:exp([
        qast:raw("row("),
        qast:join([Node || {_F, Node} <- FieldsList], qast:raw(",")),
        qast:raw(")")
    ], #{type => Type}).
