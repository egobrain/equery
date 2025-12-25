-module(pg_sql).

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

         '+'/2,
         '-'/2,
         '*'/2,
         '/'/2,
         'abs'/1
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
         array_agg/1,
         trunc/2
        ]).

-export([
         min/2,
         max/2,
         row/1,
         row/2
        ]).

-export([
         coalesce/1,
         in/2,
         exists/1
        ]).

%% Array functions
-export([
         '@>'/2
        ]).

%% Type function
-export([
         as/2,
         set_type/2
        ]).

%% =============================================================================
%% Sql operations
%% =============================================================================

-type value() :: qast:ast_node() | any().

%% = Primitive =================================================================

%% @TODO wrap values in $value before validation and add $value match
-spec 'andalso'(V, V) -> V when V :: boolean() | qast:ast_node().
'andalso'(true, B) -> maybe_inherit_type(B, boolean);
'andalso'(A, true) -> maybe_inherit_type(A, boolean);

'andalso'(false, _) -> false;
'andalso'(_, false) -> false;

'andalso'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" and "), B, qast:raw(")")], #{type => boolean}).

-spec 'orelse'(V, V) -> V when V :: boolean() | qast:ast_node().
'orelse'(true, _) -> true;
'orelse'(_, true) -> true;
'orelse'(false, B) -> maybe_inherit_type(B, boolean);
'orelse'(A, false) -> maybe_inherit_type(A, boolean);
'orelse'(A0, B0) ->
    A = maybe_inherit_type(A0, boolean),
    B = maybe_inherit_type(B0, boolean),
    qast:exp([qast:raw("("), A, qast:raw(" or "), B, qast:raw(")")], #{type => boolean}).

-spec 'not'(V) -> V when V :: boolean() | qast:ast_node().
'not'(A) when is_boolean(A) -> not A;
'not'(A0) ->
    A = maybe_inherit_type(A0, boolean),
    qast:exp([qast:raw("not "), A], #{type => boolean}).

-spec '=:='(value(), value()) -> qast:ast_node().
'=:='(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" = "), B, qast:raw(")")], #{type => boolean}).

-spec '=/='(value(), value()) -> qast:ast_node().
'=/='(A, B) -> 'not'('=:='(A,B)).

-spec '>'(value(), value()) -> qast:ast_node().
'>'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" > "), B, qast:raw(")")], #{type => boolean}).
-spec '>='(value(), value()) -> qast:ast_node().
'>='(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" >= "), B, qast:raw(")")], #{type => boolean}).
-spec '<'(value(), value()) -> qast:ast_node().
'<'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" < "), B, qast:raw(")")], #{type => boolean}).
-spec '=<'(value(), value()) -> qast:ast_node().
'=<'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" <= "), B, qast:raw(")")], #{type => boolean}).

-spec 'is'(value(), value()) -> qast:ast_node().
is(A, B) ->
    qast:exp([A, qast:raw(" is "), B], #{type => boolean}).

-spec 'is_null'(value()) -> qast:ast_node().
is_null(A) ->
    is(A, qast:raw("null")).

%% @TODO type opts
-spec '+'(value(), value()) -> qast:ast_node().
'+'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" + "), B, qast:raw(")")]).
-spec '-'(value(), value()) -> qast:ast_node().
'-'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" - "), B, qast:raw(")")]).
-spec '*'(value(), value()) -> qast:ast_node().
'*'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" * "), B, qast:raw(")")]).
-spec '/'(value(), value()) -> qast:ast_node().
'/'(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("("), A, qast:raw(" / "), B, qast:raw(")")]).

-spec 'abs'(value()) -> qast:ast_node().
abs(A) ->
    qast:exp([qast:raw("abs("), A, qast:raw(")")], qast:opts(A)).

%% = LIKE ======================================================================

-spec '~'(value(), value()) -> qast:ast_node().
'~'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" ~ "), B, qast:raw(")")], #{type => boolean}).
-spec '~*'(value(), value()) -> qast:ast_node().
'~*'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" ~* "), B, qast:raw(")")], #{type => boolean}).
like(A, B) ->
    qast:exp([A, qast:raw(" like "), B], #{type => boolean}).
ilike(A, B) ->
    qast:exp([A, qast:raw(" ilike "), B], #{type => boolean}).

%% = Aggregators ===============================================================

-spec sum(qast:ast_node()) -> qast:ast_node().
sum(Ast) ->
    call("sum", [Ast], qast:opts(Ast)).

-spec count(qast:ast_node()) -> qast:ast_node().
count(Ast) ->
    call("count", [Ast], #{type => integer}).

-spec 'min'(value()) -> qast:ast_node().
min(Ast) ->
    call("min", [Ast], qast:opts(Ast)).

-spec 'max'(value()) -> qast:ast_node().
max(Ast) ->
    call("max", [Ast], qast:opts(Ast)).

-spec 'distinct'(value()) -> qast:ast_node().
distinct(Ast) ->
    call("distinct ", [Ast], qast:opts(Ast)).

-spec 'array_agg'(value()) -> qast:ast_node().
array_agg(Ast) ->
    Opts = qast:opts(Ast),
    Type = maps:get(type, Opts, undefined),
    NewOpts = Opts#{type => {array, Type}},
    call("array_agg", [Ast], NewOpts).

-spec 'trunc'(value(), qast:ast_node() | non_neg_integer()) -> qast:ast_node().
'trunc'(V, N) ->
    call("trunc", [V, N], qast:opts(V)).

%% = Math ======================================================================

-spec 'min'(value(), value()) -> qast:ast_node().
min(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-spec 'max'(value(), value()) -> qast:ast_node().
max(A0, B0) ->
    {A, B} = same_type(A0, B0),
    qast:exp([qast:raw("GREATEST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

%% = Additional operations =====================================================

-spec row(#{atom() => qast:ast_node()}) -> qast:ast_node().
row(Fields) when is_map(Fields) ->
    row(undefined, Fields).

-spec row(Model :: module(), #{atom() => qast:ast_node()}) -> qast:ast_node().
row(Model, Fields) when is_map(Fields) ->
    FieldsList = ?MAPS_TO_LIST(Fields),
    Type = {record, {model, Model, [{F, qast:opts(Node)} || {F, Node} <- FieldsList]}},
    qast:exp([
        qast:raw("row("),
        qast:join([Node || {_F, Node} <- FieldsList], qast:raw(",")),
        qast:raw(")")
    ], #{type => Type}).

coalesce([H|_]=List) ->
    qast:exp([
        qast:raw("coalesce("),
        qast:join([Node || Node <- List], qast:raw(",")),
        qast:raw(")")
    ], maps:with([type], qast:opts(H))).

in(A0, #query{}=Q) ->
    Select = qsql:select(Q),
    A = array_item_type(A0, Select),
    qast:exp([A, qast:raw(" in ("), Select, qast:raw(")")], #{type => boolean});
in(A, [Item]) ->
    '=:='(A, Item);
in(A0, B0) ->
    {A, B} = array_in_type(A0, B0),
    qast:exp([A, qast:raw(" = ANY("), B, qast:raw(")")], #{type => boolean}).

exists(#query{}=Q) ->
    qast:exp([qast:raw("exists ("), qsql:select(Q), qast:raw(")")], #{type => boolean}).

-spec call(iodata(), [value()], qast:opts()) -> qast:ast_node().
call(FunName, Args, Opts) ->
    qast:exp([
        qast:raw([FunName, "("]),
        qast:join(Args, qast:raw(",")),
        qast:raw(")")
    ], Opts).

%% = Array oprterations ========================================================

'@>'(A, B) ->
    qast:exp([A, qast:raw(" @> "), B], #{type => boolean}).

%% = Type functions ============================================================

as(Ast, Type) ->
    Opts = qast:opts(Ast),
    qast:exp([
        qast:raw("("), Ast, qast:raw(")::"),
        qast:raw(type_str(Type))
    ], Opts#{type => Type}).

set_type(Ast, Type) ->
    Opts = qast:opts(Ast),
    qast:set_opts(Ast, Opts#{type => Type}).

same_type(A, B) ->
    case {qast:is_ast(A), qast:is_ast(B)} of
        {true, false} -> {A, inherit_type(B, A)};
        {false, true} -> {inherit_type(A, B), B};
        _ -> {A, B}
    end.

array_item_type(A, ArrayAst) ->
    case qast:is_ast(A) of
        true -> A;
        false ->
            case qast:opts(ArrayAst) of
                #{type := {array, T}} -> qast:value(A, #{type => T});
                _ -> A
            end
    end.

array_in_type(Item, Array) ->
    case {qast:is_ast(Item), qast:is_ast(Array)} of
        {true, false} ->
            case qast:opts(Item) of
                #{type := T} ->
                    {Item, qast:value(Array, #{type => {array, T}})};
                _ ->
                    {Item, Array}
            end;
        {false, true} ->
            case qast:opts(Array) of
                #{type := {array, T}} ->
                    {qast:value(Item, #{type => T}), Array};
                _ ->
                    {Item, Array}
            end;
        _ ->
            {Item, Array}
    end.

maybe_inherit_type(A, Type) ->
    case qast:is_ast(A) of
        true -> A;
        false -> qast:value(A, #{type => Type})
    end.

inherit_type(To, From) ->
    TypeOpts = maps:with([type], qast:opts(From)),
    qast:value(To, TypeOpts).

type_str(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, latin1);
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
