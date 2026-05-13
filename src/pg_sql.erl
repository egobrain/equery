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
         case_when/1,
         case_when/2,
         in/2,
         exists/1
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

-spec 'is'(value(), value()) -> qast:ast_node().
is(A, B) ->
    qast:exp([A, qast:raw(" is "), B], #{type => boolean}).

-spec 'is_null'(value()) -> qast:ast_node().
is_null(A) ->
    is(A, qast:raw("null")).

-spec 'is_not_null'(value()) -> qast:ast_node().
is_not_null(A) ->
    qast:exp([A, qast:raw(" is not null")], #{type => boolean}).

-spec is_distinct_from(value(), value()) -> qast:ast_node().
is_distinct_from(A, B) ->
    qast:exp([A, qast:raw(" is distinct from "), B], #{type => boolean}).

-spec is_not_distinct_from(value(), value()) -> qast:ast_node().
is_not_distinct_from(A, B) ->
    qast:exp([A, qast:raw(" is not distinct from "), B], #{type => boolean}).

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

-spec 'abs'(value()) -> qast:ast_node().
abs(A) ->
    qast:exp([qast:raw("abs("), A, qast:raw(")")], qast:opts(A)).

-spec 'div'(value(), value()) -> qast:ast_node().
'div'(A, B) ->
    call("div", [A, B], qast:opts(A)).

-spec 'rem'(value(), value()) -> qast:ast_node().
'rem'(A, B) -> mod(A, B).

%% = Numeric functions =========================================================

-spec mod(value(), value()) -> qast:ast_node().
mod(A, B) ->
    call("mod", [A, B], qast:opts(A)).

-spec round(value()) -> qast:ast_node().
round(A) ->
    call("round", [A], qast:opts(A)).

-spec round(value(), value()) -> qast:ast_node().
round(A, N) ->
    call("round", [A, N], qast:opts(A)).

-spec ceil(value()) -> qast:ast_node().
ceil(A) ->
    call("ceil", [A], qast:opts(A)).

-spec floor(value()) -> qast:ast_node().
floor(A) ->
    call("floor", [A], qast:opts(A)).

-spec power(value(), value()) -> qast:ast_node().
power(A, B) ->
    call("power", [A, B], qast:opts(A)).

-spec sqrt(value()) -> qast:ast_node().
sqrt(A) ->
    call("sqrt", [A], qast:opts(A)).

-spec ln(value()) -> qast:ast_node().
ln(A) ->
    call("ln", [A], qast:opts(A)).

-spec log(value()) -> qast:ast_node().
log(A) ->
    call("log", [A], qast:opts(A)).

-spec log(value(), value()) -> qast:ast_node().
log(B, A) ->
    call("log", [B, A], qast:opts(A)).

-spec exp(value()) -> qast:ast_node().
exp(A) ->
    call("exp", [A], qast:opts(A)).

-spec sign(value()) -> qast:ast_node().
sign(A) ->
    call("sign", [A], qast:opts(A)).

-spec random() -> qast:ast_node().
random() ->
    call("random", [], #{}).

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
min(A, B) ->
    qast:exp([qast:raw("LEAST("), A, qast:raw(","), B, qast:raw(")")], qast:opts(A)).

-spec 'max'(value(), value()) -> qast:ast_node().
max(A, B) ->
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

-spec case_when([{value(), value()}, ...]) -> qast:ast_node().
case_when(Whens) ->
    case_when_(Whens, undefined).

-spec case_when([{value(), value()}, ...], value()) -> qast:ast_node().
case_when(Whens, Else) ->
    case_when_(Whens, {else, Else}).

case_when_([{_, FirstThen}|_]=Whens, ElseSpec) ->
    Opts = maps:with([type], qast:opts(FirstThen)),
    WhenExps = lists:map(fun({W, T}) ->
        qast:exp([qast:raw(" when "), W, qast:raw(" then "), T])
    end, Whens),
    ElseExp = case ElseSpec of
        undefined -> qast:raw("");
        {else, E} -> qast:exp([qast:raw(" else "), E])
    end,
    qast:exp([
        qast:raw("case"),
        qast:exp(WhenExps),
        ElseExp,
        qast:raw(" end")
    ], Opts).

in(A, #query{}=Q) ->
    qast:exp([A, qast:raw(" in ("), qsql:select(Q), qast:raw(")")], #{type => boolean});
in(A, [Item]) ->
    '=:='(A, Item);
in(A, B) ->
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

%% = String functions ==========================================================

-spec concat([value(), ...]) -> qast:ast_node().
concat(List) when is_list(List) ->
    call("concat", List, #{type => text}).

-spec concat(value(), value()) -> qast:ast_node().
concat(A, B) ->
    call("concat", [A, B], #{type => text}).

-spec length(value()) -> qast:ast_node().
length(A) ->
    call("length", [A], #{type => integer}).

-spec char_length(value()) -> qast:ast_node().
char_length(A) ->
    call("char_length", [A], #{type => integer}).

-spec lower(value()) -> qast:ast_node().
lower(A) ->
    call("lower", [A], qast:opts(A)).

-spec upper(value()) -> qast:ast_node().
upper(A) ->
    call("upper", [A], qast:opts(A)).

-spec trim(value()) -> qast:ast_node().
trim(A) ->
    call("trim", [A], qast:opts(A)).

-spec trim(value(), value()) -> qast:ast_node().
trim(A, Chars) ->
    call("trim", [A, Chars], qast:opts(A)).

-spec ltrim(value()) -> qast:ast_node().
ltrim(A) ->
    call("ltrim", [A], qast:opts(A)).

-spec ltrim(value(), value()) -> qast:ast_node().
ltrim(A, Chars) ->
    call("ltrim", [A, Chars], qast:opts(A)).

-spec rtrim(value()) -> qast:ast_node().
rtrim(A) ->
    call("rtrim", [A], qast:opts(A)).

-spec rtrim(value(), value()) -> qast:ast_node().
rtrim(A, Chars) ->
    call("rtrim", [A, Chars], qast:opts(A)).

-spec replace(value(), value(), value()) -> qast:ast_node().
replace(A, From, To) ->
    call("replace", [A, From, To], qast:opts(A)).

-spec split_part(value(), value(), value()) -> qast:ast_node().
split_part(A, Delim, N) ->
    call("split_part", [A, Delim, N], #{type => text}).

-spec substring(value(), value()) -> qast:ast_node().
substring(A, From) ->
    call("substring", [A, From], qast:opts(A)).

-spec substring(value(), value(), value()) -> qast:ast_node().
substring(A, From, Len) ->
    call("substring", [A, From, Len], qast:opts(A)).

-spec strpos(value(), value()) -> qast:ast_node().
strpos(Haystack, Needle) ->
    call("strpos", [Haystack, Needle], #{type => integer}).

-spec starts_with(value(), value()) -> qast:ast_node().
starts_with(A, Prefix) ->
    call("starts_with", [A, Prefix], #{type => boolean}).

-spec regexp_replace(value(), value(), value()) -> qast:ast_node().
regexp_replace(A, Pattern, Repl) ->
    call("regexp_replace", [A, Pattern, Repl], qast:opts(A)).

-spec regexp_replace(value(), value(), value(), value()) -> qast:ast_node().
regexp_replace(A, Pattern, Repl, Flags) ->
    call("regexp_replace", [A, Pattern, Repl, Flags], qast:opts(A)).

-spec regexp_match(value(), value()) -> qast:ast_node().
regexp_match(A, Pattern) ->
    call("regexp_match", [A, Pattern], #{type => {array, text}}).

-spec regexp_match(value(), value(), value()) -> qast:ast_node().
regexp_match(A, Pattern, Flags) ->
    call("regexp_match", [A, Pattern, Flags], #{type => {array, text}}).

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
