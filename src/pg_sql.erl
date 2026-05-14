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

-type agg_order_spec() :: value()
                        | {value(), asc | desc}
                        | {value(), asc | desc, nulls_first | nulls_last}.
-type agg_order_specs() :: [agg_order_spec(), ...].

-spec 'array_agg'(value(), agg_order_specs()) -> qast:ast_node().
array_agg(Ast, OrderSpecs) ->
    Opts = qast:opts(Ast),
    Type = maps:get(type, Opts, undefined),
    NewOpts = Opts#{type => {array, Type}},
    qast:exp([
        qast:raw("array_agg("), Ast,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], NewOpts).

-spec avg(value()) -> qast:ast_node().
avg(A) ->
    call("avg", [A], qast:opts(A)).

-spec bool_and(value()) -> qast:ast_node().
bool_and(A) ->
    call("bool_and", [A], #{type => boolean}).

-spec bool_or(value()) -> qast:ast_node().
bool_or(A) ->
    call("bool_or", [A], #{type => boolean}).

-spec every(value()) -> qast:ast_node().
every(A) ->
    call("every", [A], #{type => boolean}).

-spec string_agg(value(), value()) -> qast:ast_node().
string_agg(Expr, Sep) ->
    call("string_agg", [Expr, Sep], #{type => text}).

-spec string_agg(value(), value(), agg_order_specs()) -> qast:ast_node().
string_agg(Expr, Sep, OrderSpecs) ->
    qast:exp([
        qast:raw("string_agg("), Expr, qast:raw(","), Sep,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => text}).

-spec json_agg(value()) -> qast:ast_node().
json_agg(A) ->
    call("json_agg", [A], #{type => json}).

-spec json_agg(value(), agg_order_specs()) -> qast:ast_node().
json_agg(A, OrderSpecs) ->
    qast:exp([
        qast:raw("json_agg("), A,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => json}).

-spec jsonb_agg(value()) -> qast:ast_node().
jsonb_agg(A) ->
    call("jsonb_agg", [A], #{type => jsonb}).

-spec jsonb_agg(value(), agg_order_specs()) -> qast:ast_node().
jsonb_agg(A, OrderSpecs) ->
    qast:exp([
        qast:raw("jsonb_agg("), A,
        agg_order_by(OrderSpecs),
        qast:raw(")")
    ], #{type => jsonb}).

-spec json_object_agg(value(), value()) -> qast:ast_node().
json_object_agg(K, V) ->
    call("json_object_agg", [K, V], #{type => json}).

-spec jsonb_object_agg(value(), value()) -> qast:ast_node().
jsonb_object_agg(K, V) ->
    call("jsonb_object_agg", [K, V], #{type => jsonb}).

-spec percentile_cont(value(), value()) -> qast:ast_node().
percentile_cont(Frac, OrderExpr) ->
    qast:exp([
        qast:raw("percentile_cont("), Frac,
        qast:raw(") within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-spec percentile_disc(value(), value()) -> qast:ast_node().
percentile_disc(Frac, OrderExpr) ->
    qast:exp([
        qast:raw("percentile_disc("), Frac,
        qast:raw(") within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-spec mode(value()) -> qast:ast_node().
mode(OrderExpr) ->
    qast:exp([
        qast:raw("mode() within group (order by "), OrderExpr,
        qast:raw(")")
    ], qast:opts(OrderExpr)).

-spec filter(qast:ast_node(), value()) -> qast:ast_node().
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

-spec '<@'(value(), value()) -> qast:ast_node().
'<@'(A, B) ->
    qast:exp([A, qast:raw(" <@ "), B], #{type => boolean}).

-spec '&&'(value(), value()) -> qast:ast_node().
'&&'(A, B) ->
    qast:exp([A, qast:raw(" && "), B], #{type => boolean}).

-spec array_length(value()) -> qast:ast_node().
array_length(Arr) ->
    array_length(Arr, 1).

-spec array_length(value(), value()) -> qast:ast_node().
array_length(Arr, Dim) ->
    call("array_length", [Arr, Dim], #{type => integer}).

-spec array_position(value(), value()) -> qast:ast_node().
array_position(Arr, Elem) ->
    call("array_position", [Arr, Elem], #{type => integer}).

-spec array_append(value(), value()) -> qast:ast_node().
array_append(Arr, Elem) ->
    call("array_append", [Arr, Elem], qast:opts(Arr)).

-spec array_prepend(value(), value()) -> qast:ast_node().
array_prepend(Elem, Arr) ->
    call("array_prepend", [Elem, Arr], qast:opts(Arr)).

-spec array_remove(value(), value()) -> qast:ast_node().
array_remove(Arr, Elem) ->
    call("array_remove", [Arr, Elem], qast:opts(Arr)).

-spec array_replace(value(), value(), value()) -> qast:ast_node().
array_replace(Arr, From, To) ->
    call("array_replace", [Arr, From, To], qast:opts(Arr)).

-spec array_cat(value(), value()) -> qast:ast_node().
array_cat(A, B) ->
    call("array_cat", [A, B], qast:opts(A)).

-spec unnest(value()) -> qast:ast_node().
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

-spec array([value()]) -> qast:ast_node().
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

-spec '||'(value(), value()) -> qast:ast_node().
'||'(A, B) ->
    qast:exp([qast:raw("("), A, qast:raw(" || "), B, qast:raw(")")], qast:opts(A)).

%% = Date/time functions =======================================================

-spec now() -> qast:ast_node().
now() ->
    call("now", [], #{type => timestamptz}).

-spec current_timestamp() -> qast:ast_node().
current_timestamp() ->
    qast:raw("current_timestamp", #{type => timestamptz}).

-spec current_date() -> qast:ast_node().
current_date() ->
    qast:raw("current_date", #{type => date}).

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

-spec date_trunc(datetime_field(), value()) -> qast:ast_node().
date_trunc(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    call("date_trunc", [qast:value(FieldBin, #{type => text}), Source], qast:opts(Source)).

-spec extract(datetime_field(), value()) -> qast:ast_node().
extract(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    qast:exp([
        qast:raw("extract("),
        qast:raw(FieldBin),
        qast:raw(" from "),
        Source,
        qast:raw(")")
    ], #{type => numeric}).

-spec date_part(datetime_field(), value()) -> qast:ast_node().
date_part(Field, Source) ->
    FieldBin = datetime_field_str(Field),
    call("date_part", [qast:value(FieldBin, #{type => text}), Source], #{type => float8}).

-spec age(value()) -> qast:ast_node().
age(A) ->
    call("age", [A], #{type => interval}).

-spec age(value(), value()) -> qast:ast_node().
age(A, B) ->
    call("age", [A, B], #{type => interval}).

-spec to_char(value(), value()) -> qast:ast_node().
to_char(A, Fmt) ->
    call("to_char", [A, Fmt], #{type => text}).

-spec to_date(value(), value()) -> qast:ast_node().
to_date(A, Fmt) ->
    call("to_date", [A, Fmt], #{type => date}).

-spec to_timestamp(value()) -> qast:ast_node().
to_timestamp(A) ->
    call("to_timestamp", [A], #{type => timestamptz}).

-spec to_timestamp(value(), value()) -> qast:ast_node().
to_timestamp(A, Fmt) ->
    call("to_timestamp", [A, Fmt], #{type => timestamptz}).

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
