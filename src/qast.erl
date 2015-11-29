-module(qast).

-export([
         field/2,
         value/1, value/2,
         raw/1,
         'andalso'/2,
         'orelse'/2,

         '=:='/2,
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

-type field_name() :: binary().
-type field() :: {'$field', non_neg_integer(), field_name()}.


-spec field(reference(), field_name()) -> field().
field(TableRef, Name) -> {'$field', TableRef, Name}.

value(V) ->
    value(V, undefined).

value(V, Type) ->
    {'$value', Type, V}.

'andalso'(A, B) ->
    {'$exp', [raw("("), A, raw(" and "), B, raw(")")]}.
'orelse'(A, B) ->
    {'$exp', [raw("("), A, raw(" or "), B, raw(")")]}.
'not'(A) ->
    {'$exp', [raw("not "), A]}.

'=:='(A, B) ->
    {'$exp', [raw("("), A, raw(" = "), B, raw(")")]}.
'>'(A, B) ->
    {'$exp', [raw("("), A, raw(" > "), B, raw(")")]}.
'>='(A, B) ->
    {'$exp', [raw("("), A, raw(" >= "), B, raw(")")]}.
'<'(A, B) ->
    {'$exp', [raw("("), A, raw(" < "), B, raw(")")]}.
'=<'(A, B) ->
    {'$exp', [raw("("), A, raw(" <= "), B, raw(")")]}.

'+'(A, B) ->
    {'$exp', [raw("("), A, raw(" + "), B, raw(")")]}.
'-'(A, B) ->
    {'$exp', [raw("("), A, raw(" - "), B, raw(")")]}.
'*'(A, B) ->
    {'$exp', [raw("("), A, raw(" * "), B, raw(")")]}.
'/'(A, B) ->
    {'$exp', [raw("("), A, raw(" / "), B, raw(")")]}.

%% =============================================================================
%% Internal
%% =============================================================================

raw(V) -> {'$raw', V}.
