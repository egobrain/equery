-module(equery_utils).
-moduledoc """
Identifier wrapping and shared grammar helpers.

Mostly internal: identifier quoting (`wrap/1`, `wrap_table/1`,
`field_name/1`) and the order-item builder shared between top-level
`ORDER BY` and aggregate-internal ordering (`order_item_exp/1`).
""".

-export([
         wrap/1,
         wrap_table/1,
         field_name/1,
         to_binary/1,
         order_item_exp/1
        ]).

-spec wrap(iodata()) -> iolist().
wrap(F) ->
    ["\"", F, "\""].

-spec wrap_table(binary() | {binary(), binary()}) -> iolist().
wrap_table({Schema, Table}) ->
    [wrap(Schema), $., wrap(Table)];
wrap_table(Table) ->
    wrap(Table).

-spec field_name(atom()) -> iolist().
field_name(Atom) when is_atom(Atom) ->
    wrap(atom_to_list(Atom)).

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
to_binary(Bin) when is_binary(Bin) ->
    Bin.

-spec order_item_exp({qast:ast_node(), asc | desc}
                   | {qast:ast_node(), asc | desc, nulls_first | nulls_last})
    -> qast:ast_node().
order_item_exp({Field, Direction}) ->
    qast:exp([Field, direction_exp(Direction)]);
order_item_exp({Field, Direction, Nulls}) ->
    qast:exp([Field, direction_exp(Direction), nulls_exp(Nulls)]).

direction_exp(asc) -> qast:raw(<<" ASC">>);
direction_exp(desc) -> qast:raw(<<" DESC">>).

nulls_exp(nulls_first) -> qast:raw(<<" NULLS FIRST">>);
nulls_exp(nulls_last) -> qast:raw(<<" NULLS LAST">>).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

to_binary_test() ->
    ?assertEqual(<<"atom">>, to_binary(atom)),
    ?assertEqual(<<"123">>, to_binary(123)),
    ?assertEqual(<<"bin">>, to_binary(<<"bin">>)).

-endif.
