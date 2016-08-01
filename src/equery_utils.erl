-module(equery_utils).

-export([
         wrap/1,
         string_wrap/1,
         field_name/1,
         to_binary/1
        ]).

-spec wrap(iodata()) -> iolist().
wrap(F) ->
    ["\"", F, "\""].

-spec string_wrap(iodata()) -> iolist().
string_wrap(F) ->
    ["'", F, "'"].

-spec field_name(atom()) -> iolist().
field_name(Atom) when is_atom(Atom) ->
    wrap(atom_to_list(Atom)).

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, latin1);
to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
