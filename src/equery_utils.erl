-module(equery_utils).

-export([
         wrap/1,
         field_name/1
        ]).

-spec wrap(iodata()) -> iolist().
wrap(F) ->
    ["\"", F, "\""].

-spec field_name(atom()) -> iolist().
field_name(Atom) when is_atom(Atom) ->
    wrap(atom_to_list(Atom)).
