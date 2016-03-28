-module(equery_utils).

-export([
         wrap/1,
         field_name/1
        ]).

wrap(F) ->
    ["\"", F, "\""].

field_name(Atom) when is_atom(Atom) ->
    wrap(atom_to_list(Atom)).
