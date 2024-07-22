-ifndef(CTH_HRL).
-define(CTH_HRL, true).

-ifdef(TEST).
-define(MAPS_TO_LIST(M), lists:sort(maps:to_list(M))).
-else.
-define(MAPS_TO_LIST(M), maps:to_list(M)).
-endif.

-endif.
