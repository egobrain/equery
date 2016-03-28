%% -*- erlang -*-

-module(test_m).

-export([
         schema/0,
         filter/1
        ]).

schema() ->
    #{fields => #{
        id => #{type => integer}
      },
      table => <<"test">>
    }.

filter(Min) ->
    q:pipe([
        q:from(test_m),
        q:where(fun([#{id := Id}]) ->
            Id > Min
            bor bnot (bnot Min) %% skiped ops
        end)
    ]).
