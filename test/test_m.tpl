%% -*- erlang -*-

-module(test_m).

-export([
         schema/0,
         filter/2,
         wrong/1
        ]).

schema() ->
    #{fields => #{
        id => #{type => integer}
      },
      table => <<"test">>
    }.

filter(Min, Q) ->
    q:where(
        fun ([#{id := Id}]) ->
            Id > Min
                bor bnot (bnot Min); %% skiped ops
            ([#{id := Id}, _]) ->
            Id =:= Min
        end, Q).

wrong(_Q) ->
    q:pipe([
        q:from(test_m),
        q:where(1)
    ]).
