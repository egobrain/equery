-module(tree_m).

-export([schema/0]).

schema() ->
    #{
        fields => #{
            id => #{type => serial},
            parentId => #{type => integer, required => true},
            value => #{type => varchar}
        },
        table => <<"tree">>
    }.
