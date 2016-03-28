-module(qast).

-export([
         field/3,
         value/1, value/2,
         raw/1,
         exp/1, exp/2,
         table/1,

         is_ast/1,
         opts/1,
         set_opts/2,
         join/2
        ]).

-export([
         to_sql/1
        ]).

field(TableRef, Name, Opts) ->
    exp([table(TableRef), raw([".", equery_utils:field_name(Name)])], Opts).

value(V) -> value(V, #{}).
value(V, Opts) -> {'$value', Opts, V}.

exp(V) -> exp(V, #{}).
exp(V, Opts) -> {'$exp', Opts, V}.

raw(V) -> raw(V, #{}).
raw(V, Opts) -> {'$raw', Opts, V}.

table(Ref) -> {'$table', Ref}.

opts({'$value', Opts, _}) -> Opts;
opts({'$exp', Opts, _}) -> Opts;
opts({'$raw', Opts, _}) -> Opts;
opts(_) -> #{}.

set_opts({'$value', _Opts, Value}, NewOpts) -> {'$value', NewOpts, Value};
set_opts({'$exp', _Opts, Exp}, NewOpts) -> {'$exp', NewOpts, Exp};
set_opts({'$raw', _Opts, Raw}, NewOpts) -> {'$raw', NewOpts, Raw};
set_opts({'$table', _Raw}, _NewOpts) -> throw(is_table);
set_opts(V, NewOpts) -> value(V, NewOpts).

is_ast({'$value', _Opts, _Value}) -> true;
is_ast({'$exp', _Opts, _Exp}) -> true;
is_ast({'$raw', _Opts, _Raw}) -> true;
is_ast({'$table', _Raw}) -> true;
is_ast(_) -> false.

%% =============================================================================
%% Utils
%% =============================================================================

join([], _Sep) -> [];
join([H|T], Sep) ->
    qast:exp([H|lists:foldr(fun(I, Acc) -> [Sep,I|Acc] end, [], T)]).

-record(state, {
            aliases=#{}, tables_cnt=0,
            args=[], args_cnt=0
       }).
to_sql(Ast) ->
    {Sql, #state{args=Args}} = traverse(
        fun({'$value', _Opts, V}, #state{args=Vs, args_cnt=Cnt}=St) ->
               NewCnt = Cnt+1,
               {index(NewCnt), St#state{args=[V|Vs], args_cnt=NewCnt}};
           ({'$table', TRef}, St) ->
               get_table_alias(TRef, St);
           ({'$raw', _Opts, V}, St) ->
               {V, St}
        end, #state{}, Ast),
    {iolist_to_binary(Sql), lists:reverse(Args)}.

%% =============================================================================
%% Internal
%% =============================================================================

traverse(F, Acc, {T, _Opts, List}) when T =:= '$exp' ->
    lists:mapfoldl(fun(E, A) -> traverse(F, A, E) end, Acc, List);
traverse(F, Acc, {'$raw', _, _}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$value', _Opts, _V}=Item) ->
    F(Item, Acc);
traverse(F, Acc, {'$table', _V}=Item) ->
    F(Item, Acc);
%% Other is value
traverse(F, Acc, V) ->
    F(qast:value(V), Acc).

index(N) ->
    [ $$, integer_to_binary(N) ].

table_alias(Int) ->
    equery_utils:wrap(lists:concat(["__table-",Int])).

get_table_alias(TRef, #state{aliases=As, tables_cnt=Cnt}=St) ->
    case maps:find(TRef, As) of
        {ok, TAlias} -> {TAlias, St};
        error ->
            TAlias = table_alias(Cnt),
            As2 = maps:put(TRef, TAlias, As),
            {TAlias, St#state{aliases=As2, tables_cnt=Cnt+1}}
    end.
