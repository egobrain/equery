[![Build Status](https://travis-ci.org/egobrain/equery.png?branch=master)](https://travis-ci.org/egobrain/equery.png?branch=master)
[![Coveralls](https://img.shields.io/coveralls/egobrain/equery.svg)](https://coveralls.io/github/egobrain/equery)
[![GitHub tag](https://img.shields.io/github/tag/egobrain/equery.svg)](https://github.com/egobrain/equery)
[![Hex.pm](https://img.shields.io/hexpm/v/equery.svg)](https://hex.pm/packages/equery)

# equery: erlang postgresql sql generator library
----------------------------------------------------

## Description ##

Library for postgresql sql generation.

## Simple Example

```erlang
1> Schema = #{
    fields => #{
        id => #{},
        name => #{}
    },
    table => <<"users">>}.
2> Q = q:from(Schema).
3> Q2 = q:where(fun([#{id := Id}]) -> Id > 3 end, Q).
4> qast:to_sql(qsql:select(Q2)).
{<<"select \"__table-0\".\"id\",\"__table-0\".\"name\" from \"users\" as \"__table-0\" where (\"__table-0\".\"id\" > $1)">>,
 [3]}
```

## More description will be later...