-record(query, {
            schema = undefined :: #{},
            where = undefined,
            data = [],
            select = #{},
            set = #{},
            tables = [],
            joins = [],
            group_by = [],
            order_by = [],
            limit :: non_neg_integer(),
            offset :: non_neg_integer()
         }).
