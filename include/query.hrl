-record(query, {
            schema :: q:schema(),
            with = undefined,
            distinct :: q:distinct() | undefined,
            where :: qast:ast_node() | undefined,
            data = []:: q:data(),
            select = #{} :: q:select(),
            set = #{} :: q:set() | #query{},
            tables = [] :: [{real, iolist(), reference()} | q:table()],
            joins = [] :: [{q:join_type(), qast:ast_node(), qast:ast_node()}],
            group_by = [] :: [qast:ast_node()],
            order_by = [] :: q:order(),
            on_conflict = #{} :: #{q:conflict_target() => q:conflict_action()},
            limit :: non_neg_integer() | undefined,
            offset :: non_neg_integer() | undefined,
            for_update = false :: boolean()
         }).
