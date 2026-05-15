-record(query, {
            schema :: q:schema(),
            with = undefined,
            distinct :: q:distinct() | undefined,
            where :: qast:ast_node() | undefined,
            data = []:: q:data(),
            select = #{} :: q:select(),
            set = #{} :: q:set() | #query{},
            tables = [] :: [q:real_table() | q:table()],
            joins = [] :: [{q:join_type(), qast:ast_node(), qast:ast_node()}],
            group_by = [] :: [qast:ast_node()],
            having :: qast:ast_node() | undefined,
            order_by = [] :: q:order(),
            on_conflict = #{} :: #{q:stored_conflict_target() => q:conflict_action()},
            limit :: {non_neg_integer(), q:ties_mode()} | undefined,
            offset :: non_neg_integer() | undefined,
            lock :: {q:row_lock_level(), [q:real_table()], q:wait_policy()} | undefined
         }).
