pub fn sizeOf(comptime Entity: type) comptime_int {
    var entity_size = 0;
    for (Entity.map_fields_meta.values) |field_meta| {
        entity_size += field_meta.size;
    }

    return entity_size;
}
