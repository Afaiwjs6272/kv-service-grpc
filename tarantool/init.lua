box.cfg {
    listen = 3301
}

box.once('init', function()
    local kv = box.schema.space.create('KV', {
        if_not_exists = true,
        format = {
            {'key', 'string'},
            {'value', 'varbinary'}
        }
    })

    kv:create_index('primary', {
        parts = {'key'},
        if_not_exists = true
    })

    if box.schema.user.exists('kvuser') == false then
        box.schema.user.create('kvuser', {password = 'kvpassword'})
    end

    box.schema.user.grant('kvuser', 'read,write', 'space', 'KV')

    box.schema.user.grant('kvuser', 'execute', 'universe')
end)

print('Tarantool KV Storage ready on port 3301')