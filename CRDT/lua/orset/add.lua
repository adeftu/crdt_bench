-- Add a value to the store.

-- ARGV[1] = ttl (seconds)
-- ARGV[2] = value
-- ARGV[3] = added.rc
-- ARGV[4] = added.rs

-- Return: nothing.

local ttl, value, added_rc, added_rs = unpack(ARGV)
ttl = tonumber(ttl)
local added_t = tonumber(redis.call('incr', string.format('timestamp:%s:%s', added_rc, added_rs)))
local id = string.format('%s.%s.%s', added_rc, added_rs, redis.call('incr', 'element:next.id'))
redis.call('hmset', 'element:'..id, 'value', value,
                                    'added.t', added_t,
                                    'added.rc', added_rc,
                                    'added.rs', added_rs)
if (ttl >= 0) then
  redis.call('expire', 'element:'..id, ttl)
end
redis.call('sadd', 'ids:'..value, id)
redis.call('lpush', string.format('index:%s:%s', added_rc, added_rs), string.format('%d:%s', added_t, id))
