-- Remove a value from the store.

-- ARGV[1] = ttl (seconds)
-- ARGV[2] = value
-- ARGV[3] = removed.rc
-- ARGV[4] = removed.rs

-- Return: nothing.

local ttl, value, removed_rc, removed_rs = unpack(ARGV)
ttl = tonumber(ttl)
local removed_t = tonumber(redis.call('incr', string.format('timestamp:%s:%s', removed_rc, removed_rs)))
local ids = redis.call('smembers', 'ids:'..value)
for i,id in ipairs(ids) do
  local added_rc, added_rs = unpack(redis.call('hmget', 'element:'..id, 'added.rc', 'added.rs'))
  if (added_rc) then      -- key is not expired
    redis.call('hmset', 'element:'..id, 'removed.t', removed_t,
                                        'removed.rc', removed_rc,
                                        'removed.rs', removed_rs)
    if (ttl >= 0) then
      redis.call('expire', 'element:'..id, ttl)
    end
    redis.call('lpush', string.format('index:%s:%s', removed_rc, removed_rs), string.format('%d:%s', removed_t, id))
  end
end
