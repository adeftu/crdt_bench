-- Set timestamp for (rc,rs) to maximum between current and other_t.

-- ARGV[1] = rc
-- ARGV[2] = rs
-- ARGV[3] = other_t

-- Return: nothing.

local rc, rs, other_t = unpack(ARGV)
local timestamp_key = string.format('timestamp:%s:%s', rc, rs)
local current_t = redis.call('get', timestamp_key) or 0
redis.call('set', timestamp_key, math.max(tonumber(current_t), tonumber(other_t)))
