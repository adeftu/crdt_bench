-- Get the timestamp for each (rc,rs) node in the topology.

-- Return: array of strings in the format 'rc:rs:t', where 't' is the timestamp of node (rc,rs).

local nodes = redis.call('smembers', 'topology')
local timestamps = {}
for i,node in ipairs(nodes) do
  local rc_rs = string.match(node, '[^:]*:[^:]*')
  local t = redis.call('get', 'timestamp:'..rc_rs)
  if (t) then table.insert(timestamps, string.format('%s:%s', rc_rs, t)) end
end
return timestamps
