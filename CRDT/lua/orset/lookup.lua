-- Search a value in the store.

-- ARGV[1] = value

-- Return: TRUE if value found, FALSE otherwise.

local value = unpack(ARGV)
local ids = redis.call('smembers', 'ids:'..value)
for i,id1 in ipairs(ids) do
  local added_t1, added_rc1, added_rs1, removed_t1 = 
    unpack(redis.call('hmget', 'element:'..id1, 'added.t', 'added.rc', 'added.rs', 'removed.t'))
  if (added_t1 and not removed_t1) then        -- key is not expired and tuple is not removed
    local exists = true
    for j,id2 in ipairs(ids) do
      if (i ~= j) then
        local added_t2, added_rc2, added_rs2, removed_t2, removed_rc2, removed_rs2 = 
          unpack(redis.call('hmget', 'element:'..id2, 'added.t', 'added.rc', 'added.rs', 'removed.t', 'removed.rc', 'removed.rs'))
        if (added_t2 and       -- key is not expired
          tonumber(added_t2) == tonumber(added_t1) and
          added_rc2 == added_rc1 and
          added_rs2 == added_rs1 and
          removed_t2) then
          exists = false
        end
      end
    end
    if (exists) then return true end
  end
end
return false
