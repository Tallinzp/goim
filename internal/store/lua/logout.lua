-- Logout Lua: compare-and-delete (spec: Logout Lua).
-- KEYS[1] sess:{uid}; ARGV[1] field; ARGV[2] owner prefix 'gw|epoch|conn_id|'.
-- Returns 1 if the field belonged to this connection and was deleted, else 0.
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur and string.sub(cur, 1, #ARGV[2]) == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return 1
end
return 0
