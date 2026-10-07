-- Sweeper, one uid: delete every field written by the dead epoch.
-- KEYS[1] sess:{uid}; ARGV[1] epoch prefix 'gw|epoch|'. Returns the number deleted.
local n = 0
local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
  if string.sub(all[i + 1], 1, #ARGV[1]) == ARGV[1] then
    redis.call('HDEL', KEYS[1], all[i])
    n = n + 1
  end
end
return n
