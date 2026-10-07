-- ReconcileEnd delete phase, one uid (spec: Reconcile handling / Delete phase).
-- KEYS[1] sess:{uid}; ARGV[1] epoch prefix 'gw|epoch|'
-- ARGV[2], ARGV[3], ...: field, value pairs Logic selected; each is deleted only if unchanged.
-- Returns {deleted, fields still pointing at this epoch}.
local deleted = 0
for i = 2, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
    redis.call('HDEL', KEYS[1], ARGV[i])
    deleted = deleted + 1
  end
end
local remaining = 0
for _, v in ipairs(redis.call('HVALS', KEYS[1])) do
  if string.sub(v, 1, #ARGV[1]) == ARGV[1] then remaining = remaining + 1 end
end
return {deleted, remaining}
