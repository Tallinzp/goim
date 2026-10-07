-- GatewayLease renewal (spec: GatewayLease, R3-O3), safe to re-run.
-- KEYS[1] gwlease:gw:{epoch}; ARGV[1] per-call nonce; ARGV[2] lease TTL ms.
-- The value is '<nonce>|<missing>'. go-redis re-sends a command whose reply was
-- lost after the write; a re-run (same nonce) returns the first run's answer,
-- never 'live' for a key that run created.
-- Returns {missing (1 or 0), Redis TIME in ms on this shard}.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local cur = redis.call('GET', KEYS[1])
local tag = ARGV[1] .. '|'
local missing
if cur and string.sub(cur, 1, #tag) == tag then
  missing = tonumber(string.sub(cur, #tag + 1))
elseif cur then
  missing = 0
else
  missing = 1
end
redis.call('SET', KEYS[1], tag .. missing, 'PX', ARGV[2])
return {missing, now}
