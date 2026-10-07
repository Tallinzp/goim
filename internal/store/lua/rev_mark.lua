-- Revocation marker write (spec: Revoke step 1). Idempotent: max(stored, new).
-- KEYS[1] rev:{uid}; ARGV[1] '*' or 'appid:platform'; ARGV[2] revoked_before_ms; ARGV[3] TTL ms.
-- The TTL (= sess TTL, R4M-1) is refreshed on every write, even when the stored value is kept.
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if not cur or tonumber(cur) < tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
