-- Reconcile compare-and-set (spec: Reconcile handling, rules 3 and 4).
-- KEYS[1] sess:{uid}; KEYS[2] rev:{uid}
-- ARGV[1] field; ARGV[2] value Logic observed ('' = absent); ARGV[3] chunk entry value
-- ARGV[4] tok_iat_ms; ARGV[5] sent_ms; ARGV[6] login_deadline_ms; ARGV[7] sess TTL ms
-- Returns REVOKED | REFUSED | UNCHANGED | WRITTEN.
-- Rule 1 is re-checked here: a Revoke that lands between Logic's read and this
-- write (marker confirmed, field deleted) must not be undone by a restore.
local marks = redis.call('HMGET', KEYS[2], '*', ARGV[1])
local marker = 0
for i = 1, 2 do
  if marks[i] then
    local m = tonumber(marks[i])
    if m > marker then marker = m end
  end
end
if tonumber(ARGV[4]) < marker then return 'REVOKED' end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if now - tonumber(ARGV[5]) > tonumber(ARGV[6]) then return 'REFUSED' end

local cur = redis.call('HGET', KEYS[1], ARGV[1]) or ''
if cur ~= ARGV[2] then return 'UNCHANGED' end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[7])
return 'WRITTEN'
