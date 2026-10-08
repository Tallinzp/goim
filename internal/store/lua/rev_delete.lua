-- Revoke step 2: delete the sess fields in scope whose token predates their effective marker.
-- A field written by a login with a newer token (after the marker) is kept; a value
-- whose tok_iat is not a number cannot be a valid session and is deleted.
-- KEYS[1] sess:{uid}; KEYS[2] rev:{uid}; KEYS[3] revdel:{uid}:<scope>:<before_ms>.
-- ARGV[1] '*' (every field) or one 'appid:platform'; ARGV[2] KEYS[3] TTL ms.
-- Every deleted entry is also kept in KEYS[3], so a re-run of the same request
-- (a lost reply, or the account service's retry) returns it again for the kicks (PI-2).
-- Returns {field, value, ...}: every entry this request has deleted so far.
local function split(s)
  local out = {}
  for part in string.gmatch(s .. '|', '([^|]*)|') do out[#out + 1] = part end
  return out
end
local function marker(f)
  local m = redis.call('HMGET', KEYS[2], '*', f)
  local best = 0
  for i = 1, 2 do
    if m[i] then
      local v = tonumber(m[i])
      if v > best then best = v end
    end
  end
  return best
end

local fields
if ARGV[1] == '*' then
  fields = redis.call('HKEYS', KEYS[1])
else
  fields = {ARGV[1]}
end
local deleted = false
for _, f in ipairs(fields) do
  local v = redis.call('HGET', KEYS[1], f)
  if v then
    local iat = tonumber(split(v)[7] or '')
    if iat == nil or iat < marker(f) then
      redis.call('HDEL', KEYS[1], f)
      redis.call('HSET', KEYS[3], f, v)
      deleted = true
    end
  end
end
if deleted then redis.call('PEXPIRE', KEYS[3], ARGV[2]) end
return redis.call('HGETALL', KEYS[3])
