-- Revoke step 2: delete the sess fields in scope whose token predates their effective marker.
-- A field written by a login with a newer token (after the marker) is kept.
-- KEYS[1] sess:{uid}; KEYS[2] rev:{uid}; ARGV[1] '*' (every field) or one 'appid:platform'.
-- Returns {field, value, field, value, ...} of the deleted entries, for the kicks.
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
local out = {}
for _, f in ipairs(fields) do
  local v = redis.call('HGET', KEYS[1], f)
  if v and tonumber(split(v)[7]) < marker(f) then
    redis.call('HDEL', KEYS[1], f)
    out[#out + 1] = f
    out[#out + 1] = v
  end
end
return out
