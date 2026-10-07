-- Login Lua (spec: Redis schema / Login Lua). One slot: sess:{uid} and rev:{uid}.
-- KEYS[1] sess:{uid}        KEYS[2] rev:{uid}
-- ARGV[1] field             ARGV[2] new entry value
-- ARGV[3] device_id         ARGV[4] conn_seq
-- ARGV[5] recv_ms           ARGV[6] tok_iat_ms
-- ARGV[7] login_deadline_ms ARGV[8] sess TTL ms
-- Returns {code, stored value or ''}; code ALREADY means this exact entry is already stored.
-- epoch and conn_id are uint64 and are never converted to Lua numbers (doubles).
local function split(s)
  local out = {}
  for part in string.gmatch(s .. '|', '([^|]*)|') do out[#out + 1] = part end
  return out
end

local field = ARGV[1]

-- 1. Revocation first (R4H-OP1): the effective marker is max(rev[*], rev[field]).
local marks = redis.call('HMGET', KEYS[2], '*', field)
local marker = 0
for i = 1, 2 do
  if marks[i] then
    local m = tonumber(marks[i])
    if m > marker then marker = m end
  end
end
if tonumber(ARGV[6]) < marker then return {'REVOKED', ''} end

-- A re-run of this exact request: go-redis re-sends a pipeline whose reply was
-- lost after the write. Report it instead of judging our own write as STALE.
if redis.call('HGET', KEYS[1], field) == ARGV[2] then return {'ALREADY', ''} end

-- 2. Deadline (R1-O4): no write later than recv_ms + login_deadline.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local recv = tonumber(ARGV[5])
local deadline = tonumber(ARGV[7])
if now - recv > deadline then return {'EXPIRED', ''} end

-- 3. The login table.
local code = 'WRITTEN'
local stored = redis.call('HGET', KEYS[1], field)
if stored then
  local s = split(stored)
  local s_seq, s_login = tonumber(s[5]), tonumber(s[6])
  if s[4] == ARGV[3] then
    if tonumber(ARGV[4]) > s_seq then
      code = 'REPLACED'
    elseif recv <= s_login + deadline then
      return {'STALE', stored}
    else
      code = 'REGRESSED'
    end
  elseif recv >= s_login then
    code = 'KICKED'
  else
    return {'SUPERSEDED', stored}
  end
end
redis.call('HSET', KEYS[1], field, ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return {code, stored or ''}
