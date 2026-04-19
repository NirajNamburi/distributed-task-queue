-- Atomically mark a task as RUNNING after it has been claimed.
--
-- KEYS[1] : task hash key  (e.g. dtq:task:<id>)
-- ARGV[1] : worker_id
-- ARGV[2] : now (epoch seconds, as a string)
--
-- Returns: 1 if the task hash existed and was updated, 0 if the hash did
--          not exist (caller should treat this as "task was wiped, drop").

local task_key = KEYS[1]
local worker_id = ARGV[1]
local now = ARGV[2]

if redis.call("EXISTS", task_key) == 0 then
    return 0
end

redis.call("HSET", task_key,
    "state", "RUNNING",
    "worker_id", worker_id,
    "claimed_at", now)
return 1
