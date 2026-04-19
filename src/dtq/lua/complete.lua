-- Atomically finalize a successful task.
--
-- KEYS[1] : processing list for this worker (e.g. dtq:processing:worker-1)
-- KEYS[2] : task hash key (e.g. dtq:task:<id>)
-- ARGV[1] : task_id (the value to LREM from the processing list)
-- ARGV[2] : now (epoch seconds)
-- ARGV[3] : pickled result blob (binary-safe)
--
-- Returns: number of items LREM'd from the processing list (typically 1).

local processing_key = KEYS[1]
local task_key = KEYS[2]
local task_id = ARGV[1]
local now = ARGV[2]
local result = ARGV[3]

local removed = redis.call("LREM", processing_key, 0, task_id)

if redis.call("EXISTS", task_key) == 1 then
    redis.call("HSET", task_key,
        "state", "SUCCESS",
        "completed_at", now,
        "result", result)
    -- Clear any stale error string from prior attempts.
    redis.call("HDEL", task_key, "error", "error_type")
end

return removed
