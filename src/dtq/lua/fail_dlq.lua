-- Atomically move a permanently-failed task to the dead-letter queue.
--
-- KEYS[1] : processing list for this worker
-- KEYS[2] : task hash key
-- KEYS[3] : dead-letter queue key
-- ARGV[1] : task_id
-- ARGV[2] : now (epoch seconds)
-- ARGV[3] : attempts (integer)
-- ARGV[4] : error message
-- ARGV[5] : error type name
--
-- Returns: number of items LREM'd from the processing list.

local processing_key = KEYS[1]
local task_key = KEYS[2]
local dlq_key = KEYS[3]
local task_id = ARGV[1]
local now = ARGV[2]
local attempts = ARGV[3]
local err = ARGV[4]
local err_type = ARGV[5]

local removed = redis.call("LREM", processing_key, 0, task_id)

if redis.call("EXISTS", task_key) == 1 then
    redis.call("HSET", task_key,
        "state", "FAILED",
        "attempts", attempts,
        "error", err,
        "error_type", err_type,
        "completed_at", now)
end

redis.call("LPUSH", dlq_key, task_id)
return removed
