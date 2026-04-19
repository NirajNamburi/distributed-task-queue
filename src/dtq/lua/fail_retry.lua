-- Atomically schedule a failed task for delayed retry.
--
-- KEYS[1] : processing list for this worker
-- KEYS[2] : task hash key
-- KEYS[3] : retry zset key (e.g. dtq:retries)
-- ARGV[1] : task_id
-- ARGV[2] : now (epoch seconds)
-- ARGV[3] : run_at (epoch seconds when the task should be re-tried)
-- ARGV[4] : attempts (integer, the count AFTER this failure)
-- ARGV[5] : error message
-- ARGV[6] : error type name (exception class)
--
-- Returns: number of items LREM'd from the processing list.

local processing_key = KEYS[1]
local task_key = KEYS[2]
local retry_zset = KEYS[3]
local task_id = ARGV[1]
local now = ARGV[2]
local run_at = ARGV[3]
local attempts = ARGV[4]
local err = ARGV[5]
local err_type = ARGV[6]

local removed = redis.call("LREM", processing_key, 0, task_id)

if redis.call("EXISTS", task_key) == 1 then
    redis.call("HSET", task_key,
        "state", "RETRYING",
        "attempts", attempts,
        "next_run_at", run_at,
        "error", err,
        "error_type", err_type,
        "completed_at", now)
end

redis.call("ZADD", retry_zset, run_at, task_id)
return removed
