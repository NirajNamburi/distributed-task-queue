-- Drain a dead worker's processing list back into the pending queue.
--
-- Called by the reaper after detecting a stale heartbeat. Each task in the
-- worker's processing list has its attempts counter incremented (so a worker
-- that loops crashing on the same task eventually trips the DLQ path) and is
-- moved back to the pending queue. The worker's heartbeat entry is removed.
--
-- KEYS[1] : processing list for the dead worker
-- KEYS[2] : pending queue key
-- KEYS[3] : heartbeat hash key
-- ARGV[1] : worker_id
-- ARGV[2] : task hash prefix (e.g. "dtq:task:")
-- ARGV[3] : now (epoch seconds)
-- ARGV[4] : max_retries (default cap; per-task max_retries is preserved if set)
-- ARGV[5] : dlq key
--
-- Returns: number of tasks requeued (may be 0).

local processing_key = KEYS[1]
local pending_key = KEYS[2]
local heartbeat_key = KEYS[3]
local worker_id = ARGV[1]
local task_prefix = ARGV[2]
local now = ARGV[3]
local default_max = tonumber(ARGV[4])
local dlq_key = ARGV[5]

local requeued = 0
local dlq_count = 0

while true do
    -- Pop one task at a time so we never lose anything mid-iteration on error.
    local task_id = redis.call("RPOP", processing_key)
    if not task_id then
        break
    end

    local task_key = task_prefix .. task_id
    local exists = redis.call("EXISTS", task_key) == 1

    local attempts = 0
    local max_retries = default_max
    if exists then
        local raw_attempts = redis.call("HGET", task_key, "attempts")
        attempts = tonumber(raw_attempts) or 0
        local raw_max = redis.call("HGET", task_key, "max_retries")
        if raw_max then
            max_retries = tonumber(raw_max) or default_max
        end
    end

    attempts = attempts + 1

    if attempts > max_retries then
        if exists then
            redis.call("HSET", task_key,
                "state", "FAILED",
                "attempts", attempts,
                "error", "worker died with task in flight (exceeded max_retries on recovery)",
                "error_type", "WorkerDied",
                "completed_at", now)
        end
        redis.call("LPUSH", dlq_key, task_id)
        dlq_count = dlq_count + 1
    else
        if exists then
            redis.call("HSET", task_key,
                "state", "QUEUED",
                "attempts", attempts,
                "error", "worker died with task in flight (recovered by reaper)",
                "error_type", "WorkerDied")
        end
        redis.call("LPUSH", pending_key, task_id)
        requeued = requeued + 1
    end
end

redis.call("HDEL", heartbeat_key, worker_id)
return {requeued, dlq_count}
