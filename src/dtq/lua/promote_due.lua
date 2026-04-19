-- Promote due retries from the retry zset back into the pending queue.
--
-- KEYS[1] : retry zset key
-- KEYS[2] : pending queue key
-- KEYS[3] : task hash prefix used to update each task's state
-- ARGV[1] : now (epoch seconds, max score to promote)
-- ARGV[2] : batch limit (max tasks to promote in one call)
-- ARGV[3] : task hash prefix string
--
-- Returns: number of tasks promoted.

local retry_key = KEYS[1]
local pending_key = KEYS[2]
local now = ARGV[1]
local limit = tonumber(ARGV[2])
local task_prefix = ARGV[3]

local due = redis.call("ZRANGEBYSCORE", retry_key, "-inf", now, "LIMIT", 0, limit)
local promoted = 0

for _, task_id in ipairs(due) do
    redis.call("ZREM", retry_key, task_id)
    redis.call("LPUSH", pending_key, task_id)
    local task_key = task_prefix .. task_id
    if redis.call("EXISTS", task_key) == 1 then
        redis.call("HSET", task_key, "state", "QUEUED")
        redis.call("HDEL", task_key, "next_run_at")
    end
    promoted = promoted + 1
end

return promoted
