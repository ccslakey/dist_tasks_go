package redisstream

const promoteRetriesScript = `
-- TODO: Atomically move due retry jobs back into the main stream.
-- Suggested shape:
-- 1. ZRANGEBYSCORE retry key for due jobs
-- 2. XADD each serialized job back to the main stream
-- 3. ZREM promoted retry entries
return 0
`
