wrk.method = 'POST'
wrk.headers['Content-Type'] = 'application/json'
local fire_at = os.date('!%Y-%m-%dT%H:%M:%SZ', os.time() + 3600)

-- thread id is assigned in setup() (master state) and injected as a GLOBAL
-- into each thread's state via thread:set. Do NOT shadow it with a local.
local next_id = 0
function setup(thread)
  thread:set('tid', next_id)
  next_id = next_id + 1
end

function init(args)
  n = 0
end

function request()
  n = n + 1
  local key = string.format('wrk-t%d-r%d', tid, n)   -- unique: (thread, per-thread counter)
  wrk.body = string.format('{"payload":"{}","fireAt":"%s","idempotencyKey":"%s","callbackUrl":"http://localhost:9000/hook"}', fire_at, key)
  return wrk.format()
end
