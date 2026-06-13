"""Lua scripts for atomic Guardrail operations.

Each script runs in **one Redis round-trip** so concurrent
precheck / consume requests can't ever race past the cap.
Doc07 §5.1 / §5.2.

Scripts use ARGV indexing rather than ``redis.call('HGET', ...)``
arithmetic strings so we can keep the bookkeeping cheap and
predictable across forks.

Return shapes
-------------

``PRECHECK_LUA``
    ``{1, "ok"}`` — accepted, quotas decremented.
    ``{0, layer_dim}`` — rejected; layer_dim is one of
    ``agent_tokens`` / ``agent_cycles`` / ``run_tokens`` /
    ``run_cycles``. The Python wrapper raises ``QuotaExceeded``.
    The script does NOT mutate any key on rejection.

``CONSUME_LUA``
    ``{used_tokens, used_cycles, used_cost_centi, exhausted_flag}``.
    Always succeeds (the LLM call already happened); we adjust
    counters by ``actual - est`` and set ``quota_exhausted`` if any
    dimension passed its cap. The exhausted_flag is 1/0.

``RELEASE_LUA``
    ``{1}`` if release happened, ``{0}`` if reservation already gone.
    Returns the est_tokens & cycle_inc that were rolled back so
    callers can observe parity in tests.

Quota semantics (Doc07 §2.1)
----------------------------

* ``agent.max_tokens_per_call`` — **per-call** bound: a single LLM
  call's token estimate must fit. Cumulative agent token usage is
  tracked for telemetry but never compared against this cap.
* ``agent.max_cycles`` — **cumulative**: per-instance lifetime.
* ``run.max_total_tokens`` — **cumulative** for the whole Run.
* ``run.max_cycles_per_run`` — **cumulative** for the whole Run.

Cost is stored as integer cents-of-cents (``actual_cost * 10000``)
so we don't need ``HINCRBYFLOAT`` (which behaves oddly under
``redis-cluster`` cross-slot loads). The Python side scales back.

KEYS layout (per script call):
    KEYS[1] = run hash key      (e.g. ``guardrail:run:run_xxx``)
    KEYS[2] = agent hash key    (e.g. ``guardrail:agent:agt_xxx``)
    KEYS[3] = reservation key   (per-call) — set by precheck w/ PEXPIRE.
"""

from __future__ import annotations

# Cost is stored as integer "cost_centi-cents" — fixed-point scale.
COST_SCALE = 10_000


# ============================================================
# precheck script
# ============================================================
PRECHECK_LUA = """
-- Args:
--   est_tokens, cycle_inc,
--   run_limit_tokens, run_limit_cycles,
--   agent_limit_tokens, agent_limit_cycles,
--   reservation_ttl_ms,
--   reservation_id

local est_tokens         = tonumber(ARGV[1])
local cycle_inc          = tonumber(ARGV[2])
local run_limit_tokens   = tonumber(ARGV[3])
local run_limit_cycles   = tonumber(ARGV[4])
local agent_limit_tokens = tonumber(ARGV[5])
local agent_limit_cycles = tonumber(ARGV[6])
local reservation_ttl_ms = tonumber(ARGV[7])
local reservation_id     = ARGV[8]

-- A previously-touched ``quota_exhausted=1`` short-circuits all
-- subsequent precheck calls without further math.
local run_exhausted = redis.call('HGET', KEYS[1], 'quota_exhausted')
if run_exhausted == '1' then
    return {0, 'run_exhausted'}
end
local agent_exhausted = redis.call('HGET', KEYS[2], 'quota_exhausted')
if agent_exhausted == '1' then
    return {0, 'agent_exhausted'}
end

local run_tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens_used') or '0')
local run_cycles = tonumber(redis.call('HGET', KEYS[1], 'cycles_used') or '0')
local ag_cycles  = tonumber(redis.call('HGET', KEYS[2], 'cycles_used') or '0')

-- agent.max_tokens_per_call is a PER-CALL bound (Doc07 §2.1) —
-- compare est_tokens directly, NOT cumulative ag_tokens.
if est_tokens > agent_limit_tokens then
    return {0, 'agent_tokens'}
end
-- agent.max_cycles is cumulative for the instance lifetime.
if ag_cycles + cycle_inc > agent_limit_cycles then
    return {0, 'agent_cycles'}
end
-- run-level caps are always cumulative.
if run_tokens + est_tokens > run_limit_tokens then
    return {0, 'run_tokens'}
end
if run_cycles + cycle_inc > run_limit_cycles then
    return {0, 'run_cycles'}
end

-- Pre-charge.
redis.call('HINCRBY', KEYS[1], 'tokens_used', est_tokens)
redis.call('HINCRBY', KEYS[1], 'cycles_used', cycle_inc)
-- Track agent.tokens_used for telemetry only (not enforced).
redis.call('HINCRBY', KEYS[2], 'tokens_used', est_tokens)
redis.call('HINCRBY', KEYS[2], 'cycles_used', cycle_inc)
redis.call('HINCRBY', KEYS[1], 'reservations_total', 1)

-- Track reservation for later release / TTL.
redis.call('HSET', KEYS[3],
    'run_hash',   KEYS[1],
    'agent_hash', KEYS[2],
    'est_tokens', est_tokens,
    'cycle_inc',  cycle_inc,
    'rid',        reservation_id,
    'state',      'active')
redis.call('PEXPIRE', KEYS[3], reservation_ttl_ms)

return {1, 'ok'}
"""


# ============================================================
# consume script
# ============================================================
CONSUME_LUA = """
-- Args:
--   actual_tokens, actual_cost_centi,
--   run_limit_tokens, run_limit_cycles,
--   agent_limit_tokens, agent_limit_cycles,
--   dry_charge       -- "1" to skip mutation (replay)
-- KEYS:
--   1 = run hash, 2 = agent hash, 3 = reservation key

local actual_tokens      = tonumber(ARGV[1])
local actual_cost_centi  = tonumber(ARGV[2])
local run_limit_tokens   = tonumber(ARGV[3])
local run_limit_cycles   = tonumber(ARGV[4])
local agent_limit_tokens = tonumber(ARGV[5])
local agent_limit_cycles = tonumber(ARGV[6])
local dry_charge         = ARGV[7]

-- Look up the original pre-charge so we can compute delta.
local rsv = redis.call('HMGET', KEYS[3],
    'est_tokens', 'cycle_inc', 'state')
local est_tokens = tonumber(rsv[1] or '0')
local cycle_inc  = tonumber(rsv[2] or '1')
local state      = rsv[3] or 'missing'

-- Mark reservation consumed (or 'late_consume' if it expired).
local late = '0'
if state == 'missing' then
    late = '1'  -- reservation expired before consume()
end
if state ~= 'consumed' then
    redis.call('HSET', KEYS[3], 'state', 'consumed')
end

if dry_charge == '1' then
    -- Replay mode: do not mutate counters; fold the est-pre-charge
    -- back so the run usage isn't artificially inflated.
    redis.call('HINCRBY', KEYS[1], 'tokens_used', -est_tokens)
    redis.call('HINCRBY', KEYS[2], 'tokens_used', -est_tokens)
    redis.call('HINCRBY', KEYS[1], 'cycles_used', -cycle_inc)
    redis.call('HINCRBY', KEYS[2], 'cycles_used', -cycle_inc)
    local r_t = tonumber(redis.call('HGET', KEYS[1], 'tokens_used') or '0')
    local r_c = tonumber(redis.call('HGET', KEYS[1], 'cycles_used') or '0')
    local cost = tonumber(redis.call('HGET', KEYS[1], 'cost_used_centi') or '0')
    return {r_t, r_c, cost, 0, late}
end

-- Real consume: apply delta = actual - est for tokens. Cycles
-- are not refunded (cycle is "we burned a thinking step").
local delta_tokens = actual_tokens - est_tokens
if delta_tokens ~= 0 then
    redis.call('HINCRBY', KEYS[1], 'tokens_used', delta_tokens)
    redis.call('HINCRBY', KEYS[2], 'tokens_used', delta_tokens)
end

-- Cost is informational only — accumulate and return.
if actual_cost_centi ~= 0 then
    redis.call('HINCRBY', KEYS[1], 'cost_used_centi', actual_cost_centi)
end

local final_run_tokens   = tonumber(redis.call('HGET', KEYS[1], 'tokens_used') or '0')
local final_run_cycles   = tonumber(redis.call('HGET', KEYS[1], 'cycles_used') or '0')
local final_ag_tokens    = tonumber(redis.call('HGET', KEYS[2], 'tokens_used') or '0')
local final_ag_cycles    = tonumber(redis.call('HGET', KEYS[2], 'cycles_used') or '0')
local final_run_cost     = tonumber(redis.call('HGET', KEYS[1], 'cost_used_centi') or '0')

local exhausted = 0
if final_run_tokens   > run_limit_tokens   then exhausted = 1 end
if final_run_cycles   > run_limit_cycles   then exhausted = 1 end
-- agent.tokens is per-call; we DO NOT sticky-exhaust on cumulative
-- overflow there (each future call's est_tokens is checked independently
-- against agent_limit_tokens at precheck). Only cycles are sticky.
if final_ag_cycles    > agent_limit_cycles then exhausted = 1 end

if exhausted == 1 then
    redis.call('HSET', KEYS[1], 'quota_exhausted', '1')
    redis.call('HSET', KEYS[2], 'quota_exhausted', '1')
end

return {final_run_tokens, final_run_cycles, final_run_cost, exhausted, late}
"""


# ============================================================
# release script
# ============================================================
RELEASE_LUA = """
-- Roll back a reservation that was created but never consumed.
-- Used when the LLM call errored out before consume(), or when the
-- reservation expired.
-- KEYS: 1 = run hash, 2 = agent hash, 3 = reservation key
-- ARGV: (none)

local rsv = redis.call('HMGET', KEYS[3],
    'est_tokens', 'cycle_inc', 'state')
local est_tokens = tonumber(rsv[1] or '0')
local cycle_inc  = tonumber(rsv[2] or '0')
local state      = rsv[3]

if state == nil or state == 'consumed' or state == 'released' then
    return {0, 0, 0}
end

redis.call('HINCRBY', KEYS[1], 'tokens_used', -est_tokens)
redis.call('HINCRBY', KEYS[2], 'tokens_used', -est_tokens)
redis.call('HINCRBY', KEYS[1], 'cycles_used', -cycle_inc)
redis.call('HINCRBY', KEYS[2], 'cycles_used', -cycle_inc)
redis.call('HSET', KEYS[3], 'state', 'released')

return {1, est_tokens, cycle_inc}
"""
