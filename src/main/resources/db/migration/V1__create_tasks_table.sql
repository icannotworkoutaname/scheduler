CREATE TABLE tasks (
                       id                 UUID PRIMARY KEY,
                       idempotency_key    TEXT NOT NULL,
                       payload            JSONB NOT NULL,
                       callback_url       TEXT NOT NULL,
                       fire_at            TIMESTAMPTZ NOT NULL,

                       state              TEXT NOT NULL DEFAULT 'pending',
                       version            BIGINT NOT NULL DEFAULT 0,
                       shard              SMALLINT NOT NULL,

                       lease_owner        TEXT,
                       lease_expires_at   TIMESTAMPTZ,
                       attempt_count      INTEGER NOT NULL DEFAULT 0,

                       created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                       updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

                       CONSTRAINT tasks_state_check CHECK (
                           state IN ('pending', 'firing', 'succeeded', 'retrying', 'dead', 'cancelled')
                           ),
                       CONSTRAINT tasks_shard_check CHECK (shard >= 0 AND shard < 64)
);
CREATE UNIQUE INDEX tasks_idempotency_key_uq ON tasks (idempotency_key);
CREATE INDEX tasks_shard_state_fire_at_idx ON tasks (shard, state, fire_at);
CREATE INDEX tasks_firing_lease_expires_idx ON tasks (lease_expires_at)
    WHERE state = 'firing';
CREATE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tasks_set_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
COMMENT ON TABLE tasks IS 'Source of truth for scheduled tasks. See requirements.md §6 for the state machine.';
COMMENT ON COLUMN tasks.shard IS 'hash(id) % 64, computed once at insert time by the application';