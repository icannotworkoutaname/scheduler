-- V2: shard 租约表(requirements.md §7.4)。
--
-- 跟 tasks 表的设计哲学不同:这张表行数固定是 64,永远不会再多。
-- 不需要为它优化查询路径(64 行的全表扫描本身就是免费的),
-- 表本身的价值在于提供一个所有节点都能通过乐观锁竞争的共享状态,
-- 不是在存储量上有任何压力。

CREATE TABLE shards (
    shard_id         SMALLINT PRIMARY KEY,
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    version          BIGINT NOT NULL DEFAULT 0
);

-- 64 行,shard_id 从 0 到 63,跟 tasks.shard 的取值范围严丝合缝对应
-- (tasks 表的 CHECK 约束 `shard >= 0 AND shard < 64` 就是这个范围)。
-- 这些行不是运行时动态创建的——64 是设计时就定死的分片粒度,
-- 迁移脚本负责把它们一次性铺好。
INSERT INTO shards (shard_id)
SELECT generate_series(0, 63);

COMMENT ON TABLE shards IS 'One row per logical shard. No coordinator — nodes compete for rows via optimistic locking, same primitive as tasks.version.';
