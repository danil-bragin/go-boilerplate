-- +goose Up
-- Hash-chain tamper-evidence. Each audit row carries:
--   prev_hash  — entry_hash of the immediately preceding row in the chain
--   entry_hash — sha256(prev_hash || canonical(actor,action,subject,at,metadata))
-- so any in-place edit of a historical row breaks the chain from that row
-- forward and audit.VerifyChain detects the first divergence.
--
-- Chain scope is GLOBAL (a single chain over the whole table). Serialization
-- across concurrent writers is provided by a one-row chain_head table locked
-- FOR UPDATE inside the audit insert transaction: every Record reads + advances
-- the head under that lock, so the chain order is total and gap-free even under
-- concurrent commands. See audit.Record + BenchmarkRecord for the contention
-- cost this imposes (it serializes audit writes — documented as the throughput
-- ceiling of a single global chain).
alter table audit_log
    add column prev_hash  bytea,
    add column entry_hash bytea;

create table audit_chain_head (
    id         smallint primary key default 1,
    last_hash  bytea       not null,
    updated_at timestamptz not null default now(),
    constraint audit_chain_head_singleton check (id = 1)
);

-- Genesis: an all-zero 32-byte hash is the prev_hash of the first real entry.
insert into audit_chain_head (id, last_hash)
values (1, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);

-- +goose Down
drop table audit_chain_head;
alter table audit_log
    drop column entry_hash,
    drop column prev_hash;
