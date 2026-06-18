# Code Commenting Standard

DishtaYantra is a commercial product; comments are part of the deliverable. This
document defines the bar. The guiding principle is simple:

> **Comment the *why*, not the *what*.** The code already says what it does. A
> comment earns its place by explaining intent, constraints, and consequences that
> the code cannot.

## What a good comment/docstring contains

For a non-trivial function, class, or block, document whichever of these apply:

- **Intent / contract** — what the caller can rely on, not a paraphrase of the body.
- **Invariants** — what must stay true (e.g. "offsets are contiguous and increasing",
  "recompute only when input changed").
- **Concurrency** — thread-safety, which thread runs this, locks held/required,
  single-producer/single-consumer assumptions.
- **Ordering & timing** — FIFO guarantees, when a value becomes durable/visible.
- **Units & ranges** — ms vs s, bytes vs records, 1-based vs 0-based, inclusive/exclusive.
- **Failure modes** — what it raises, what it returns on miss, what it does on a
  downstream error (retry? drop? block?).
- **Why this way** — the non-obvious design choice and the alternative rejected
  (e.g. "RLock because append() holds the lock and calls size_bytes()").
- **Gotchas** — anything that has bitten someone or will (e.g. "flush() ≠ fsync()").

## What NOT to write

- Restatements of the name: `# increment counter` above `counter += 1`;
  `"""Set the subscriber."""` on `set_subscriber()`. These are noise.
- Comments that will silently rot: avoid repeating a literal/value that lives in code.
- Commented-out code (delete it; that's what version control is for).
- "Comment theater" — docstrings added only to raise a coverage metric.

## Practical rules

1. **Every module** starts with a docstring: what it is, where it sits in the system,
   and any cross-cutting invariant it upholds.
2. **Every public class** documents its responsibility and its contract; **abstract
   base classes document the contract each method must honour** (this is where
   implementers look).
3. **Public functions/methods** get a docstring when behaviour isn't obvious from the
   signature; trivial accessors may be left bare rather than padded with boilerplate.
4. **Tricky blocks** get an inline comment stating the *why*. If you had to think about
   it, the next reader will too.
5. Keep comments next to what they describe, and **update them in the same commit** as
   the code — a stale comment is worse than none.
6. Prefer making the code self-explanatory (good names) over explaining unclear code.

## Reference implementation

`core/egress/` is maintained to this standard — see `wal.py` (module + ABC contract +
per-backend notes), `drainer.py` (concurrency + ordering), and `async_publisher.py`
(decision logic). Use it as the template when documenting other modules.
