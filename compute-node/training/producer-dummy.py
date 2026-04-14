"""
Streaming producer for the disaggregated-training experiment.

Writes shard files into a shared (tmpfs) directory at a fixed cadence to
emulate a remote data source feeding a GPU consumer over time. Each shard
is written atomically: the tensors are torch.saved to `<n>.tmp` and
then renamed to `<n>`, so a consumer polling the directory can never
observe a partially written file. After the last shard, an empty `DONE`
sentinel is created to signal end of stream.

The producer also logs per-shard size and the wall-clock gap between
successive writes, which lets you cross-check the consumer's wait-time
measurements against the cadence actually achieved here.
"""

import argparse
import os
import time

import torch

# Fixed dataset shape. Must match the consumer.
VOCAB_SIZE  = 10000
SEQ_LEN     = 64
NUM_CLASSES = 2


def human_bytes(n):
    """Format a byte count as KiB or MiB for log readability."""
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if n < 1024:
            return f"{n:6.1f} {unit}"
        n /= 1024
    return f"{n:6.1f} TiB"


def write_shard(data_dir, idx, size):
    """
    Build a random shard of `size` samples and write it atomically.
    Returns (final_path, bytes_written).
    """
    # Deterministic per-shard seed so producer and consumer agree on content
    # if you ever want to replay a run.
    torch.manual_seed(idx)
    X = torch.randint(0, VOCAB_SIZE, (size, SEQ_LEN))
    y = torch.randint(0, NUM_CLASSES, (size,))

    final = os.path.join(data_dir, f'shard_{idx:04d}.pt')
    tmp   = final + '.tmp'

    # Atomic publish: save under a temp name, then rename. Rename is atomic
    # within a single filesystem, so the consumer's glob() will never match
    # a half-written file.
    torch.save({'X': X, 'y': y}, tmp)
    os.rename(tmp, final)

    return final, os.path.getsize(final)


def clean_dir(data_dir):
    """Remove leftover shards and any stale DONE sentinel from a prior run."""
    removed = 0
    for name in os.listdir(data_dir):
        if name.startswith('shard_') or name == 'DONE':
            os.unlink(os.path.join(data_dir, name))
            removed += 1
    return removed


def main():
    ap = argparse.ArgumentParser(description="Streaming shard producer")
    ap.add_argument('--data-dir',   default=os.environ.get('DATA_DIR', '/tmp/shm'),
                    help='directory to publish shards into (default: $DATA_DIR or /tmp/shm)')
    ap.add_argument('--num-shards', type=int,   default=8,
                    help='number of shards to publish before signaling DONE')
    ap.add_argument('--shard-size', type=int,   default=1000,
                    help='samples per shard')
    ap.add_argument('--interval',   type=float, default=0.5,
                    help='seconds to sleep between successive shard publishes')
    args = ap.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    stale = clean_dir(args.data_dir)

    # ── banner ───────────────────────────────────────────────────────────
    print("=" * 72, flush=True)
    print(f"PRODUCER  dir={args.data_dir}  shards={args.num_shards}  "
          f"size={args.shard_size}  interval={args.interval}s", flush=True)
    if stale:
        print(f"  cleaned {stale} leftover file(s) from previous run", flush=True)
    print("=" * 72, flush=True)
    print(f"{'idx':>4} {'file':<20} {'bytes':>10} "
          f"{'gap_ms':>8} {'elapsed_s':>10}", flush=True)
    print("-" * 72, flush=True)

    # ── publish loop ─────────────────────────────────────────────────────
    t_start    = time.perf_counter()
    t_prev     = t_start
    total_bytes = 0

    for i in range(args.num_shards):
        path, nbytes = write_shard(args.data_dir, i, args.shard_size)
        now = time.perf_counter()
        gap_ms = (now - t_prev) * 1000.0
        elapsed = now - t_start
        total_bytes += nbytes

        print(f"{i+1:>4} {os.path.basename(path):<20} "
              f"{human_bytes(nbytes):>10} "
              f"{gap_ms:>8.1f} {elapsed:>10.2f}", flush=True)
        t_prev = now

        # Sleep to emulate network cadence. Skip after the last shard so
        # DONE appears promptly.
        if i < args.num_shards - 1:
            time.sleep(args.interval)

    # ── signal end of stream ─────────────────────────────────────────────
    done_path = os.path.join(args.data_dir, 'DONE')
    open(done_path, 'w').close()

    wall = time.perf_counter() - t_start
    samples = args.num_shards * args.shard_size

    print("-" * 72, flush=True)
    print(f"PRODUCER DONE  shards={args.num_shards}  samples={samples}  "
          f"bytes={human_bytes(total_bytes)}", flush=True)
    print(f"               wall={wall:.2f}s  "
          f"avg_rate={samples/wall:.1f} samp/s  "
          f"{human_bytes(total_bytes/wall)}/s", flush=True)
    print(f"               sentinel={done_path}", flush=True)
    print("=" * 72, flush=True)


if __name__ == '__main__':
    main()
