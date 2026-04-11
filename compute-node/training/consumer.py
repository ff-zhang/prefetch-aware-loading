"""
Streaming consumer for the disaggregated-training experiment.

Watches a shared (tmpfs) directory for shard files published by the
producer, trains on each one as it arrives, deletes it, and loops until a
DONE sentinel appears. The goal is to measure how much of wall-clock time
the GPU spends waiting for data versus doing useful work, which is the
starvation signal the networks project is after.

Per shard we log:
  wait_ms   time blocked on next_shard() before the file showed up
  load_ms   time spent in torch.load (I/O + deserialization)
  train_ms  time spent on the forward/backward/step loop
  loss      average cross-entropy over the shard
  acc       fraction correct on the shard

At the end we print wall-clock totals, a starvation ratio
(wait / (wait + load + train)), throughput in samples/sec, and
min/mean/median/p95/max for each phase.

Producer contract
-----------------
- Shards are named `<DATA_DIR>/shard_*.pt`, each a dict
  {'X': LongTensor [N, SEQ_LEN], 'y': LongTensor [N]}.
- Shards are published atomically (`.tmp` + rename), so a partial file
  is never visible.
- Empty file `<DATA_DIR>/DONE` signals end of stream.
- Shards are consumed in filename-sorted order.
"""

import glob
import os
import statistics
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# ── config ───────────────────────────────────────────────────────────────
DATA_DIR      = os.environ.get('DATA_DIR', '/tmp')
DONE_SENTINEL = os.path.join(DATA_DIR, 'DONE')
POLL_INTERVAL = 0.01          # seconds to sleep when no shard is available
VOCAB_SIZE    = 10000
SEQ_LEN       = 64
EMBED_DIM     = 128
HIDDEN_DIM    = 256
NUM_CLASSES   = 2
BATCH_SIZE    = 64
LR            = 1e-3
DEVICE        = torch.device('cuda' if torch.cuda.is_available() else 'cpu')


# ── model ─────────────────────────────────────────────────────────────────
class TextClassifier(nn.Module):
    """Small embedding + LSTM + linear head. Kept tiny so the dominant cost
    on a real GPU run is either the shard I/O or the producer cadence,
    which is what the experiment wants to expose."""

    def __init__(self):
        super().__init__()
        self.embed = nn.Embedding(VOCAB_SIZE, EMBED_DIM, padding_idx=0)
        self.lstm  = nn.LSTM(EMBED_DIM, HIDDEN_DIM, batch_first=True)
        self.fc    = nn.Linear(HIDDEN_DIM, NUM_CLASSES)

    def forward(self, x):
        x = self.embed(x)
        _, (h, _) = self.lstm(x)
        return self.fc(h[-1])


# ── helpers ──────────────────────────────────────────────────────────────
def human_bytes(n):
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if n < 1024:
            return f"{n:6.1f} {unit}"
        n /= 1024
    return f"{n:6.1f} TiB"


def summarize(label, values_ms):
    """Pretty-print distribution stats for a list of millisecond samples."""
    if not values_ms:
        print(f"  {label:<8}  (no samples)", flush=True)
        return
    vs = sorted(values_ms)
    n = len(vs)
    p = lambda q: vs[min(n - 1, int(q * n))]  # simple nearest-rank
    print(f"  {label:<8}  "
          f"min {vs[0]:8.1f}  "
          f"p50 {statistics.median(vs):8.1f}  "
          f"mean {statistics.fmean(vs):8.1f}  "
          f"p95 {p(0.95):8.1f}  "
          f"max {vs[-1]:8.1f}  ms", flush=True)


# ── shard acquisition ────────────────────────────────────────────────────
def next_shard():
    """
    Block until a shard is available or DONE is signaled.
    Returns the shard path, or None if the stream is finished.

    The ordering of checks matters: we look for shards first and only then
    check for DONE. That way, if the producer writes the final shards and
    immediately publishes DONE, we still drain everything before exiting.
    """
    while True:
        shards = sorted(glob.glob(os.path.join(DATA_DIR, 'shard_*.pt')))
        if shards:
            return shards[0]
        if os.path.exists(DONE_SENTINEL):
            return None
        time.sleep(POLL_INTERVAL)


def consume_shard(path):
    """
    Load a shard from tmpfs and immediately unlink it.

    We unlink before training so that even if the training step crashes,
    the file is gone and the producer's write budget is freed. Returns
    (X, y, nbytes, load_seconds).
    """
    nbytes = os.path.getsize(path)
    t0 = time.perf_counter()
    data = torch.load(path, weights_only=True, map_location='cpu')
    load_s = time.perf_counter() - t0
    os.unlink(path)
    return data['X'], data['y'], nbytes, load_s


# ── training ─────────────────────────────────────────────────────────────
def train_on_shard(model, optimizer, loss_fn, X, y):
    """
    Run one pass over a single shard. Returns (avg_loss, correct, total).

    A shard is small enough that one DataLoader epoch over it is the right
    granularity. We don't reuse a DataLoader across shards because each
    shard's underlying TensorDataset is different.
    """
    loader = DataLoader(
        TensorDataset(X, y),
        batch_size=BATCH_SIZE,
        shuffle=True,
        pin_memory=(DEVICE.type == 'cuda'),
    )
    model.train()
    shard_loss, correct, total = 0.0, 0, 0
    for X_batch, y_batch in loader:
        X_batch = X_batch.to(DEVICE, non_blocking=True)
        y_batch = y_batch.to(DEVICE, non_blocking=True)
        optimizer.zero_grad()
        out  = model(X_batch)
        loss = loss_fn(out, y_batch)
        loss.backward()
        optimizer.step()
        shard_loss += loss.item() * y_batch.size(0)
        correct    += (out.argmax(1) == y_batch).sum().item()
        total      += y_batch.size(0)
    return shard_loss / max(total, 1), correct, total


# ── main ─────────────────────────────────────────────────────────────────
def main():
    model     = TextClassifier().to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn   = nn.CrossEntropyLoss()
    n_params  = sum(p.numel() for p in model.parameters())

    # ── banner ───────────────────────────────────────────────────────────
    print("=" * 88, flush=True)
    print(f"CONSUMER  device={DEVICE}  dir={DATA_DIR}  "
          f"batch={BATCH_SIZE}  params={n_params:,}", flush=True)
    print("=" * 88, flush=True)
    print(f"{'idx':>4} {'file':<20} {'bytes':>10} "
          f"{'wait_ms':>9} {'load_ms':>9} {'train_ms':>9} "
          f"{'loss':>7} {'acc':>6}", flush=True)
    print("-" * 88, flush=True)

    # Per-shard histories for the final summary.
    wait_hist, load_hist, train_hist = [], [], []
    shard_idx = 0
    total_seen, total_correct, total_loss_sum, total_bytes = 0, 0, 0.0, 0

    t_run_start = time.perf_counter()

    while True:
        # ── 1. wait for next shard ──────────────────────────────────────
        t_wait_start = time.perf_counter()
        path = next_shard()
        wait_s = time.perf_counter() - t_wait_start
        if path is None:
            break  # producer signaled DONE and directory is drained

        # ── 2. load + unlink ────────────────────────────────────────────
        X, y, nbytes, load_s = consume_shard(path)

        # ── 3. train ────────────────────────────────────────────────────
        t_train_start = time.perf_counter()
        avg_loss, correct, n = train_on_shard(model, optimizer, loss_fn, X, y)
        train_s = time.perf_counter() - t_train_start

        # ── 4. record + log ─────────────────────────────────────────────
        shard_idx      += 1
        total_seen     += n
        total_correct  += correct
        total_loss_sum += avg_loss * n
        total_bytes    += nbytes
        wait_hist.append(wait_s * 1000.0)
        load_hist.append(load_s * 1000.0)
        train_hist.append(train_s * 1000.0)

        print(
            f"{shard_idx:>4} {os.path.basename(path):<20} "
            f"{human_bytes(nbytes):>10} "
            f"{wait_s*1000:>9.1f} {load_s*1000:>9.1f} {train_s*1000:>9.1f} "
            f"{avg_loss:>7.4f} {correct/max(n,1):>6.3f}",
            flush=True,
        )

    wall = time.perf_counter() - t_run_start

    # ── summary ──────────────────────────────────────────────────────────
    print("-" * 88, flush=True)
    if shard_idx == 0:
        print("CONSUMER DONE  no shards were seen (was the producer running?)",
              flush=True)
        print("=" * 88, flush=True)
        return

    total_wait  = sum(wait_hist)  / 1000.0
    total_load  = sum(load_hist)  / 1000.0
    total_train = sum(train_hist) / 1000.0
    busy_frac = total_train / wall if wall > 0 else 0.0
    stall_frac = total_wait / wall if wall > 0 else 0.0

    print(f"CONSUMER DONE  shards={shard_idx}  samples={total_seen}  "
          f"bytes={human_bytes(total_bytes)}", flush=True)
    print(f"               wall={wall:.2f}s  "
          f"train={total_train:.2f}s ({busy_frac*100:.1f}%)  "
          f"wait={total_wait:.2f}s ({stall_frac*100:.1f}%)  "
          f"load={total_load:.2f}s", flush=True)
    print(f"               throughput={total_seen/wall:.1f} samp/s  "
          f"{human_bytes(total_bytes/wall)}/s", flush=True)
    print(f"               final_loss={total_loss_sum/total_seen:.4f}  "
          f"final_acc={total_correct/total_seen:.3f}", flush=True)
    print(f"               gpu_utilization (train / wall) = "
          f"{busy_frac*100:.1f}%", flush=True)
    print(f"               starvation ratio (wait / wall) = "
          f"{stall_frac*100:.1f}%", flush=True)
    print("  per-shard distributions:", flush=True)
    summarize("wait",  wait_hist)
    summarize("load",  load_hist)
    summarize("train", train_hist)
    print("=" * 88, flush=True)


if __name__ == '__main__':
    main()
