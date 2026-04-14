"""
Streaming consumer for the disaggregated-training experiment.

Watches a shared (tmpfs) directory for shard files published by the
producer, trains on each one as it arrives, deletes it, and loops until a
DONE sentinel appears. The goal is to measure how much of wall-clock time
the GPU spends waiting for data versus doing useful work, which is the
starvation signal the networks project is after.

Lifecycle of a shard
--------------------
  1. wait    block until a shard file appears in DATA_DIR
  2. load    torch.load into pageable host memory
  3. pin     copy host tensors into pinned (page-locked) memory so the
             driver can DMA them to the GPU without an extra staging copy
  4. h2d     async copy host -> device, then torch.cuda.synchronize() to
             prove the bytes are on the GPU before we touch the source
  5. unlink  only now do we remove the file from tmpfs; if any earlier
             step crashes the producer's data is still recoverable
  6. train   forward / backward / step over the on-device tensors
  7. free    drop host references; GPU tensors live until next iteration

Per shard we log:
  wait_ms   time blocked on next_shard() before the file showed up
            plus any synthetic upstream stall injected by --consumer-stall
  load_ms   time spent in torch.load / file decode / host-side materialization
  pin_ms    time spent copying into pinned memory
  h2d_ms    time spent on the host->device copy, measured with CUDA events
  train_ms  time spent on the forward/backward/step loop
  loss      average cross-entropy over the shard
  acc       fraction correct on the shard

At the end we print wall-clock totals, GPU utilization, starvation ratio,
throughput, and min/p50/mean/p95/max distributions for each phase.

Producer contract
-----------------
- Shards are named `<DATA_DIR>/shard_*.pt`, each a dict
  {'X': LongTensor [N, SEQ_LEN], 'y': LongTensor [N]}.
- Shards are published atomically (`.tmp` + rename).
- Empty file `<DATA_DIR>/DONE` signals end of stream.
- Shards are consumed in filename-sorted order.
"""

import argparse
import glob
import os
import re
import statistics
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# ── config ───────────────────────────────────────────────────────────────
DATA_DIR      = os.environ.get('DATA_DIR', '/dev/shm')
DONE_SENTINEL = os.path.join(DATA_DIR, 'DONE')
POLL_INTERVAL = 0.01          # seconds to sleep when no shard is available
VOCAB_SIZE    = 10000
SEQ_LEN       = 1024
EMBED_DIM     = 128
HIDDEN_DIM    = 256
NUM_CLASSES   = 2
BATCH_SIZE    = 1000          # default; overridden by --batch-size
LR            = 1e-3
DEVICE        = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
USE_CUDA      = (DEVICE.type == 'cuda')


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
    p = lambda q: vs[min(n - 1, int(q * n))]
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
    """
    while True:
        raw_shards = glob.glob(os.path.join(DATA_DIR, 'shard_*.pt'))
        if raw_shards:
            shards = sorted(
                raw_shards,
                key=lambda x: int(re.search(r'shard_(\d+)\.pt', x).group(1))
            )
            return shards[0]

        if os.path.exists(DONE_SENTINEL):
            os.unlink(DONE_SENTINEL)
            return None

        time.sleep(POLL_INTERVAL)


def load_and_stage_shard(path, consumer_stall=False):
    """
    Load a shard, copy it onto the GPU, confirm the copy is finished, and
    only then unlink the source file from tmpfs.

    If consumer_stall is True, skip reading the file and generate random
    tensors of shape [BATCH_SIZE, SEQ_LEN] / [BATCH_SIZE] directly. The
    file is still unlinked at the end so the consumer stays in sync with
    the producer's cadence.

    Why this ordering matters
    -------------------------
    `tensor.to(cuda, non_blocking=True)` queues an async DMA on the current
    CUDA stream and returns immediately. The host buffer must stay alive
    until that DMA actually completes, otherwise we'd be racing the copy.
    `torch.cuda.synchronize()` blocks until every queued op on the device
    is finished, which gives us a clean "data is on GPU" boundary. Only
    after that boundary do we delete the file and drop the host tensors.

    On CPU runs there is no GPU, so the "stage" is a no-op and we keep the
    tensors on the host. We still unlink only after staging completes, so
    a crash anywhere up to that point leaves the file recoverable.

    Returns:
        X_dev, y_dev      tensors on DEVICE (or CPU on CPU-only runs)
        nbytes            on-disk shard size
        load_s            host-side load/materialization time (seconds)
        pin_s             time to copy into pinned memory (seconds, 0 on CPU)
        h2d_s             host->device copy time (seconds, 0 on CPU)
        injected_wait_s   synthetic upstream wait injected by --consumer-stall
    """
    nbytes = os.path.getsize(path)

    # ── 1. host load / synthetic stall ──────────────────────────────────
    t0 = time.perf_counter()
    injected_wait_s = 0.0

    if consumer_stall:
        # Skip file I/O entirely; generate random tensors of the expected shape.
        X_host = torch.randint(0, VOCAB_SIZE, (BATCH_SIZE, SEQ_LEN))
        y_host = torch.randint(0, NUM_CLASSES, (BATCH_SIZE,))

        # This is meant to emulate upstream starvation, not local load time.
        injected_wait_s = 0.1
        time.sleep(injected_wait_s)
    else:
        # Read the raw bytes from tmpfs.
        data = torch.from_file(path, shared=False, size=nbytes // 8, dtype=torch.int64)

        # Reconstruct X and y based on the expected shard structure.
        n_elements_x = BATCH_SIZE * SEQ_LEN
        X_host = data[:n_elements_x].view(BATCH_SIZE, SEQ_LEN) % VOCAB_SIZE
        y_host = data[n_elements_x:n_elements_x + BATCH_SIZE].view(BATCH_SIZE) % NUM_CLASSES

    total_host_s = time.perf_counter() - t0
    load_s = max(0.0, total_host_s - injected_wait_s)

    pin_s, h2d_s = 0.0, 0.0

    if USE_CUDA:
        # ── 2. pin host memory ──────────────────────────────────────────
        t1 = time.perf_counter()
        X_host = X_host.pin_memory()
        y_host = y_host.pin_memory()
        pin_s = time.perf_counter() - t1

        # ── 3. async H2D copy, timed with CUDA events ──────────────────
        start_evt = torch.cuda.Event(enable_timing=True)
        end_evt   = torch.cuda.Event(enable_timing=True)
        start_evt.record()
        X_dev = X_host.to(DEVICE, non_blocking=True)
        y_dev = y_host.to(DEVICE, non_blocking=True)
        end_evt.record()

        # ── 4. wait until the copy has actually committed ───────────────
        torch.cuda.synchronize()
        h2d_s = start_evt.elapsed_time(end_evt) / 1000.0  # ms -> s
    else:
        X_dev, y_dev = X_host, y_host

    # ── 5. NOW it is safe to delete the file ────────────────────────────
    os.unlink(path)

    if not consumer_stall:
        del data
    del X_host, y_host

    return X_dev, y_dev, nbytes, load_s, pin_s, h2d_s, injected_wait_s


# ── training ─────────────────────────────────────────────────────────────
def train_on_shard(model, optimizer, loss_fn, X_dev, y_dev):
    """
    Run one pass over a single shard. Returns (avg_loss, correct, total).

    The shard tensors are already on DEVICE, so the DataLoader just slices
    them. We pass pin_memory=False because pinning only makes sense for
    host->device staging, and these tensors are already on the device.
    """
    loader = DataLoader(
        TensorDataset(X_dev, y_dev),
        batch_size=BATCH_SIZE,
        shuffle=True,
        pin_memory=False,
    )
    model.train()
    shard_loss, correct, total = 0.0, 0, 0
    for X_batch, y_batch in loader:
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
    ap = argparse.ArgumentParser(description="Streaming shard consumer")
    ap.add_argument(
        '--consumer-stall',
        action='store_true',
        help='skip file I/O and generate random data instead of reading shards'
    )
    ap.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='samples per shard batch (default: 1000)'
    )
    args = ap.parse_args()

    global BATCH_SIZE
    BATCH_SIZE = args.batch_size

    model     = TextClassifier().to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn   = nn.CrossEntropyLoss()
    n_params  = sum(p.numel() for p in model.parameters())

    # ── banner ───────────────────────────────────────────────────────────
    print("=" * 104, flush=True)
    print(f"CONSUMER  device={DEVICE}  dir={DATA_DIR}  "
          f"batch={BATCH_SIZE}  params={n_params:,}"
          + ("  [consumer-stall: data generated locally]" if args.consumer_stall else ""),
          flush=True)
    print("=" * 104, flush=True)
    print(f"{'idx':>4} {'file':<20} {'bytes':>10} "
          f"{'wait_ms':>9} {'load_ms':>9} {'pin_ms':>8} {'h2d_ms':>8} "
          f"{'train_ms':>9} {'loss':>7} {'acc':>6}", flush=True)
    print("-" * 104, flush=True)

    wait_hist, load_hist, pin_hist, h2d_hist, train_hist = [], [], [], [], []
    shard_idx = 0
    total_seen, total_correct, total_loss_sum, total_bytes = 0, 0, 0.0, 0

    t_run_start = time.perf_counter()

    while True:
        # ── 1. wait for next shard ──────────────────────────────────────
        t_wait_start = time.perf_counter()
        path = next_shard()
        wait_s = time.perf_counter() - t_wait_start
        if path is None:
            break

        # ── 2. load + stage to GPU + unlink (in that order) ─────────────
        X_dev, y_dev, nbytes, load_s, pin_s, h2d_s, injected_wait_s = load_and_stage_shard(
            path, consumer_stall=args.consumer_stall
        )

        # Count synthetic consumer-stall time as starvation wait, not load.
        wait_s += injected_wait_s

        # ── 3. train ────────────────────────────────────────────────────
        t_train_start = time.perf_counter()
        avg_loss, correct, n = train_on_shard(model, optimizer, loss_fn, X_dev, y_dev)
        train_s = time.perf_counter() - t_train_start

        del X_dev, y_dev

        # ── 4. record + log ─────────────────────────────────────────────
        shard_idx      += 1
        total_seen     += n
        total_correct  += correct
        total_loss_sum += avg_loss * n
        total_bytes    += nbytes
        wait_hist.append(wait_s   * 1000.0)
        load_hist.append(load_s   * 1000.0)
        pin_hist.append(pin_s     * 1000.0)
        h2d_hist.append(h2d_s     * 1000.0)
        train_hist.append(train_s * 1000.0)

        print(
            f"{shard_idx:>4} {os.path.basename(path):<20} "
            f"{human_bytes(nbytes):>10} "
            f"{wait_s*1000:>9.1f} {load_s*1000:>9.1f} "
            f"{pin_s*1000:>8.1f} {h2d_s*1000:>8.1f} "
            f"{train_s*1000:>9.1f} "
            f"{avg_loss:>7.4f} {correct/max(n,1):>6.3f}",
            flush=True,
        )

    wall = time.perf_counter() - t_run_start

    # ── summary ──────────────────────────────────────────────────────────
    print("-" * 104, flush=True)
    if shard_idx == 0:
        print("CONSUMER DONE  no shards were seen (was the producer running?)", flush=True)
        print("=" * 104, flush=True)
        return

    total_wait  = sum(wait_hist)  / 1000.0
    total_load  = sum(load_hist)  / 1000.0
    total_pin   = sum(pin_hist)   / 1000.0
    total_h2d   = sum(h2d_hist)   / 1000.0
    total_train = sum(train_hist) / 1000.0

    busy_frac  = total_train / wall if wall > 0 else 0.0
    stall_frac = total_wait  / wall if wall > 0 else 0.0
    io_frac    = (total_load + total_pin + total_h2d) / wall if wall > 0 else 0.0

    print(f"CONSUMER DONE  shards={shard_idx}  samples={total_seen}  "
          f"bytes={human_bytes(total_bytes)}", flush=True)
    print(f"               wall={wall:.2f}s  "
          f"train={total_train:.2f}s ({busy_frac*100:.1f}%)  "
          f"wait={total_wait:.2f}s ({stall_frac*100:.1f}%)  "
          f"io={total_load+total_pin+total_h2d:.2f}s ({io_frac*100:.1f}%)",
          flush=True)
    print(f"               io breakdown: load={total_load:.2f}s  "
          f"pin={total_pin:.2f}s  h2d={total_h2d:.2f}s", flush=True)
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
    summarize("pin",   pin_hist)
    summarize("h2d",   h2d_hist)
    summarize("train", train_hist)
    print("=" * 104, flush=True)


if __name__ == '__main__':
    main()
