# Demonstration guide

`scripts/demo.sh` runs everything below automatically. Use `DEMO_PAUSE=1 scripts/demo.sh` to pause between steps during a live presentation, or follow this guide by hand. It covers every item in specification §10.

```bash
docker compose up -d --build
alias objstore="python3 client/objstore_cli.py"
objstore cluster                      # expect: 4 UP, replication HEALTHY
```

Keep the dashboard (<http://localhost:8080/>) open on a second screen. It refreshes every 2 seconds and shows worker states, replication counters, chunk placement and the event log.

---

## 1. Object storage: upload, retrieve, verify

```bash
mkdir -p demo-data && echo "hello object store" > demo-data/hello.txt
ID=$(objstore upload demo-data/hello.txt -q)
objstore download $ID -o demo-data/hello.out.txt
cmp demo-data/hello.txt demo-data/hello.out.txt && echo identical
objstore verify $ID demo-data/hello.txt        # compares SHA-256: MATCH
```

Any file type works: try an image, a PDF, an `.mp4` or a `.zip`. The original `Content-Type` is returned on download.

## 2. Chunking and distribution

```bash
head -c 5500000 /dev/urandom > demo-data/big.bin       # 5.5 MB → 6 chunks
BIG=$(objstore upload demo-data/big.bin -q)
objstore locate $BIG
```

Expected: chunks 0–4 are 1.00 MB, chunk 5 is about 0.25 MB, and the worker pairs rotate (worker1+worker2, worker2+worker3, …). `objstore workers` shows the chunk files on every worker.

## 3. Replication

`objstore locate $BIG` shows `2/2` copies per chunk, each on two **different** workers, and a "Copies per worker" summary. `objstore cluster` shows `copies = 2 × chunks` and `replication HEALTHY`.

To see the copies on disk inside the containers:

```bash
for w in worker1 worker2 worker3 worker4; do echo $w; docker exec $w ls /data/chunks | grep -c .chunk; done
```

## 4. Metadata

```bash
ID=$(objstore upload demo-data/big.bin -m owner=alice -m department=research -q)
objstore info $ID                                      # system + custom metadata, no data transfer
objstore set-meta $ID owner=bob reviewed=true --remove department
objstore info $ID                                      # metadata_version 2; checksum and chunks unchanged
objstore set-meta $ID owner=eve --if-match 1           # 409 Conflict (stale version)
```

## 5. Worker failure

```bash
FID=$(objstore upload demo-data/big.bin -q)
objstore locate $FID                 # BEFORE: everything 2/2

docker stop worker2
objstore wait-worker worker2 DOWN    # detected within ~10 s (heartbeat timeout)
objstore locate $FID                 # DURING: some chunks 1/2, "worker2 [healthy/DOWN]"
objstore cluster                     # under-replicated > 0
objstore verify $FID demo-data/big.bin   # still MATCH, served from the other copies

objstore wait-healthy                # after the 20 s grace period: re-replicated to other workers
objstore locate $FID                 # 2/2 again on worker1/3/4 (worker2's copies still listed as DOWN)

docker start worker2
objstore wait-worker worker2 UP      # registers → SYNCING → reconciled → UP
objstore wait-healthy $FID           # surplus third copies trimmed
objstore locate $FID                 # AFTER: exactly 2 copies per chunk
objstore events                      # worker_down, replica_created, worker_synced, replica_trimmed
```

The answer to "is the replication factor maintained?" is yes, both during the outage (after the grace period) and after the restart.

## 6. Data integrity

```bash
IID=$(objstore upload demo-data/big.bin -q)
read CHUNK W1 W2 <<< "$(objstore locate $IID --chunk 1)"
scripts/corrupt_chunk.sh $W1 $CHUNK flip         # flip 16 bytes inside the stored chunk
objstore verify $IID demo-data/big.bin           # still MATCH: the bad copy is never returned
objstore scrub > /dev/null                       # integrity scan of every copy
objstore events --level error                    # corruption_detected … on $W1
objstore wait-healthy $IID && objstore locate $IID   # copy restored in place on $W1
```

Other kinds of damage: `scripts/corrupt_chunk.sh <worker> <chunk> truncate` and `… delete`. Detection also happens without a manual scrub: on the next read of the bad copy, through the worker's scrubber (every 60 s) with a heartbeat report, and when a worker rejoins.

If *both* copies of a chunk are corrupted, the download returns `500 data_integrity_error` instead of corrupted bytes.
