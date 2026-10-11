# miningcore-live-rigs

**Realtime rig connect / disconnect events for [Miningcore](https://github.com/oliverw/miningcore) pools** — one small Python file, no dependencies.

Miningcore's API refreshes miners and workers on its stats interval and keeps a worker "online" for the whole hashrate window. Result: a dashboard shows a new rig **minutes** after it connects, and keeps a dead rig "online" for **~10 minutes**.

`mcev.py` follows the Miningcore container logs instead and turns them into an event feed:

```
[ts] [I] [pool] [CID] Authorized worker ADDR.worker   -> rig ADDR/worker is online
[ts] [I] [pool] [CID] Connection closed               -> that connection is gone
```

- events within ~1 s of the stratum login / disconnect
- a 20 s grace period, so a miner that reconnects at once (pool switch, network blip) doesn't produce an off/on pair
- replays each log from the container start: the online set is exact after a restart
- several containers, every pool id, `ADDR.worker` and `worker@ADDR` logins

Running in production on **[VEIN Pool](https://veinpool.fr/?lang=en)** (19 coins), where the site toasts *"your rig rig1 is online"* the moment it connects.

## API

```
GET /pools/{id}/events[?since=N]
  -> {"last": N, "online": 3, "events": [{"id", "kind": "rig_online|rig_offline", "miner", "worker", "scheme", "ts"}]}

GET /pools/all/events[?since=N]
  -> {"last": N, "online": {"pool1": 3}, "rigs": {"pool1": ["ADDR|rig1"]}, "events": [... with "pool"]}

GET /health
```

Poll with `?since=<last>` every few seconds; without `since` you get the latest events.

## Run

```bash
MCEV_CONTAINERS=miningcore MCEV_PORT=4020 python3 mcev.py
```

| Variable | Default | |
|---|---|---|
| `MCEV_CONTAINERS` | `miningcore` | comma-separated Docker container names running Miningcore |
| `MCEV_PORT` | `4020` | listens on 127.0.0.1 |
| `MCEV_GRACE` | `20` | seconds before a closed rig is announced offline |

The user running it needs access to `docker logs`. A systemd unit and an nginx snippet are in [`deploy/`](deploy/).

## License

MIT — made by [VEIN Pool](https://veinpool.fr/?lang=en). Issues and PRs welcome.
