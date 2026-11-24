# Run stage and prod on a Mac

The demo Project (`adops init --demo`) ships three Environments that all run against local Docker
databases: dev, stage and prod. Each one has its own Warehouse database (`warehouse`,
`warehouse_stage`, `warehouse_prod`) and its own Reverse Sync target (`shop_dev`, `shop`,
`shop_prod`). This guide shows how to run stage and prod by hand on a Mac, and how to start the
Server so their Schedules run there too.

Setting up Promotion through GitHub (self-hosted runners, the prod approval) is a separate task:
see [Promotion setup](promotion.md).

## Before you start

- **A Postgres Warehouse.** The demo uses one. A Server whose Warehouse is a DuckDB file must run on
  Linux: the Warehouse lock on a DuckDB file is a file lock (`flock`), and a file lock is not reliably
  shared between macOS and a container through a folder mount, so two runs could write to the same
  DuckDB file at once. The Postgres Warehouse lock lives inside Postgres, so it works the same on a Mac.
- **A Docker runtime that supports host networking.** The Server's containers use
  `network_mode: host` to reach the databases on `127.0.0.1`.
  - **OrbStack**: supports host networking with no extra setting. This is what was tried (see
    [Recorded trial](#recorded-trial)).
  - **Docker Desktop 4.34 or later**: turn on **Settings > Resources > Network > Enable host
    networking**, then restart Docker Desktop. This should work, but it has **not been tried**.
- `adops` installed on the Mac, for running Pipelines by hand.

## Step 1 — Start the databases with prod's databases created

The databases create `warehouse_prod` and `shop_prod` only the first time their volumes are made.
If you started the demo before it had a prod Environment, recreate the volumes. This deletes the
demo data in them, and the seed scripts load it again:

```sh
docker compose down -v
docker compose up -d --wait
```

## Step 2 — Run Pipelines in stage or prod

```sh
adops run shop_sync --env stage
adops run shop_sync --env prod
adops run --all --env prod
```

A prod run writes only to `warehouse_prod` and `shop_prod`. The dev and stage databases are left as
they were.

The demo's prod points at the local Docker databases. Before you point it at a real prod machine,
edit `host`, `port` and `database` under `prod:` in `adops.yaml`.

**The Assistant stays on dev and Branch Environments.** Never ask it to run `--env stage` or
`--env prod`. Run those commands yourself, or let the Server and Promotion run them.

## Step 3 — Start the Server for stage or prod

The Server runs each Pipeline on its Schedule, in the one Environment named by `ADOPS_ENVIRONMENT`
in `.env` (`stage` by default). Only Pipelines with a `schedule:` line run. For a quick try, add
`schedule: "* * * * *"` (every minute) as the first line of `pipelines/shop_sync.yaml`.

Build the Server image once, then start it:

```sh
docker compose --profile server build
docker compose --profile server up -d --wait --no-build
```

If you work from an adops source checkout, or the adops version in `Dockerfile` is not on the package
index yet, build the image from a wheel instead. Copy the wheel into the Project folder first, and
build **both** Server services this way. A plain `up` without `--no-build` would rebuild the
webserver without the wheel and fail:

```sh
docker compose --profile server build --build-arg ADOPS_PACKAGE=/build/adops-0.1.0-py3-none-any.whl
```

Open the Dagster page at `http://127.0.0.1:3000`. If another program already uses port 3000, set
`DAGSTER_WEBSERVER_PORT` in `.env` to a free port before starting the Server.

## Step 4 — Switch the Server between stage and prod

Run one Server per machine at a time. Stage and prod Servers use the same Dagster page port and the
same Dagster metadata database (`DAGSTER_PG_DB`). To switch:

1. In the Dagster page, check that no run is in progress.
2. Stop the Server cleanly:

   ```sh
   docker compose --profile server stop dagster_daemon dagster_webserver
   ```

3. In `.env`, set `ADOPS_ENVIRONMENT=prod` (or back to `stage`).
4. Start it again:

   ```sh
   docker compose --profile server up -d --wait --no-build
   ```

`.env` decides which Environment this machine's Server runs. Change it only when you mean to switch,
and set it back when you are done with prod, so the next start does not quietly run prod.

What the trial showed after switching from stage to prod:

- **Run history is shared.** The prod Server's Dagster page listed stage's earlier scheduled run,
  and the `adops run --env prod` run made by hand, next to prod's own run. No label says which
  Environment a run belongs to. Read run history knowing it mixes both.
- **The Schedule carried over.** `shop_sync_schedule` stayed on, in the state the code declares, and
  the prod Server ran it at the next minute without any action. The on/off state lives in the shared
  database, so turning a Schedule off while on stage should leave it off for prod too. The trial did
  not try this.
- **Recovery left stage's runs alone.** On start, `adops server-recover` printed only
  `alerts are off: no webhook URL is set`, and stage's runs stayed `SUCCESS`. That is because the
  stage Server was stopped cleanly with no run in progress. `server-recover` marks every run left
  `STARTING` or `STARTED` as failed, whichever Environment made it. So if the stage Server crashed mid-run and you
  started it as prod, it would mark stage's run failed and send the alert for prod. Stopping cleanly
  avoids this.

If you want separate run history per Environment, give each Environment its own Dagster database:
create a second database in the `dagster_meta` container and change `DAGSTER_PG_DB` in `.env` when
you switch. adops does not set this up for you.

## Recorded trial

Tried on 2026-11-23 on a Mac (macOS 27.2.1, Apple silicon) with **OrbStack 2.2.5, Docker Engine
29.8.2**. Docker Desktop was not tried.

Commands, run from a fresh demo Project (`<repo>` is an adops source checkout):

```sh
cd <repo> && uv build --wheel --out-dir dist
adops init shop --demo && cd shop
cp .env.example .env
cp <repo>/dist/adops-0.1.0-py3-none-any.whl .
docker compose down -v
docker compose up -d --wait
adops run shop_sync --env prod
# Port 3000 was taken on this Mac, so the trial used 3300.
sed -i '' 's/^DAGSTER_WEBSERVER_PORT=.*/DAGSTER_WEBSERVER_PORT=3300/' .env
printf 'schedule: "* * * * *"\n' | cat - pipelines/shop_sync.yaml > p.tmp && mv p.tmp pipelines/shop_sync.yaml
docker compose --profile server build --build-arg ADOPS_PACKAGE=/build/adops-0.1.0-py3-none-any.whl
docker compose --profile server up -d --wait --no-build        # ADOPS_ENVIRONMENT=stage
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3300/server_info
docker compose --profile server stop dagster_daemon dagster_webserver
sed -i '' 's/^ADOPS_ENVIRONMENT=.*/ADOPS_ENVIRONMENT=prod/' .env
docker compose --profile server up -d --wait --no-build        # ADOPS_ENVIRONMENT=prod
docker compose --profile server down -v --rmi local
```

What was observed:

| Check | Result |
|---|---|
| `adops run shop_sync --env prod` from the Mac | `wrote: inserted 20, updated 0, deleted 0` and `Pipeline shop_sync passed in Environment prod.`; `shop_prod.customer_stats` held 20 rows |
| Mac browser/`curl` to the webserver on `127.0.0.1:3300` | HTTP 200 within about 20 seconds after `up` returned |
| Host-networked containers reach Postgres published on `127.0.0.1` | yes: the daemon read and wrote the Dagster database on 15434 and the Warehouse and shop on 15433/15432 |
| Stage Server, scheduled run | run `430c7981-8887-4ea2-87b3-3a2397620338`: `SUCCESS`; `shop.customer_stats` held 20 rows |
| Switch to prod | clean stop, `.env` edited, restart; `server-recover` marked nothing |
| Prod Server, scheduled run | `shop_prod.customer_stats` was emptied before the switch; run `7ccea034-52a4-43f8-b3f4-9d705661fdf5`: `SUCCESS`, and it held 20 rows again |
| Prod Dagster page run list | `7ccea034` (prod, scheduled), `430c7981` (stage, scheduled), `d247c422` (the hand-run prod run), with no Environment label |

No blocker was found.
