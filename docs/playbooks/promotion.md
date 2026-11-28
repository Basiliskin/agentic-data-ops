# Promotion setup: from a pull request to stage and prod

Promotion moves a reviewed change from a Branch Environment to stage and then to prod, through Git.
`adops init` writes three GitHub Actions workflows into your Project that do this for you. They only
work after the one-time setup below, which adops cannot do for you because it needs admin rights on
your GitHub repository and on your Server machines.

You do not need to know Git or Docker to follow this guide: every step is a command to copy and paste,
or a setting to click. Your Assistant can run the commands for you.

## What the workflows do

`adops init` writes these files into `.github/workflows/` of your Project:

| File | Runs when | What it does | Where it runs |
|---|---|---|---|
| `adops-pull-request.yml` | a pull request is opened or updated (`opened`, `synchronize`, `reopened`) | `adops validate`, then `adops run --all --branch <branch>`: every Pipeline in that pull request's Branch Environment | the stage Server (runner label `adops-stage`) |
| `adops-pull-request.yml` | a pull request is closed or merged (`closed`) | `adops drop-branch <branch>`: removes that Branch Environment | the stage Server (runner label `adops-stage`) |
| `adops-stage.yml` | a change is merged to `main` | validates the new version, copies it into the stage Server's `releases/<commit>` and points `current` at it (`adops server-release`, which also deletes old release folders), reloads the Server's Pipelines and Schedules, then `adops run --all --changed --env stage` from `current`, which reruns only the Changed Steps | the stage Server (runner label `adops-stage`) |
| `adops-prod.yml` | a release is published | the same as stage, for the release's tag, then `adops run --all --changed --env prod` from `current`, after a required reviewer approves | the prod Server (runner label `adops-prod`, environment `prod`) |

A pull request from a **fork** (a copy of your repository under someone else's account) is a
**Fork PR**. It only gets `adops validate`, on a machine GitHub provides, with no access to your
Servers or secrets. Its Branch run is skipped; to run it, a maintainer pushes the same change to a
branch in your own repository.

**A deploy reruns only what changed.** For each Pipeline, `--changed` reruns from the first Step
whose definition changed since that Environment's last successful run, plus every Step after it. A
Pipeline with no changed Step is not run, and its Steps show `unchanged in stage` (or `prod`) in the
log; if it has a Schedule, its next scheduled run refreshes its data. A Pipeline runs in full when it
has no Schedule, when that Environment has no saved Step Fingerprints for it yet (its first run
there, or on prod its first deploy after upgrading adops), when that Environment's block in `adops.yaml` (or
the time zone or alert webhook) changed, or when it has a new accepted-change record. To rerun
everything by hand, run this in the Server folder on that machine (use `--env prod` on the prod
Server):

```sh
docker compose --profile server run --rm --no-deps dagster_daemon adops run --all --env stage --project /project/current
```

**The first deploy to prod after upgrading adops** creates two new tables in prod's `adops` schema:
`_adops_step_fingerprints` and `_adops_deploy_state`. That deploy runs every Pipeline in full, because
prod has no saved fingerprints yet; later deploys rerun only the Changed Steps. The prod Warehouse
user already writes to that schema, so no new rights are needed.

If a validate fails, no file in the Server folder changes and the Server keeps running the last good
version. A deploy never runs `git` in the Server folder and never restarts the Server's containers.

## Before you start

You need:

- **One machine per Server**: one for stage and, when you add prod, a separate one for prod. A
  Server whose Warehouse is a DuckDB file must run Linux. Its Warehouse lock (see below) uses file
  locks, and these are not reliably shared between macOS and a Docker container through a Docker
  Desktop for macOS folder mount, so two runs could write to a DuckDB Warehouse at once. A Server
  with a Postgres Warehouse may run on a Mac: this was tried on OrbStack, not on Docker Desktop. See
  [Run stage and prod on a Mac](run-stage-and-prod-on-a-mac.md).
- Docker with the `docker compose` command on each machine.
- Admin rights on the GitHub repository that holds your Project.
- A **private** GitHub repository. GitHub's own advice is that self-hosted runners "should almost
  never be used for public repositories": anyone can open a pull request against a public
  repository, and a workflow change in that pull request could run their code on your Server, next
  to your Warehouse and its passwords
  ([GitHub: Hardening for self-hosted runners](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions#hardening-for-self-hosted-runners)).
  If your repository must be public, the fork approval setting in step 4 is the only thing standing
  between a stranger's pull request and your Server.

In the commands below, replace these placeholders once, everywhere they appear:

- `<your-server-folder>`: the folder on the Server machine that holds the Project, for example
  `/srv/adops`. It is the same value you give `ADOPS_SERVER_DIR` in step 3.
- `<runner-user>`: the Linux user the runner runs as, for example `github-runner`.
- `<owner>/<repo>`: your GitHub repository, for example `acme/analytics`.

## Step 1 — Put the Project on the Server machine as a Git clone

Clone your repository once to get its files onto the machine. The workflows never run `git` in this
folder: each deploy copies the new version into its own folder under `releases/`.

```sh
sudo mkdir -p <your-server-folder>
sudo chown <runner-user> <your-server-folder>
sudo -u <runner-user> git clone https://github.com/<owner>/<repo>.git <your-server-folder>
```

For a private repository, `git clone` asks you to sign in once. The workflows never fetch from GitHub
on the Server: they hand the new version over from the runner's own checkout.

Then create the Server's `.env` from the example and fill in the real passwords for this machine's
Warehouse, Sources and Targets:

```sh
cd <your-server-folder>
sudo -u <runner-user> cp .env.example .env
sudo -u <runner-user> nano .env
```

`.env` and the `.adops/` folder stay in `<your-server-folder>` and are never committed: the Project's
`.gitignore` already lists both. **Warehouse secrets live only in each Server machine's `.env`. Never
add them as GitHub secrets.** Make sure `.env` sets `DAGSTER_WEBSERVER_PORT` (the example sets
`3000`): the stage and prod workflows read it to reload the Server. Set `ADOPS_ENVIRONMENT` to the
Environment this machine runs: `stage` on the stage machine (the example's value) and **`prod` on the
prod machine**. A prod machine left at `stage` runs stage's Schedules while its workflow deploys prod.

**Plain Projects only:** a Project made with `adops init` without `--demo` defines only the `dev`
Environment. Add a `stage` Environment to `adops.yaml` (same shape as `dev`, pointing at the stage
Warehouse) and merge it before you start the Server, or the Server and `adops-stage.yml` both fail.
For prod, step 6 below comes first.

Before you start the Server, lay the folder out as release folders: do steps 2 and 3 of
[One-time move to release folders](#one-time-move-to-release-folders). The Server reads the Project
from the `current` link, so it does not start without it.

Start the Server once:

```sh
cd <your-server-folder>
sudo -u <runner-user> docker compose --profile server up -d --build
```

## Step 2 — Install a self-hosted runner on the Server machine

A **self-hosted runner** is GitHub's small agent program that runs workflow jobs on your own machine.
Each Server machine gets one, with a label that says which Server it is:

| Server | Runner label |
|---|---|
| stage | `adops-stage` |
| prod | `adops-prod` |

1. On GitHub, open your repository, then **Settings > Actions > Runners > New self-hosted runner**.
   Choose **Linux** and your machine's architecture.
2. On the Server machine, as `<runner-user>`, run the download and `config.sh` commands GitHub shows.
   When `config.sh` asks for additional labels, enter `adops-stage` on the stage machine or
   `adops-prod` on the prod machine. You can also pass the label directly:

   ```sh
   ./config.sh --url https://github.com/<owner>/<repo> --token <token-from-the-github-page> --labels adops-stage
   ```

   `<token-from-the-github-page>` is the one-time token shown on that GitHub page; it expires after
   one hour.
3. Install and start the runner as a service, so it keeps running after you log out and after a
   reboot (from the runner's folder):

   ```sh
   sudo ./svc.sh install <runner-user>
   sudo ./svc.sh start
   sudo ./svc.sh status
   ```

   ([GitHub: Configuring the self-hosted runner application as a service](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service))
4. Check that the runner user can use Docker. Every job runs adops inside the Server's image with
   `docker compose run`, so a runner user without Docker access fails every job:

   ```sh
   sudo -u <runner-user> docker compose version
   sudo -u <runner-user> docker ps
   ```

   If `docker ps` says "permission denied", add the user to the `docker` group, then restart the
   runner service:

   ```sh
   sudo usermod -aG docker <runner-user>
   sudo ./svc.sh stop
   sudo ./svc.sh start
   ```

**A Mac stage Server** (Postgres Warehouse only, see "Before you start"): choose **macOS** on the
GitHub page, run `config.sh` as yourself, and skip the `sudo` and `usermod` parts above: the runner
runs as your user, who can already use Docker. `./svc.sh install` sets it up as a launch agent that
starts when you log in, or run `./run.sh` in a terminal for a trial.

The runner shows as **Idle** under **Settings > Actions > Runners** when it is ready. Use the label
exactly as written (`adops-stage`, not `stage`): a job whose label matches no runner waits forever.

## Step 3 — Set the Server folder variables on GitHub

The workflows read the Server's folder from a GitHub **variable** (not a secret) named
`ADOPS_SERVER_DIR`. It is set twice, because stage and prod are different machines:

1. **For stage — a repository variable.** **Settings > Secrets and variables > Actions > Variables >
   New repository variable**. Name: `ADOPS_SERVER_DIR`. Value: the stage machine's
   `<your-server-folder>`.
2. **For prod — an environment variable on the `prod` environment** (created in step 5). On the
   `prod` environment page, under **Environment variables**, add `ADOPS_SERVER_DIR` with the prod
   machine's `<your-server-folder>`. A job that uses the `prod` environment reads this value instead
   of the repository variable with the same name.

**Optional — `ADOPS_PACKAGE`.** Only the Fork PR validate job installs adops itself, by default the
exact version your Project was made with (`adops==<version>`). To make it install something else, such
as a wheel file URL or a Git URL, add a repository variable `ADOPS_PACKAGE` with that pip install
target. Leave it unset otherwise.

## Step 4 — Keep fork pull requests from running on your Server

The workflow's fork check (`head.repo.full_name != github.repository`) sends Fork PRs to a GitHub
machine. **That check is not a security boundary on its own.** A Fork PR can change the workflow file
itself, and its changed version is what runs. What stops fork code from reaching your Server is
GitHub's approval setting, which makes a maintainer look at a Fork PR before any workflow runs for it.
Keep it on at its strictest:

Open **Settings > Actions > General**, then:

- **Private repository:** under **Fork pull request workflows**, GitHub leaves **Run workflows from
  fork pull requests** off for a new private repository, so no Fork PR runs anything. That is the
  safest setting; keep it unless you need Fork PRs. If you turn it on, also turn on **Require approval
  for fork pull request workflows**, and leave **Send secrets to workflows from pull requests** off.
- **Public repository:** under **Approval for running fork pull request workflows from
  contributors**, choose **Require approval for all external contributors** (older GitHub pages call
  it "Require approval for all outside collaborators"). Do not choose the "first-time contributors"
  options: they let anyone who once had a pull request accepted run code on your Server without
  approval.

Before you approve a Fork PR's workflow run, check that it does not change anything under
`.github/workflows/`.

([GitHub: Managing GitHub Actions settings for a repository](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository))

## Step 5 — Create the prod environment with required reviewers

A release deploys to prod only after a person approves it.

1. **Settings > Environments > New environment**. Name it exactly `prod`, then **Configure
   environment**.
2. Tick **Required reviewers** and add the people or teams who may approve a prod deploy (up to 6).
   Save.
3. On the same page, add the `ADOPS_SERVER_DIR` environment variable from step 3.

When a release is published, the `adops-prod.yml` job **waits** until one of those reviewers clicks
**Review deployments > Approve and deploy** on the workflow run. Without required reviewers, every
published release deploys to prod at once.

On GitHub Free, Pro and Team, required reviewers work only on public repositories; a private
repository needs GitHub Enterprise for them. (Tried 2026-11-27: on a private repository GitHub
refuses with "Please ensure the billing plan supports the required reviewers protection rule", so
create the environment without reviewers.) Without them, limit who can publish releases instead:
only people with write access to the repository can.

([GitHub: Managing environments for deployment](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment))

## Step 6 — Turn on prod in adops.yaml

A plain Project's `adops.yaml` ships with the prod Environment commented out. Before the first
release, uncomment the `# prod:` block (remove the `# ` at the start of each of its lines), point it
at the prod machine's Warehouse, Sources and Targets, and merge that change. A demo Project ships a
prod that runs against the local Docker databases instead: point its `host`, `port` and `database`
lines at the prod machine before the first release, and merge that change. The prod machine's
`.env` holds the matching passwords.

## Step 7 — Try it

The first push of a new repository to `main` starts no `adops stage` deploy, so it needs no runner
yet. Until stage has run once, a pull request's Branch run has nothing in stage to compare with and
rebuilds every Step of each Pipeline. Stage first runs when that first pull request is merged; from
then on a Branch run rebuilds only the Steps that changed. A repository created on GitHub with a
README and then pushed to is not a first push: that push may still start `adops stage`, so if no
runner exists yet, cancel that run under **Actions**.

1. Open a pull request from a branch in your repository. `adops pull request` runs on the stage
   Server; its log shows each Pipeline's Branch run.
2. Merge it. A second `adops pull request` run drops the Branch Environment, and `adops stage` deploys
   to stage.
3. Publish a release. `adops prod` waits for approval, then deploys to prod.

## One-time move to release folders

From this version on, a deploy never changes the files a run in progress is reading. It copies the
new version into its own folder, `<your-server-folder>/releases/<commit>`, then points a link named
`current` at it in one step. A run reads the folder `current` named when it started, to the end,
retries included. `adops server-release` does this, and it stops with
`... has the old layout ...` or `... is half moved ...` until the Server folder has been moved once,
by hand, as below. It changes no file when it stops.

1. **Make sure no run is in progress.** On the Dagster page, open **Runs**: nothing may be Queued,
   Starting or Started. Wait for it to finish if it is.
2. Copy the version the Server runs now into its release folder, and link the shared `.env` and
   `.adops/` into it:

   ```sh
   cd <your-server-folder>
   commit=$(sudo -u <runner-user> git rev-parse HEAD)
   sudo -u <runner-user> mkdir -p .adops "releases/$commit"
   sudo -u <runner-user> sh -c "git archive HEAD | tar -x -C releases/$commit"
   sudo -u <runner-user> ln -s ../../.env "releases/$commit/.env"
   sudo -u <runner-user> ln -s ../../.adops "releases/$commit/.adops"
   ```

3. Create the `current` link. It must be **relative** (`releases/<commit>`, not a path starting with
   `/`): the containers see the Server folder at `/project`, so an absolute link would point at
   nothing inside them, and `adops server-release` refuses it.

   ```sh
   sudo -u <runner-user> ln -s "releases/$commit" current
   ```

4. Keep `.env`, `.adops/`, `server/`, `docker-compose.yml` and `Dockerfile` where they are, at the
   root of `<your-server-folder>`. Every version shares them. The other Project files at the root are no
   longer read; you may leave them.
5. Point the Server at `current`. A Project made with this version of `adops init` already has
   `ADOPS_PROJECT: /project/current` in `docker-compose.yml`; skip to the command below. In an
   older one, under `dagster_daemon:` → `environment:`, change `ADOPS_PROJECT: /project` to
   `ADOPS_PROJECT: /project/current`. Make the same change in your Project's `docker-compose.yml`
   and merge it, so the next deploy does not stop with "This commit changes the Server config".
   Then recreate the containers once (no run is in progress, step 1):

   ```sh
   sudo -u <runner-user> docker compose --profile server up -d
   ```

6. Check it with the next deploy: its log shows `current: releases/<commit>` for the new commit.

## When a deploy changes the Server config

`docker-compose.yml` and the `server/*.yaml` files at the root of `<your-server-folder>` are shared by
every release and read when the containers start, so a deploy never changes them. When a commit
adds, removes or changes one of them, `adops server-release` stops with
`This commit changes the Server config: <files>` and changes no file. Copy them by hand:

1. Make sure no run is in progress (Dagster page → **Runs**), because the next command restarts the
   Server, and a restart kills runs in progress.
2. On the Server machine, bring each named file to that commit's version:

   ```sh
   cd <your-server-folder>
   sudo -u <runner-user> git fetch origin
   sudo -u <runner-user> sh -c 'git show <commit>:docker-compose.yml > docker-compose.yml'
   sudo -u <runner-user> sh -c 'git show <commit>:server/<file>.yaml > server/<file>.yaml'
   sudo -u <runner-user> docker compose --profile server up -d
   ```

   For a file the commit removed, delete it from `server/` instead.
3. Open the deploy that stopped and click **Re-run jobs**.

### Which release folders a deploy deletes

After switching `current`, `adops server-release` deletes old release folders, keeping the newest 3,
the one `current` names, and every folder a run in progress is reading. Each run records its folder
when it starts. Two cases:

- A run that has not recorded a folder yet (it is queued, or it started before this version of
  adops) may be reading any folder, so **nothing is deleted** while it is in progress, and the log
  says `Old release folders were kept`. The next deploy after it finishes deletes them.
- Branch runs read the pull request's checkout, not a release folder, so they keep no release
  folder.

Each release folder is a full copy of the Project without `.git`, `.env` and `.adops/`, so the Server
folder needs room for about 4 of them (the newest 3 plus the one being copied), more while long runs
hold older ones. Check with `du -sh <your-server-folder>/releases`.

## After a deploy: Schedules pick up the change by themselves

The stage and prod workflows reload the Server's Pipelines and Schedules after `current` points at
the new release. A run already in progress keeps reading the release folder it started with, to the
end, retries included; runs that start after the switch read the new one. The Dagster page shows the new version at once. The Dagster daemon, which starts
scheduled runs, picks up new, removed or changed Schedules within about 60 seconds, because it
refreshes its own copy every 60 seconds. **Do not restart the Server to pick up a change**: a restart
kills runs that are in progress, which the reload exists to avoid.

## When two runs want the same Warehouse: the Warehouse lock

Every run that writes to a Warehouse (a Branch run, a stage or prod deploy, a Schedule, and
`adops drop-branch`) first takes that Warehouse's **Warehouse lock**, one Pipeline at a time. Branch
runs and drop-branch use stage's Warehouse, so they share stage's lock. If another run holds it:

- The new run **waits up to 30 minutes**, then fails with
  `Another run is using the stage Warehouse; gave up after 30 minutes.` (or `prod`). It is not retried.
- A **scheduled run on the Server holds the Server's single run slot while it waits**, so other
  Schedules on that Server queue behind it. A long Branch run can therefore delay stage's Schedules.
- On the Server, a timed-out run fails and **sends an alert** like any other failure (when
  `ALERT_WEBHOOK_URL` is set in `.env`). In GitHub, the job turns red with the message above; rerun it
  with **Re-run jobs** once the other run has finished.
- `adops run --all` takes the lock once per Pipeline, so another run may slip in between two Pipelines
  of the same deploy.

## When two branches get the same Branch Environment

A Branch Environment's name is the branch name lower-cased, with every run of characters other than
letters and digits turned into `_`. So `feature/new-orders` and `feature-new/orders` both become
`feature_new_orders`, and two open pull requests on those branches **share one Branch Environment**:
each Branch run overwrites the other's tables, and closing either pull request drops the Branch
Environment of **both**.

To recover:

1. Rename one of the branches so the names differ after that rule (for example `feature/new-orders-2`)
   and open a new pull request from it; close the old one. Closing it drops the shared Branch
   Environment.
2. On the other pull request, open its latest `adops pull request` run and click **Re-run jobs**, so
   its Branch Environment is built again.

To clean up by hand instead, run this on the stage Server machine, with `<branch>` being either branch
name, then rerun the remaining pull request's workflow as in step 2:

```sh
cd <your-server-folder>
sudo -u <runner-user> docker compose --profile server run --rm --no-deps dagster_daemon adops drop-branch <branch>
```

## Recorded trials

| Date | Where | Result |
|---|---|---|
| 2026-11-26 | Stage Server on a Mac (OrbStack, Postgres Warehouse), runner run with `./run.sh`, a fresh private demo repository, Server image built from a wheel | **PR → Branch run, merge → stage and drop on close all passed.** The first deploy failed at the reload step: the workflows asked Dagster for the location `adops.interface.server`, but Dagster names it `adops.interface.server:defs`. Fixed the same day. After that, merge → stage passed (22 s), a pull request adding a dbt model got a Branch run that deferred the unchanged Sync to stage and built only the Transform (17 s), and closing it dropped its schemas. Not yet tried: release → prod, a Linux runner as a service, root-owned `.adops/` files, a reload during a run in progress. |
| 2026-11-27 | Prod Server on an AWS `m7i-flex.large` VM (Ubuntu 24.04, 2 cores, 8 GB), runner installed as a systemd service, a fresh private demo repository, Server image built from a wheel | **Release → prod passed.** `v0.1.0` made the first prod deploy (26 s; prod Warehouse built, 20 rows written to `shop_prod`). `v0.2.0` added a dbt model and a `schedule:` line: the deploy updated the folder although Dagster had left root-owned files in `.adops/` and `server/logs` (Git ignores them), the reload picked up the new Schedule as RUNNING without a container restart, and prod built the model (25 s). Required reviewers were refused on the private repository (GitHub Free). Not tried: a deploy while a run is in progress. |

Both trials used the earlier deploy, which moved the Server folder with `git checkout` and so could
change files under a run in progress. The release-folder deploy (copy into `releases/<commit>`, switch
`current`) is built for that case; no real-Server trial of it is recorded yet.
