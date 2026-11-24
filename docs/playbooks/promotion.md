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
| `adops-stage.yml` | a change is merged to `main` | validates the new version, moves the stage Server's Project folder to it, reloads the Server's Pipelines and Schedules, then `adops run --all --env stage` | the stage Server (runner label `adops-stage`) |
| `adops-prod.yml` | a release is published | the same as stage, for the release's tag, then `adops run --all --env prod`, after a required reviewer approves | the prod Server (runner label `adops-prod`, environment `prod`) |

A pull request from a **fork** (a copy of your repository under someone else's account) is a
**Fork PR**. It only gets `adops validate`, on a machine GitHub provides, with no access to your
Servers or secrets. Its Branch run is skipped; to run it, a maintainer pushes the same change to a
branch in your own repository.

If a validate fails, the Server's Project folder is not touched and the Server keeps running the last
good version.

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

The workflows move this folder to each new version with `git`, so it must be a clone of your
repository, not a copied folder.

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

- **Private repository:** under **Fork pull request workflows**, turn on **Require approval for fork
  pull request workflows**, and leave **Send secrets to workflows from pull requests** off.
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
repository needs GitHub Enterprise for them. Without them, limit who can publish releases instead:
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

1. Open a pull request from a branch in your repository. `adops pull request` runs on the stage
   Server; its log shows each Pipeline's Branch run.
2. Merge it. A second `adops pull request` run drops the Branch Environment, and `adops stage` deploys
   to stage.
3. Publish a release. `adops prod` waits for approval, then deploys to prod.

## After a deploy: Schedules pick up the change by themselves

The stage and prod workflows reload the Server's Pipelines and Schedules after moving its folder to
the new version. The Dagster page shows the new version at once. The Dagster daemon, which starts
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
