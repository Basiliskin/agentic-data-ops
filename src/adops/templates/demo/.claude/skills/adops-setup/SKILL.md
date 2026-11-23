---
name: adops-setup
description: Get a new adops Project ready to use on this computer - checks Docker, free ports, the .env file, git and the GitHub CLI, then starts the local databases and tests the dev connections. Use it the first time someone opens a new adops Project, or whenever Docker or the database will not start.
---

# Set up an adops Project

The person you are helping may never have used Docker, Git or a terminal. Do every step yourself,
and before each one say in one plain sentence what you are checking and why it matters. When a
check fails, stop there: tell the person what is wrong and exactly what they need to do, and do
not go on to the next step until it passes. Never start the databases before every check in
steps 1 to 5 has passed.

Run all commands from the Project folder (the folder that holds `adops.yaml`).

## 1. Docker is installed and running

Say: "Your local databases run inside Docker, so I'm checking that Docker is installed and running."

- Run `docker --version`. If it fails, Docker is not installed: ask the person to install Docker
  Desktop (or Docker Engine on Linux) and tell you when it is done.
- Run `docker info`. If it fails, Docker is installed but not running: ask the person to open
  Docker Desktop and wait until it says it is running, then check again.

## 2. The ports the databases need are free

Say: "Each local database listens on a port of this computer. If another program already uses
that port, the database cannot start, so I'm checking they are free."

- Read the port settings from `.env`. If `.env` does not exist yet, read them from `.env.example`
  instead. Never assume a port number: the values in these files are the only truth.
- In `docker-compose.yml`, look at the services that have no `profiles:` line (those are the ones
  `docker compose up` starts) and note which `..._PORT` variable each `ports:` entry uses.
- For each of those ports, run `lsof -nP -iTCP:<port> -sTCP:LISTEN`. Output means the port is
  taken. Tell the person which program holds it (the command name in the output) and offer two
  fixes: close that program, or pick a free port by changing the value in `.env`. If the port
  belongs to the dev Warehouse or a Source, the same port must also be changed in `adops.yaml`
  under `environments.dev`; ask the person to make that edit, because `adops.yaml` is read-only
  for you.
- If the program holding a port is Docker itself, run `docker compose ps` first: when this
  Project's own database is already listed as running there, the port is busy because of it,
  and that is fine.

## 3. The .env file exists

Say: "Passwords are never written in the Project's settings. They live in a private file called
`.env` that is never shared or committed."

- If `.env` is missing, copy the example with the shell: `cp .env.example .env`.
  You cannot create or change `.env` through the `mcp__adops__edit_file` tool; it refuses that
  file on purpose, so use the shell copy.
- Then ask the person to open `.env` and replace every placeholder password with a real one of
  their own. Do not read, print or repeat the values they type.
- If `.env` already exists, leave it as it is.

## 4. Git and the GitHub CLI are ready

Say: "adops keeps every change in Git and shares it through GitHub pull requests. I'll do the Git
work for you, but these two tools must be installed and signed in."

- Run `git --version`. If it fails, ask the person to install Git.
- Run `gh --version`. If it fails, ask the person to install the GitHub CLI (`gh`).
- Run `gh auth status`. If it says nobody is logged in, ask the person to type
  `gh auth login` themselves and follow its prompts, because it needs their GitHub account.
  Check `gh auth status` again afterwards.

## 5. The folder is a Git repository

Say: "Git needs to track this folder so your changes can be saved and shared."

- Run `git rev-parse --is-inside-work-tree`.
- Only if that fails (the folder is not a Git repository yet), run `git init -b main`.
  If it succeeds, the folder is already tracked: change nothing.

## 6. Start the local databases

Say: "Now I'm starting the local databases in Docker. The first start downloads them, which can
take a few minutes."

- Run `docker compose up --wait`. It returns once every database reports it is healthy.
- If it fails, read the last lines of its output and explain the cause plainly. A port that is
  in use means going back to step 2; a missing variable means a value is empty in `.env`.
- Read `adops.yaml` under `environments.dev` to see where the dev Warehouse and Sources live. In a
  new Project that was not made from the demo, the dev Warehouse (localhost:5432 until the person
  changes it) is not one of the databases `docker compose` starts: the person must provide that
  Postgres themselves and put its password in `.env`. Say so plainly rather than treating it as
  a failure of this step.

## 7. Check the connections and the Project files

Say: "Last, I'm checking that adops can reach the dev databases and that the Project's files are
valid."

- Call `mcp__adops__test_connection` with `environment` set to `"dev"`. It only opens and closes
  each connection; it reads and writes nothing. For each connection that failed, explain which
  database it is (the Warehouse, a Source or a target) and the likely fix: a wrong password in
  `.env`, a database that is not running, or a host or port in `adops.yaml` that does not match.
- Call `mcp__adops__validate`. It checks every Pipeline file and the Project settings without
  touching any database. Explain each problem it reports in plain words and which file it is in.
- When both pass, tell the person the Project is ready and that their next step is to build or
  change a Pipeline.

If anything still fails, keep the Project folder as it is and fix the cause in place: never
suggest deleting the folder or starting over in a fresh one.
