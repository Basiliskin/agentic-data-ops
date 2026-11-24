---
name: adops-share
description: Share a finished change - checks the Project has a stage Environment, puts the change on its own Git branch, commits only the Project's own files, opens a pull request on GitHub and runs the change in its Branch Environment. Use it once a Pipeline works in dev and the person wants it reviewed.
---

# Share a change through a pull request

A change reaches the shared stage and prod Environments only through Git: it goes on its own
branch, someone reviews it in a pull request, and merging that pull request is what sends it on.
You do all the Git work in the shell, because the adops tools have no Git commands. Before each
step, say in one plain sentence what you are doing and why. Explain results in plain words
rather than pasting raw output.

Run all commands from the Project folder (the folder that holds `adops.yaml`).

## 1. Check that the Project has a stage Environment

Say: "A shared change is tried in its own copy of stage, called a Branch Environment, so I'm
first checking that this Project has a stage Environment set up on this computer."

Do this before any Git command or adops tool call.

- Read `adops.yaml`. Look under `environments` for an entry named `stage` that has a
  `warehouse` block. A Branch Environment keeps its tables in the stage Warehouse, in schemas
  of its own, so without stage there is nowhere to run it.
- If that stage Warehouse names a `password_env`, note the variable name it gives. Never assume
  the name; take it from `adops.yaml`. Check that `.env` sets it, without showing the value:
  `grep -c '^<VARIABLE>=.' .env` must print 1 or more.
- If `stage` is missing, as in a Project not made from the demo, or its password variable is
  not set in `.env`, stop here. Explain: "This Project has no stage Environment yet. A shared
  change is tried in a Branch Environment, which lives inside the stage Warehouse, so stage
  must exist first." Tell the person exactly what to add: a `stage` entry under
  `environments` in `adops.yaml` with its Warehouse and Sources, and the Warehouse password in
  `.env` under the variable that entry names. `adops.yaml` is read-only for you, so ask the
  person to make that edit, then start this Skill again.

## 2. Give a brand-new repository its starting point

Say: "A pull request shows what changed compared with the main branch, so main needs a first
save to compare against."

- Run `git rev-parse --verify HEAD`. If it succeeds, the repository already has history: skip
  to step 3.
- If it fails, nothing has been saved yet. Make one starting save on main with the files
  `adops init` wrote, so the change itself still goes through review:
  - Check that `.gitignore` lists `.env` and `.adops/`, so passwords and local state can never
    be saved. If either is missing, stop and ask the person to add it.
  - Run `git status --short` and stage each listed path by name with `git add <path>`, for
    example the `.gitignore`, `.mcp.json`, `docker-compose.yml`, `Dockerfile`, `server/`,
    `.claude/` and `.github/` entries. Leave out `pipelines/`, `dbt/` and `adops.yaml`: they
    are the person's work and go in the pull request.
  - Run `git commit -m "Start the adops Project"`.

## 3. Put the change on its own branch

Say: "I'm creating a branch for this change, so main stays untouched until the change is
reviewed."

- Pick a short name in lowercase letters, digits and hyphens that says what the change does,
  for example `add-refunds-sync`. Ask the person if they want a different name.
- Run `git checkout -b <name>` before saving anything. Every save in this Skill after step 2
  happens on this branch, never on main.

## 4. Save only the Project's own files

Say: "Now I'm saving the change. I only save the Pipeline, dbt and settings files, never
passwords or local run data."

- Run `git status` and tell the person in plain words which files changed.
- Stage only these named paths, one `git add` per path that exists: `git add pipelines/`,
  `git add dbt/`, `git add adops.yaml`. Never stage everything at once, and never stage the
  password file or the local `.adops/` folder, even when `git status` lists them.
- If `git status` shows changed files outside those paths that belong to the change, name them
  to the person and stage one only after they agree.
- Run `git commit -m "<one line saying what the change does>"`.

## 5. Make sure GitHub has a copy of the repository

Say: "A pull request lives on GitHub, so I'm checking the repository has a GitHub copy to send
the branch to."

- Run `git remote get-url origin`. If it prints a URL, go on to step 6.
- If there is no remote, ask the person before creating anything: "This repository is not on
  GitHub yet. Shall I create a private GitHub repository for it?" Only after they agree, run
  `gh repo create <repository-name> --private --source=. --remote=origin`.
- If you made the starting save in step 2, send main too: `git push -u origin main`. If a stage
  Server is already connected to this repository, this first push of main starts its stage
  update, and it only carries the starting files, so nothing of the change reaches stage.

## 6. Send the branch and open the pull request

Say: "I'm sending the branch to GitHub and opening a pull request so someone can review the
change."

- Run `git push -u origin <name>`.
- Run `gh pr create --base main --head <name> --title "<what the change does>" --body "<why,
  in two or three plain sentences>"`.
- Tell the person the pull request's link from the output.
- Explain what GitHub does next: the pull request workflow checks the Project files and then
  runs every Pipeline of the branch in its Branch Environment on the stage Server. When the
  pull request is closed or merged, the same workflow removes that Branch Environment.

## 7. Try the change in its Branch Environment

Say: "Now I'm trying the change in its own Branch Environment, a private copy inside the stage
Warehouse that nothing else uses."

Every call here names the branch with `branch="<name>"` and no Environment, so the tools act
only on this branch's own schemas.

- Call `mcp__adops__test_connection` with `branch="<name>"`. If a connection fails, explain
  which database it is and stop until it is fixed.
- Call `mcp__adops__run` with the Pipeline's name and `branch="<name>"`, once; do not also call
  `mcp__adops__preview`, which would run the whole Pipeline again. If `succeeded` is true, say
  which Steps ran and how many rows each loaded, and for each Step with a `preview`, how many
  rows it would insert, update and delete in the target. If not, explain the failed Step's
  `error` and each failed check in plain words, as the adops-test-in-dev Skill does, then fix
  the change, save it again as in step 4, send it with `git push`, and repeat this step. The
  new commit updates the same pull request, so do not open another one.

When the pull request has been merged, the adops-promote Skill explains what happens next.
