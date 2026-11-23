---
name: adops-promote
description: After a pull request is merged - removes the change's Branch Environment and explains how the change now moves on to stage and then to prod. Use it when someone's pull request has been merged, or when they ask how a change reaches stage or prod.
---

# After the merge: clean up and explain Promotion

Moving a change on to stage and then prod is called Promotion. You never do it yourself: it
happens on GitHub, started by the people who own the Project. Your part is to tidy up the
change's Branch Environment and explain plainly what happens next. Before each step, say in one
plain sentence what you are doing and why.

Run all commands from the Project folder (the folder that holds `adops.yaml`).

## 1. Check that the Project has a stage Environment

Say: "A Branch Environment lives inside the stage Warehouse, so I'm checking this Project has a
stage Environment before I remove anything there."

Do this before any adops tool call.

- Read `adops.yaml`. Look under `environments` for an entry named `stage` that has a
  `warehouse` block.
- If that stage Warehouse names a `password_env`, note the variable name it gives. Never assume
  the name; take it from `adops.yaml`. Check that `.env` sets it, without showing the value:
  `grep -c '^<VARIABLE>=.' .env` must print 1 or more.
- If `stage` is missing, as in a Project not made from the demo, or its password variable is
  not set in `.env`, stop here. Explain: "This Project has no stage Environment yet. Branch
  Environments live inside the stage Warehouse, so without stage there is no Branch Environment
  to remove and nothing for a merge to promote to." Tell the person what to add: a `stage`
  entry under `environments` in `adops.yaml` with its Warehouse and Sources, and the Warehouse
  password in `.env` under the variable that entry names.

## 2. Confirm the pull request is merged

Say: "I'm checking the pull request was merged, because the clean-up is for finished changes."

- Run `gh pr view <name> --json state,mergedAt`. If `state` is not `MERGED`, tell the person the
  change is still under review and stop: the Branch Environment is still in use.

## 3. Remove the Branch Environment

Say: "The change is merged, so I'm removing its Branch Environment: the private copy of its
tables inside the stage Warehouse."

- Call `mcp__adops__drop_branch` with the branch name the pull request came from.
- Explain the reply: `dropped_schemas` lists the removed schemas, all named `br_<branch>__...`.
  Only this branch's own schemas are removed; stage's own tables are never touched.
- Tell the person this is the same clean-up the pull request workflow does by itself when a
  pull request closes. Doing it here only does it sooner, and it is safe to repeat: if the
  workflow already removed them, `dropped_schemas` is simply empty.
- If the reply says stage's Warehouse is busy, another run is using it. Wait a little and try
  again; nothing was removed.

## 4. Explain how the change moves on

Say what happens next in plain words, and do not start any of it yourself:

- Merging into main already sent the change towards stage. A GitHub workflow picks up every
  merge to main, checks the Project files and updates the stage Server with them. The person
  can watch it in the repository's Actions tab on GitHub.
- prod is updated only when someone publishes a release on GitHub. That starts another workflow
  which, after the reviewers the Project named approve it, updates the prod Server. Publishing
  a release is a decision for the people who own the Project, made on GitHub, never by you.
- If the person asks you to send the change to stage or prod yourself, explain that you only
  work in dev and in Branch Environments, and that stage and prod change only through a merge
  or a published release.

Finish by switching the local copy back to main and bringing it up to date: `git checkout main`
then `git pull`.
