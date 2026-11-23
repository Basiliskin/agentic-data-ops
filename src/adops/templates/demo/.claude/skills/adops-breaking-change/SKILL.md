---
name: adops-breaking-change
description: Accept a Breaking Change - when a Source column was removed or changed type and a Pipeline stopped because of it, accepts the new shape in dev, saves the accepted-change record on its own Git branch and opens the pull request that lets stage and prod accept it too. Use it when a run fails with "Breaking Change in <table>".
---

# Accept a Breaking Change

A Breaking Change means a column the Pipeline used to copy from a Source table is gone, or now
holds a different type of data. adops stops the Pipeline rather than quietly load data that no
longer looks the way it did. Accepting the change tells adops the new shape is intended: from
then on it stops expecting the dropped or old-typed column, and the Pipeline runs again.

You accept the change in dev only. Stage and prod accept it later, by themselves, when a small
file called an accepted-change record reaches them through a reviewed pull request. You do all
the Git work in the shell, because the adops tools have no Git commands. Before each step, say
in one plain sentence what you are doing and why. Explain results in plain words rather than
pasting raw output.

Run all commands from the Project folder (the folder that holds `adops.yaml`).

## 1. Find out what changed and ask before accepting

Say: "A Source table changed shape, so the Pipeline stopped. I'll explain what changed and ask
you before I accept it."

- Take the Pipeline, the Step and the table from the failed run's `error`, which reads like
  `Step <step>: Breaking Change in <table>: <column> dropped`. A column that changed type reads
  `<column> retyped (<old type> -> <new type>)`.
- Tell the person, in plain words, which table changed and which columns were removed or
  changed type. Say what accepting means: adops will stop expecting those columns in their old
  form, so the Warehouse no longer receives the dropped columns, and nothing in the Source is
  changed.
- If the change looks like a mistake in the Source (for example, someone removed a column by
  accident), suggest fixing the Source instead and stop.
- Ask: "Do you want me to accept this change?" Go on only after the person says yes.

## 2. Make sure the change can go through a pull request

Say: "Stage and prod accept this change only through a reviewed pull request, so I'm checking
the repository is ready for one."

- Run `git rev-parse --verify HEAD`. If it fails, the repository has no starting save yet:
  follow step 2 of the adops-share Skill first, then come back here.
- Run `git status --short` and note which files are already changed, so you can tell them apart
  from the record this Skill writes.

## 3. Put the change on its own branch

Say: "I'm creating a branch first, so the record is saved there and main stays untouched until
the change is reviewed."

- Pick a short name in lowercase letters, digits and hyphens, for example
  `accept-orders-change`. Ask the person if they want a different name.
- Run `git checkout -b <name>` before accepting anything. Every save in this Skill happens on
  this branch, never on main.

## 4. Accept the change in dev

Say: "Now I'm accepting the change in dev, which writes the record that stage and prod will
use later."

- Call `mcp__adops__accept_schema` with the Pipeline's name, the table, and
  `environment="dev"`. Always pass `"dev"`; the tool accepts nothing else.
- Tell the person in plain words what the reply says: the table now has `column_count`
  columns, and the columns in `nullable_columns` may now be empty.
- A Project not made from the demo may have no `pipelines/accepted/` folder yet. That is fine:
  the tool creates it.

## 5. Save the record on the branch

Say: "I'm saving exactly the record this change produced, and nothing else."

- If the reply has a `record_path`, stage that one file by its name: `git add <record_path>`.
  Do not stage the whole `pipelines/accepted/` folder or anything else, because other records
  there may belong to other changes.
- If `record_path` is null, no new record was written, because the records listed in
  `covered_by` already describe every change found. This is not an error, so do not accept
  again. Check those files with `git status --short <file>` and `git log --oneline -- <file>`:
  - a file that `git log` shows was already saved needs nothing more;
  - a file that was never saved (no `git log` output, or `git status` lists it) is staged by
    name with `git add <file>`.
  If every `covered_by` file was already saved, tell the person the change is already on its
  way through Git, and stop here.
- Run `git commit -m "Accept the Breaking Change in <table>"`.

## 6. Send the branch and open the pull request

Say: "I'm sending the branch to GitHub and opening a pull request, because merging it is what
lets stage, and later prod, accept this change."

- Run `git remote get-url origin`. If there is no remote, follow step 5 of the adops-share
  Skill to create one, then come back here.
- Run `git push -u origin <name>`.
- Run `gh pr create --base main --head <name> --title "Accept the Breaking Change in <table>"
  --body "<which columns were dropped or changed type, and why that is intended>"`.
- Tell the person the pull request's link from the output.
- Explain what happens next: once someone merges the pull request, the stage Server picks up
  the record and stage accepts the change on its next run. Prod accepts it the same way after
  the Project's owners publish the next release. Until then, stage and prod keep stopping on
  this change, and that is expected. You never run anything on stage or prod yourself.

## A Branch Environment with the same change

A Branch Environment cannot be accepted through the adops tools. If the same change stops a run
in a Branch Environment, ask the person to run this themselves from the Project folder:

`adops accept-schema <pipeline> <table> --branch <name>`

It accepts the new shape for that branch only and writes no record.
