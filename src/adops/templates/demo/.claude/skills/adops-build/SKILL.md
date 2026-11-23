---
name: adops-build
description: Build or change a Pipeline (and, when the Project has a dbt/ folder, its dbt models) - looks at the Project, finds and reads the right files, writes the change and checks it. Use it when someone wants to add a Source table, change a Pipeline's Steps or Schedule, or fix a Pipeline file that does not validate.
---

# Build or change a Pipeline

The person you are helping may never have written YAML or SQL. Do the reading and writing
yourself, and before each step say in one plain sentence what you are doing and why. Show the
person what you are about to change before you write it, and explain any problem in plain words
rather than pasting raw output.

## Where you can write, and where you cannot

`mcp__adops__edit_file` writes only files under `pipelines/` and `dbt/`. Everything else is
refused on purpose:

- `adops.yaml` holds the Project settings: Environments, Warehouses, Sources and targets. You can
  read it, but you never change it. When a change needs a new Source, a new connection or a new
  Environment setting, tell the person exactly which lines to add to `adops.yaml` and ask them to
  make that edit.
- `.env` holds the passwords. You never read it, and you never change it through
  `mcp__adops__edit_file`. If a new Source needs a password, ask the person to add it to `.env`
  themselves.
- `.github/` holds the GitHub workflows. Leave them as they are.

dbt models exist only when the Project has a `dbt/` folder. A new Project that was not made from
the demo has none: there, only the Pipeline files under `pipelines/` apply, and you skip every dbt
step below unless the person asks for dbt and adds it to the Project themselves.

## 1. Look at the Project

Say: "First I'm looking at what this Project already has, so the change fits in."

- Call `mcp__adops__project_view`. It lists every Pipeline with its Steps and Schedule, and every
  Environment. Passwords are never shown, only whether each one is set or missing.
- If its `problems` list is not empty, some Pipeline files do not load yet. Tell the person which
  files they are; fixing them is often the first job.

## 2. Find the files to change

Say: "Now I'm finding the files that hold the part you want to change."

- Call `mcp__adops__list_files` to see `adops.yaml` and every file under `pipelines/` and `dbt/`.
  Pass a `directory` such as `pipelines` to list only that folder.
- Check whether any path starts with `dbt/`. If none does, this Project has no dbt models.

## 3. Read them

Say: "I'm reading the current files so I change only what is needed."

- Call `mcp__adops__read_file` with each path you need, for example `pipelines/shop_sync.yaml`
  or `adops.yaml`. Any password value found in the text is shown as `***`.
- Read `adops.yaml` whenever the change names a Source, a Warehouse or a target, to be sure the
  name already exists there.

## 4. Write the change

Say what you will change in plain words, for example: "I'll add the `refunds` table to the
`shop_sync` Pipeline's Sync Step, so it is copied into the Warehouse like `orders` is."

- Call `mcp__adops__edit_file` with the Project-relative path and the whole new file content.
  It replaces the file, so always start from the text you just read and change only the lines
  that need it. It creates the file and its folders when they do not exist yet.
- Write only under `pipelines/` and, when the Project has a `dbt/` folder, under `dbt/`.

## 5. Check the change

Say: "Now I'm checking the files are valid. This does not touch any database."

- Call `mcp__adops__validate`, passing the Pipeline's name to check just that one, or nothing
  to check every Pipeline and `adops.yaml`.
- If `valid` is false, read each entry in `problems` and explain it in plain words: which file,
  which line or Step, and what is wrong. For example, "the Sync Step names a Source called
  `shopp`, but `adops.yaml` only has `shop`, so this is probably a typo."
- Fix the problem with steps 3 and 4 and call `mcp__adops__validate` again. Repeat until it
  passes. If the fix belongs in `adops.yaml`, tell the person the exact lines to change instead.
- `skipped` lists checks that could not run; mention them, but they are not failures.

When validation passes, tell the person the change is ready and that the next step is to try
it in dev with the adops-test-in-dev Skill.
