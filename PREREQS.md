# Before the workshop

Three steps, about 10 minutes. **Please finish these before the session** — we
have 90 minutes together and none of it should be spent on setup.

**You will need from your facilitator:** the workshop **catalog name**. It will
be in the calendar invite; if you cannot find it, ask before you start step 3.

---

## 1. Confirm you can log in

Open the Strategy Corps workshop Databricks workspace and confirm you can sign
in. If you cannot, tell the facilitator now rather than on the day — workspace
access and Unity Catalog grants take time to sort out and we cannot fix them
during the session.

---

## 2. Clone the workshop repo into your workspace

In the workspace UI: **Workspace → Create → Git folder**, then paste:

```
https://github.com/karthiga19/strategycorps-lakehouse-workshop.git
```

Put it under your own user folder, so you end up with
`/Workspace/Users/<your-email>/strategycorps-lakehouse-workshop`.

---

## 3. Run the setup notebook

Open **`setup/00_setup.py`** — this is the only setup notebook, and it does
everything.

**It takes two passes, and that is expected:**

1. **Run all.** It stops almost immediately and prints
   `WAITING FOR INPUT — this is not an error`. Two input boxes have appeared at
   the top of the notebook. This is normal: the boxes did not exist until that
   cell ran.

   | Box | What to enter |
   |---|---|
   | **1. Catalog** | The workshop catalog name from your facilitator |
   | **2. Schema** | Leave blank — you get your own private schema automatically |

2. **Fill in box 1, then Run all again.** This time it goes all the way through.

Two to three minutes on the second pass. It will:

1. Check your environment and stop with a clear message if anything is wrong —
   **before** creating any data.
2. **Install the Genie Code skills for you** into `~/.assistant/skills/`.
3. Generate synthetic banking files into your Volume (the raw landing zone).
4. Preview the raw files so you can see the deliberate mess.

Note: unlike some workshops, setup does **not** build any tables — you build the
whole medallion live with a pipeline in Leg 1. You want the final cell to say:

```
  ✅ READY FOR THE WORKSHOP. Nothing else to do.
```

A warning or two is fine — those cover optional features (checkpoints, MLflow,
AI functions) and will not block you. **Failures are not fine**, and the
notebook stops rather than continue; each failure line includes a fix hint.

### ⚠️ One manual step at the end

The notebook installs the Genie Code skills, but **you must start a NEW Genie
Code chat thread** for it to pick them up. Skills are discovered when a thread
opens, so an already-open thread will not see them. Hard-refresh the page if
they still do not appear.

### 📸 Then reply to the workshop invite with a screenshot

Screenshot the output of that final cell — it shows your schema and your landed
files. That is how we confirm everyone is ready.

---

## What you do NOT need

- ❌ Databricks CLI
- ❌ Node.js, Python, or anything installed locally
- ❌ A local IDE, Cursor, or Copilot
- ❌ A personal access token

Everything happens in the browser.

---

## Troubleshooting

**It stopped with "WAITING FOR INPUT"**
Working as intended. The input boxes only exist once that cell has run, so the
first pass always stops there. Type the catalog name into box 1 and Run all again.

**Every notebook I open does that**
Yes — widgets belong to a notebook, not the workspace, so each notebook needs the
catalog once. Including notebooks you create during the session.

**"Catalog not found", or a permission error**
Check the spelling first. If it is right, you are missing a grant — tell the
facilitator (a workspace admin needs to grant `USE CATALOG` on the workshop
catalog and `CREATE SCHEMA` within it).

**Genie Code does not see the skills**
Start a genuinely new thread — not a new message in an existing one — then
hard-refresh the page.

**`_generator` import fails**
You probably cloned only a single notebook rather than the whole repo. Re-do
step 2.
