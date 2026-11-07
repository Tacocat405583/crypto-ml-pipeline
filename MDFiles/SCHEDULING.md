# Running the tracker on a schedule (Windows)

How `src/tracker/coinbase.py` gets run once an hour, unattended, and why it's set up
this way. Written so someone who has never opened Task Scheduler can reproduce it.

---

## The problem

We want one Python script to run every hour, forever, without anyone watching it —
and to keep running after a reboot.

Three ways to do that, and why we picked the third:

| Approach | Why not |
|---|---|
| `while True: ... time.sleep(3600)` | Dies on reboot, on a crash, or when you close the terminal. You'd need something else to restart it — which is the actual problem. |
| Airflow / Celery / a real scheduler | Airflow doesn't run natively on Windows (needs WSL2 or Docker), and the compose file here defines ~14 services. That's a lot of machinery to fire one HTTP GET. |
| **Windows Task Scheduler** | Built into the OS. Survives reboots. One entry. ✅ |

Task Scheduler is the Windows equivalent of cron.

---

## Why there's a `.bat` file

Task Scheduler *can* run `python.exe` directly. We use a wrapper because of one
inconvenient fact:

> **Task Scheduler throws away whatever the program prints.**

The tracker writes diagnostics to stderr — bad payloads, timeouts, HTTP errors. Run
directly, all of that vanishes and the only thing you'd see is a numeric result code.
The wrapper redirects it to a file instead.

### `run_tracker.bat`, line by line

```bat
@echo off
```
Don't echo each command back into the log. Without it every line appears twice.

```bat
cd /d "%~dp0"
```
`%~dp0` is *"the folder this .bat file lives in"* — `d` = drive, `p` = path. So the
script works no matter where it's launched from. `/d` allows changing drive as well as
directory. This is what makes `.venv\Scripts\python.exe` on the next line resolve.

```bat
if not exist "logs" mkdir "logs"
```
Create the log folder on first run so you don't have to.

```bat
".venv\Scripts\python.exe" "src\tracker\coinbase.py" >> "logs\tracker.log" 2>&1
```
The important line.

- **`.venv\Scripts\python.exe`, not `python`** — a scheduled task doesn't inherit your
  terminal's `PATH` or any activated virtualenv. A bare `python` either isn't found or
  is the wrong interpreter without your dependencies. Always name the interpreter
  explicitly.
- **`>>`** appends (`>` would overwrite the log every hour).
- **`2>&1`** means "send stream 2 (stderr) to wherever stream 1 (stdout) is going", so
  errors land in the same file. Order matters: `2>&1` must come *after* the `>>`.

```bat
set RC=%ERRORLEVEL%
...
exit /b %RC%
```
`%ERRORLEVEL%` is the exit code of the last program that ran. We stash it, write the
end-of-run line, then hand it back with `exit /b`.

**Without this the task always reports success**, because the last command in the batch
file would be the `echo`, which always succeeds. Propagating the code is what makes a
failed run show as failed in Task Scheduler.

### How this pairs with the script

`coinbase.py` calls `sys.exit(1)` when it has no valid record to write. So:

```
network fine, data valid   →  row appended  →  exit 0  →  task shows 0x0
anything wrong             →  nothing written →  exit 1  →  task shows 0x1
```

A failed run writes **nothing** to the data file. Missing readings are acceptable;
wrong ones are not.

---

## Creating the task

Open **Task Scheduler** → **Create Task…** in the right-hand Actions panel.

Use *Create Task*, **not** *Create Basic Task* — the basic wizard can't do hourly
repetition and hides the settings that matter.

### General
- **Name:** `CoinbasePriceTracker`
- **Run only when user is logged on** — simplest, no password prompt. Choose *Run
  whether user is logged on or not* if you want it running while logged out; Windows
  will ask for your account password.
- ✅ **Hidden** — with *run only when logged on*, every run flashes a console window on
  screen for a second or two. Ticking *Hidden* suppresses it. It has no effect on the
  logging: stdout and stderr still land in `logs\tracker.log` via the wrapper.

### Triggers → New…
- **Daily**, recur every `1` days
- **Start:** a round hour, e.g. `2:00:00 AM`. With hourly repetition the start time sets
  the minute offset for *every* run — starting at `1:06:42` timestamps your whole series
  at six-minutes-past forever, which makes hour-bucketing in pandas needlessly annoying.
- ✅ **Repeat task every** `1 hour` **for a duration of** `Indefinitely`
  - Both halves matter. The checkbox is off by default, and the duration defaults to
    `1 day`, which stops the repetition after 24 hours.
- **Expire:** optional. A year out is a reasonable backstop against forgetting it exists.

### Actions → New…
- **Action:** Start a program
- **Program/script:** the full path to `run_tracker.bat`
- **Start in (optional):** the repo root

### Conditions
- ❌ **Untick "Start the task only if the computer is on AC power"**

  This is checked by default and it is the most common reason a laptop task silently
  stops working. Every run on battery is skipped — no error, no log line, just a gap.

- **Wake the computer to run this task** — optional. Leaving it off means no readings
  while the machine sleeps.

### Settings
- ❌ **Run task as soon as possible after a scheduled start is missed** — leave this
  **off**. It looks like the right choice (catch up after sleep instead of skipping the
  reading) and it is the single worst setting on this page. See
  *[The catch-up trap](#the-catch-up-trap)* below. Missed readings are cheap; this is not.
- ❌ **If the task fails, restart every…** — leave off. The script already retries 3×
  internally; a second retry layer just confuses the logs.
- **Stop the task if it runs longer than** `1 hour` (default is 3 days).
- ❌ **Stop if the computer switches to battery power** — checked by default, and the
  quiet twin of the AC-power condition above. It kills a run *mid-fetch* if you unplug
  at the wrong moment. Harmless to the data (a half-finished run writes nothing) but it
  shows up as a confusing non-zero result code.
- **If the task is already running:** *Do not start a new instance* — two runs appending
  to the same file at once is exactly the corruption to avoid.

---

## Gotchas

Every one of these was hit while setting this up.

**Don't quote the paths.** Task Scheduler parses a quoted *Program/script* as
"program plus arguments" and pops up a confusing dialog offering to split it — accepting
that leaves the program **empty**. The *Start in* field rejects quotes outright. Neither
field needs them as long as your path has no spaces.

**Watch out for pasted labels.** Copying `Program/script:  C:\...\run_tracker.bat` from
documentation puts the *label* into the field too. The path must start with `C:\`.

**Editing a task in the library root needs elevation.** `Set-ScheduledTask` and
`schtasks /change` both return `Access is denied` from a normal shell. The GUI handles
the elevation prompt for you.

### The catch-up trap

This one took five days to notice, because it degrades instead of breaking.

*Run task as soon as possible after a scheduled start is missed* does fire a catch-up
run when the machine wakes — about three minutes after resume. What the checkbox does
**not** tell you is that the catch-up run becomes a **new anchor for the hourly
repetition**. The original `:00` cadence keeps going; now there's a second one at
whatever minute you happened to wake up. Nothing ever clears it.

So every sleep/wake cycle permanently adds one run per hour:

```
Aug 11  woke --:--       →  runs at :00
Aug 12  woke 14:22       →  runs at :00, :25
Aug 13  woke 11:09       →  runs at :00, :25, :12
Aug 15  woke 23:21       →  runs at :00, :25, :12, :24
Aug 16  woke 12:05       →  runs at :00, :25, :12, :24, :08
```

Five console windows an hour, and `ticker_price.jsonl` quietly collecting 5 rows per
hour at ragged offsets instead of 1 on the hour — which breaks the whole reason the
trigger starts on a round hour in the first place.

**Symptom:** the tracker fires far more often than the trigger says, at offsets that
are stable to the hundredth of a second and match nothing in the task definition.

**Diagnosing it.** Group the log by minute offset — real triggers are few and the
phantoms stand out:

```powershell
Get-Content logs\tracker.log |
    Where-Object { $_ -match '\] start' } |
    ForEach-Object { if ($_ -match '\d{2}:(\d{2}:\d{2})\.\d+\] start') { $matches[1] } } |
    Group-Object | Sort-Object Count -Descending
```

Then confirm each phantom offset against a wake event — they land ~3 minutes after one:

```powershell
Get-WinEvent -FilterHashtable @{
    LogName='System'; ProviderName='Microsoft-Windows-Power-Troubleshooter'
} | Select-Object TimeCreated, @{n='WakeTime';e={$_.Properties[1].Value}}
```

**Fix:** untick the setting, then **disable and re-enable the task**. Turning the
setting off does not retire the cadences that already exist — they live in the running
scheduler service, and only a disable/enable cycle (or a reboot) clears them.

---

## Checking it works

Right-click the task → **Run**, then look at the **Last Run Result** column:

| Code | Meaning |
|---|---|
| `0x0` | Success |
| `0x1` | The script exited 1 — check `logs\tracker.log` |
| `0x41303` | Has not run yet (normal on a new task) |
| `0x2` | File not found — usually a wrong or quoted path |

Or from PowerShell:

```powershell
Get-ScheduledTaskInfo -TaskName 'CoinbasePriceTracker' |
    Select-Object LastRunTime, LastTaskResult, NextRunTime
```

Then confirm the data actually moved — a new line in `src/tracker/ticker_price.jsonl`
with a timestamp matching the run, and a matching `start` / `end rc=0` pair in
`logs/tracker.log`.

---

## Turning it off

Right-click the task → **Disable** (keeps the configuration, stops collecting) or
**Delete**. Disable is the reversible one.
