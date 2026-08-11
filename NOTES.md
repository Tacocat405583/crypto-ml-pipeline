### THIS IS FOR DOCUMENTING ISSUES I HAD DURING DEVELOPMENT ##

---

## Coinbase price tracker — init

Started the Coinbase piece in `src/tracker/coinbase.py`.

Thought process:
- Hit the public spot-price endpoint (`/v2/prices/BTC-USD/spot`) with `requests`. No auth
  needed for spot, so it's the simplest thing that gets real data flowing.
- Wrapped it in a `CoinbaseClient` class so the URL lives in one place and I can add
  more endpoints later without rewriting call sites.

### Step-by-step reasoning

1. **Pick the endpoint.** Spot price is a plain GET, no API key / signing. Good first
   target — I can verify the pipeline end-to-end before dealing with authenticated
   endpoints (accounts, orders) that need HMAC headers.
2. **Prove it works inline first.** The commented-out block at the top of the file was
   the throwaway version: `requests.get`, check `status_code`, then `.json()` to see the
   shape. Left it in as a breadcrumb for what the raw response looks like.
3. **Promote to a class.** `CoinbaseClient.__init__` takes the URL and stashes it on
   `self._url`; `get_spot_price()` does the GET + `.json()` and returns the parsed dict.
   The `if __name__ == "__main__"` block is just a smoke test so I can run the file
   directly and eyeball the output.

### Response shape (for reference)

The spot endpoint returns something like:

```json
{ "data": { "amount": "60000.00", "base": "BTC", "currency": "USD" } }
```

So the price lives at `data["data"]["amount"]` (a string — will need casting to float
before any math).

### End goal this week

Call this on a 24h cadence and append each reading into a JSON file, so I build up a
running price history to feed the ML pipeline later.

Plan / open questions:
- **Scheduling.** Options: OS-level cron / Task Scheduler, or a small loop with a
  `time.sleep`, or later an Airflow/cron job in the parked `infra/` layer. Leaning toward
  the simplest scheduler that survives a reboot.
- **Storage format.** Append each poll as one record `{ "timestamp": ..., "price": ... }`
  into a JSON file. Deciding between a single growing JSON array vs. JSON-lines (one
  object per line) — JSONL is friendlier for appends and less likely to corrupt the whole
  file on a crash mid-write.
- **Robustness to add before it runs unattended:** timeout on the request, retry on
  transient failures, and a guard so a bad/empty response doesn't poison the history file.
- **Timestamp.** Record capture time in UTC alongside each price so the series is ordered
  regardless of where it runs.

---

## Network robustness — spec before writing (2026-08-09)

JSONL records landed, so the file format is settled. This is the last thing before the
tracker can be scheduled: right now `get_spot_price` has no timeout, no status check, no
retry, and no guard, so an unattended run can hang forever or write garbage.

Writing down what it needs to do *before* writing it, so I'm not designing in the editor.

### Decisions already made

- **Failure returns `None`, it does not raise.** Caller sees `None`, skips the append, and
  waits for the next scheduled run. No half-written records, no exception traces to parse.
- **`__main__` exits non-zero when the record is `None`.** The function stays quiet and
  returns `None`; the *script* is the thing that reports failure, so Task Scheduler's run
  history shows a failed run instead of a green tick over a day with no data. This is the
  bit worth not skipping — silent failure is the whole problem with returning `None`.
- **Missing a reading is acceptable. A wrong reading is not.** When in doubt, write nothing.

### Shape

Split the network part out from the parse part — one function that gets bytes back, one
that turns them into a record. Makes both testable on their own.

- `_fetch(self) -> dict | None` — does the HTTP, the retries, and the status check.
  Returns the parsed JSON payload, or `None` if every attempt failed.
- `get_spot_price(self) -> dict | None` — calls `_fetch`, validates the shape, builds the
  `{"timestamp", "price"}` record. Returns `None` if the payload can't be trusted.
- `append_json` — unchanged. It never has to defend itself, because nothing bad reaches it.

Constants at module level rather than magic numbers inline: timeout seconds, attempt
count, backoff base.

### What `_fetch` has to handle

1. **Timeout on every request.** `requests.get` with no `timeout=` waits *forever* — not
   30s, forever. This is the fix that actually matters for unattended running.
2. **Check the status.** `raise_for_status()` is the cheap way; a 429 or 5xx becomes an
   exception instead of a `Response` I'd otherwise try to parse.
3. **Retry a few times with a growing pause.** Transient blips are the common case at a
   24h cadence, and losing a whole day's reading to one dropped packet is silly. Sleep
   longer after each failure rather than hammering.
4. **Catch the right things.** `requests.RequestException` alone is enough — checked the
   MRO on the installed `requests 2.34.2`. `Timeout`, `ConnectTimeout`, `ReadTimeout`,
   `ConnectionError`, `SSLError`, and `HTTPError` all subclass it — and so does
   `requests.exceptions.JSONDecodeError`, which inherits from both `RequestException` and
   `ValueError`. So a non-JSON body is covered by the same single `except`. I'd assumed
   I needed `(RequestException, ValueError)`; harmless, but redundant.
5. **Log which attempt failed and why, to stderr.** Otherwise a scheduled run that quietly
   returns `None` gives me nothing to debug from.

### What `get_spot_price` has to guard

The payload came back as *valid JSON* — that does not mean it's the payload I want. An
error body is still JSON. So before trusting it:

- `data` key exists, and `amount` inside it exists → otherwise `KeyError` on the chain.
- `amount` casts to `float` cleanly → otherwise `ValueError`/`TypeError` on garbage.
- Sanity-check the value: not zero, not negative. A `0.0` that reaches the file is worse
  than a gap, because it looks like data.

Any of those failing → return `None`. Same contract as `_fetch`.

### How I'll know it works

These are also the test cases for `coinbase_test.py`, so writing them here means the tests
are already specified:

- Good response → exactly one line appended, and it parses as JSON.
- Malformed payload (missing `data`, `amount` not numeric, price `0`) → nothing appended,
  returns `None`.
- All attempts fail → returns `None`, nothing appended, script exits non-zero.
- N successful calls → file is still valid JSONL, N lines.

Fake the response rather than hitting the network in tests — pass a tmp path to
`append_json` so tests never touch the real history file.

### Small thing noticed while checking the file

Lines end `\r\n`, because text mode on Windows translates `\n` to CRLF. It parses fine
(`json.loads` and `pandas.read_json(lines=True)` both cope), so it's cosmetic — but
`newline="\n"` on the `open()` call pins it to LF everywhere. Do it whenever I'm next in
that function.
