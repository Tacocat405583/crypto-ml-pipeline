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

### End goal this week

Call this on a 24h cadence and append each reading into a JSON file, so I build up a
running price history to feed the ML pipeline later.
