### THIS IS FOR DOCUMENTING ISSUES I HAD DURING DEVELOPMENT ##

---

## Coinbase price tracker — init

Started the Coinbase piece in `src/tracker/coinbase.py`.

Thought process:
- Hit the public spot-price endpoint (`/v2/prices/BTC-USD/spot`) with `requests`. No auth
  needed for spot, so it's the simplest thing that gets real data flowing.
- Wrapped it in a `CoinbaseClient` class so the URL lives in one place and I can add
  more endpoints later without rewriting call sites.

End goal this week: call this every 24h and dump the result into a JSON file for history.
