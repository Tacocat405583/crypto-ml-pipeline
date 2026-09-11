#!/bin/sh
# Positive control first: the planted race must be reported, or this run is not
# evidence of anything. Then the load tests, each of which fails if TSan halts
# the feed handler with exit code 66.
./canary 2> canary.log
code=$?
if [ "$code" -ne 66 ] || ! grep -q "WARNING: ThreadSanitizer: data race" canary.log; then
    echo "TSan did not report the planted race (exit $code): not a valid run" >&2
    cat canary.log >&2
    exit 1
fi
echo "canary: planted race reported (exit $code) -- the sanitizer is live"
exec python3 -m pytest tests/test_feed_handler.py -v -p no:cacheprovider
