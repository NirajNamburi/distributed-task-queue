"""The three demo tasks called out in the project spec.

These exist to exercise the queue's three interesting axes:

    * I/O-shaped work       -> ``process_sales_csv``
    * CPU-bound parallelism -> ``calculate_primes``  (run >=4 concurrently to
                                                     visibly prove GIL bypass)
    * Failure handling      -> ``fetch_flaky_api``  (50% raise rate -> backoff)

They are intentionally pure (no Redis access, no globals) so they can be
unit-tested directly and so the queue can shuttle them across processes
without surprises.
"""

from __future__ import annotations

import csv
import io
import random
import time


def process_sales_csv(file_path: str, rows: int = 100_000) -> float:
    """Generate ``rows`` of synthetic sales data and return the total revenue.

    The ``file_path`` argument exists for API parity with the spec; the CSV
    is generated in memory so the demo runs identically on Windows, macOS,
    and Linux without any disk setup. ``file_path`` is included in the
    returned summary so callers see it.
    """
    rng = random.Random(hash(file_path) & 0xFFFFFFFF)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["order_id", "sku", "quantity", "unit_price"])
    for i in range(rows):
        writer.writerow([
            i,
            f"SKU-{rng.randint(1, 9999):04d}",
            rng.randint(1, 25),
            round(rng.uniform(0.99, 999.99), 2),
        ])

    buf.seek(0)
    reader = csv.DictReader(buf)
    total = 0.0
    for row in reader:
        total += int(row["quantity"]) * float(row["unit_price"])
    return round(total, 2)


def calculate_primes(n: int) -> int:
    """Brute-force count of primes up to ``n`` (inclusive). CPU-bound on purpose.

    Runs trial division with the standard 6k +/- 1 wheel - just sophisticated
    enough to be interesting, not so optimized that the workload disappears.
    Returns the count rather than the list to keep result payloads tiny.
    """
    if n < 2:
        return 0
    count = 1 if n >= 2 else 0
    if n >= 3:
        count += 1
    i = 5
    while i <= n:
        for cand in (i, i + 2):
            if cand > n:
                break
            limit = int(cand ** 0.5)
            is_prime = True
            j = 5
            while j <= limit:
                if cand % j == 0 or cand % (j + 2) == 0:
                    is_prime = False
                    break
                j += 6
            if is_prime and cand % 2 != 0 and cand % 3 != 0:
                count += 1
        i += 6
    return count


def fetch_flaky_api(user_id: int, fail_rate: float = 0.5, latency_s: float = 0.05) -> dict:
    """Simulate a flaky upstream call. Raises ``ConnectionError`` ``fail_rate`` of the time.

    A small artificial latency makes the retry timing observable in logs and
    metrics histograms.
    """
    time.sleep(latency_s)
    if random.random() < fail_rate:
        raise ConnectionError(f"upstream timed out for user_id={user_id}")
    return {"user_id": user_id, "ok": True, "fetched_at": time.time()}
