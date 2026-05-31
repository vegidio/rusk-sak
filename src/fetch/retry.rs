//! Retry helper with Fibonacci backoff, shared by the `fetch` request methods.

use std::future::Future;
use std::time::Duration;

/// Maximum delay (seconds) between retry attempts. The Fibonacci backoff grows without bound, so without a ceiling a
/// large retry count would stall for minutes, then hours. Capping keeps a high retry count a bounded retry budget
/// rather than an effectively-infinite hang.
///
/// This cap is also what keeps [`fibonacci_delay`]'s closed form correct: Binet's formula is only exact through
/// `F(78)` before floating-point drift, but every Fibonacci value below this cap is `F(2)..=F(10)` (1..=55), well
/// inside the exact range — anything larger is clamped here regardless. Do **not** raise this anywhere near the
/// float-exact range (~9e15) or `fibonacci_delay` would start returning imprecise values.
const MAX_BACKOFF_SECS: u64 = 60;

/// Runs `operation`, retrying up to `retries` additional times on `Err`.
///
/// Between attempts, it sleeps for a Fibonacci-growing number of seconds: 1s before the first retry, then 2s, 3s, 5s,
/// 8s, … capped at [`MAX_BACKOFF_SECS`]. On success returns immediately; once retries are exhausted, returns the last
/// error.
pub(crate) async fn with_fibonacci_backoff<F, Fut, T, E>(retries: u32, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt: u32 = 0;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= retries {
                    return Err(err);
                }
                let secs = fibonacci_delay(attempt + 1).min(MAX_BACKOFF_SECS);
                tokio::time::sleep(Duration::from_secs(secs)).await;
                attempt += 1;
            }
        }
    }
}

/// Delay (in seconds) before the `n`-th retry (1-based): 1, 2, 3, 5, 8, 13, … — the standard Fibonacci `F(n + 1)`.
///
/// Computed in closed form via Binet's formula rather than iteratively. The `f64` math is exact only through `F(78)`,
/// but callers clamp the result to [`MAX_BACKOFF_SECS`], far below where drift begins, so every delay that actually
/// matters is computed exactly (and an overflow to `f64::INFINITY` for huge `n` saturates through `as u64`, then
/// clamps). See [`MAX_BACKOFF_SECS`] for the invariant this relies on.
fn fibonacci_delay(n: u32) -> u64 {
    let sqrt5 = 5f64.sqrt();
    let phi = (1.0 + sqrt5) / 2.0;
    // Use an `f64` exponent (not `powi`'s `i32`): a large `n` would otherwise wrap when cast to `i32`. Here a large
    // `n` just sends `powf` to `f64::INFINITY`, which saturates through `as u64` and is clamped by the caller.
    (phi.powf(n as f64 + 1.0) / sqrt5).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn fibonacci_delay_follows_sequence() {
        let expected = [1u64, 2, 3, 5, 8, 13, 21];
        for (i, &want) in expected.iter().enumerate() {
            assert_eq!(fibonacci_delay(i as u32 + 1), want);
        }
    }

    #[test]
    fn fibonacci_delay_is_capped_for_large_n() {
        // Past the float-exact range the closed form may drift, but every such value is astronomically larger than the
        // cap, so the clamped delay is always `MAX_BACKOFF_SECS`. `u32::MAX` also exercises the overflow-to-infinity
        // saturating path.
        for &n in &[100u32, 1_000, u32::MAX] {
            assert_eq!(fibonacci_delay(n).min(MAX_BACKOFF_SECS), MAX_BACKOFF_SECS);
        }
    }

    #[tokio::test]
    async fn returns_immediately_on_first_success() {
        let calls = Cell::new(0);
        let result: Result<u32, ()> = with_fibonacci_backoff(3, || {
            calls.set(calls.get() + 1);
            async { Ok(42) }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_success() {
        let calls = Cell::new(0);
        let result: Result<u32, &str> = with_fibonacci_backoff(5, || {
            calls.set(calls.get() + 1);
            async { if calls.get() < 3 { Err("boom") } else { Ok(7) } }
        })
        .await;

        assert_eq!(result, Ok(7));
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn no_retries_returns_first_error() {
        let calls = Cell::new(0);
        let result: Result<u32, &str> = with_fibonacci_backoff(0, || {
            calls.set(calls.get() + 1);
            async { Err("boom") }
        })
        .await;

        assert_eq!(result, Err("boom"));
        assert_eq!(calls.get(), 1);
    }
}
