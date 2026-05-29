//! Retry helper with Fibonacci backoff, shared by the `fetch` request methods.

use std::future::Future;
use std::time::Duration;

/// Runs `operation`, retrying up to `retries` additional times on `Err`.
///
/// Between attempts, it sleeps for a Fibonacci-growing number of seconds: 1s before the first retry, then 2s, 3s, 5s,
/// 8s, … On success returns immediately; once retries are exhausted, returns the last error.
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
                let secs = fibonacci_delay(attempt + 1);
                tokio::time::sleep(Duration::from_secs(secs)).await;
                attempt += 1;
            }
        }
    }
}

/// Delay (in seconds) before the `n`-th retry (1-based): 1, 2, 3, 5, 8, 13, …
fn fibonacci_delay(n: u32) -> u64 {
    let (mut prev, mut curr) = (1u64, 2u64); // delays for n=1 and n=2
    for _ in 1..n {
        let next = prev + curr;
        prev = curr;
        curr = next;
    }
    prev
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
