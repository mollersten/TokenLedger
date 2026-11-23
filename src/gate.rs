//! In-process admission gates that keep one busy key from tying up the
//! database pool.
//!
//! Every write to a wallet waits for its row lock while holding a pooled
//! connection. Writes to one wallet run one at a time anyway, so letting
//! more than a couple of them hold connections only takes connections away
//! from every other account: a burst on one hot tenant (an end user
//! hammering a button, a retry loop) could otherwise fill the whole pool.
//! A [`KeyedGate`] lets a few requests per key hold a connection and makes
//! the rest wait here, without one, for a bounded time.
//!
//! Gates are per process; with several API instances each has its own.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ApiError;

/// Requests per wallet that may hold a database connection at once: one
/// working, one queued on the row lock behind it.
pub const WALLET_PERMITS: usize = 2;
/// How long a request waits for its wallet's gate before a 503.
pub const WALLET_WAIT: Duration = Duration::from_secs(10);
/// Import requests per account that may run at once. The SDK sends one at a
/// time, so only a misbehaving client ever meets this.
pub const IMPORT_PERMITS: usize = 2;

/// Entries are dropped once nobody holds or waits for them, checked when the
/// map grows past this size.
const PRUNE_ABOVE: usize = 10_000;

pub struct KeyedGate {
    permits: usize,
    gates: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl KeyedGate {
    pub fn new(permits: usize) -> Self {
        Self { permits, gates: Mutex::new(HashMap::new()) }
    }

    fn semaphore(&self, key: &str) -> Arc<Semaphore> {
        let mut gates = self.gates.lock().unwrap_or_else(PoisonError::into_inner);
        if gates.len() > PRUNE_ABOVE {
            // Only the map holds an idle entry's semaphore.
            gates.retain(|_, s| Arc::strong_count(s) > 1);
        }
        gates.entry(key.to_owned()).or_insert_with(|| Arc::new(Semaphore::new(self.permits))).clone()
    }

    /// Wait up to `wait` for a permit; `None` if none came free in time.
    pub async fn acquire(&self, key: &str, wait: Duration) -> Option<OwnedSemaphorePermit> {
        let semaphore = self.semaphore(key);
        tokio::time::timeout(wait, semaphore.acquire_owned()).await.ok()?.ok()
    }

    /// Take a permit only if one is free right now.
    pub fn try_acquire(&self, key: &str) -> Option<OwnedSemaphorePermit> {
        self.semaphore(key).try_acquire_owned().ok()
    }
}

/// The gates an [`crate::AppState`] carries.
pub struct Gates {
    pub wallets: KeyedGate,
    pub imports: KeyedGate,
}

impl Default for Gates {
    fn default() -> Self {
        Self { wallets: KeyedGate::new(WALLET_PERMITS), imports: KeyedGate::new(IMPORT_PERMITS) }
    }
}

impl Gates {
    /// Admission for one write to a wallet, held until the request is done.
    pub async fn wallet(&self, account_id: &str, tenant_id: &str) -> Result<OwnedSemaphorePermit, ApiError> {
        // Tenant ids cannot contain control characters, so \u{1f} cannot
        // make two different (account, tenant) pairs collide.
        let key = format!("{account_id}\u{1f}{tenant_id}");
        self.wallets.acquire(&key, WALLET_WAIT).await.ok_or_else(|| {
            ApiError::unavailable(
                "wallet_busy",
                format!("too many requests for tenant '{tenant_id}' are in progress; retry shortly"),
                1,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_holders_per_key_and_frees_on_drop() {
        let gate = KeyedGate::new(2);
        let a1 = gate.acquire("a", Duration::from_millis(10)).await.expect("first");
        let _a2 = gate.acquire("a", Duration::from_millis(10)).await.expect("second");
        assert!(gate.acquire("a", Duration::from_millis(20)).await.is_none(), "third must wait and time out");
        assert!(gate.try_acquire("b").is_some(), "other keys are not affected");
        drop(a1);
        assert!(gate.acquire("a", Duration::from_millis(10)).await.is_some(), "freed by drop");
    }
}
