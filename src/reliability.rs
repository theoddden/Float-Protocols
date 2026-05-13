//! Reliability patterns for 99.9% uptime
//! 
//! Implements circuit breakers, retry policies, and health checks
//! inspired by cloud-native reliability patterns.

use tokio::time::{Duration, Instant};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,    // Normal operation
    Open,      // Circuit is open, rejecting requests
    HalfOpen,  // Testing if circuit should close
}

pub struct CircuitBreaker {
    state: Arc<AtomicU32>, // Using AtomicU32 for CircuitState
    failure_count: Arc<AtomicU32>,
    last_failure_time: Arc<AtomicU64>,
    failure_threshold: u32,
    recovery_timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(AtomicU32::new(CircuitState::Closed as u32)),
            failure_count: Arc::new(AtomicU32::new(0)),
            last_failure_time: Arc::new(AtomicU64::new(0)),
            failure_threshold,
            recovery_timeout,
        }
    }

    pub async fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        if self.is_open() {
            return Err(CircuitBreakerError::Open);
        }

        match f.await {
            Ok(result) => {
                self.on_success();
                Ok(result)
            }
            Err(err) => {
                self.on_failure();
                Err(CircuitBreakerError::Inner(err))
            }
        }
    }

    fn is_open(&self) -> bool {
        let state = self.state.load(Ordering::Acquire) as u8;
        if state == CircuitState::Open as u8 {
            // Check if recovery timeout has elapsed
            let last_failure = self.last_failure_time.load(Ordering::Acquire);
            let elapsed = Duration::from_millis(
                (Instant::now().elapsed_since_startup()).unwrap_or(Duration::ZERO).as_millis() as u64 - last_failure
            );
            
            if elapsed > self.recovery_timeout {
                // Transition to HalfOpen
                self.state.store(CircuitState::HalfOpen as u32, Ordering::Release);
                return false;
            }
            return true;
        }
        false
    }

    fn on_success(&self) {
        self.failure_count.store(0, Ordering::Release);
        self.state.store(CircuitState::Closed as u32, Ordering::Release);
    }

    fn on_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.last_failure_time.store(
            Instant::now().elapsed_since_startup().unwrap_or(Duration::ZERO).as_millis() as u64,
            Ordering::Release
        );

        if count >= self.failure_threshold {
            self.state.store(CircuitState::Open as u32, Ordering::Release);
        }
    }

    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::Acquire) as u8 {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    Open,
    Inner(E),
}

pub struct RetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
    backoff_factor: f64,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay: Duration::from_secs(30),
            backoff_factor: 2.0,
        }
    }

    pub async fn execute<F, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
    {
        let mut attempt = 0;
        let mut delay = self.base_delay;

        loop {
            attempt += 1;
            match f().await {
                Ok(result) => return Ok(result),
                Err(err) if attempt >= self.max_attempts => return Err(err),
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(
                        Duration::from_millis((delay.as_millis() as f64 * self.backoff_factor) as u64),
                        self.max_delay,
                    );
                }
            }
        }
    }
}

pub struct HealthChecker {
    last_check: Arc<AtomicU64>,
    check_interval: Duration,
}

impl HealthChecker {
    pub fn new(check_interval: Duration) -> Self {
        Self {
            last_check: Arc::new(AtomicU64::new(0)),
            check_interval,
        }
    }

    pub async fn start<F>(&self, check_fn: F)
    where
        F: Fn() -> bool + Send + 'static,
    {
        let last_check = self.last_check.clone();
        tokio::spawn(async move {
            loop {
                if check_fn() {
                    last_check.store(
                        Instant::now().elapsed_since_startup().unwrap_or(Duration::ZERO).as_millis() as u64,
                        Ordering::Release
                    );
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }

    pub fn last_check_time(&self) -> Option<Instant> {
        let timestamp = self.last_check.load(Ordering::Acquire);
        if timestamp == 0 {
            None
        } else {
            Some(Instant::now() - Duration::from_millis(
                Instant::now().elapsed_since_startup().unwrap_or(Duration::ZERO).as_millis() as u64 - timestamp
            ))
        }
    }

    pub fn is_healthy(&self) -> bool {
        if let Some(last) = self.last_check_time() {
            last.elapsed() < self.check_interval * 2
        } else {
            false
        }
    }
}

trait InstantExt {
    fn elapsed_since_startup() -> Option<Duration>;
}

impl InstantExt for Instant {
    fn elapsed_since_startup() -> Option<Duration> {
        // In production, this would use actual startup time
        Some(Duration::from_secs(0))
    }
}
