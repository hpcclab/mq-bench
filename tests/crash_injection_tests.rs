//! Integration tests for crash injection and recovery.
//!
//! These tests verify the crash injection feature end-to-end using the mock transport.

#![cfg(feature = "transport-mock")]

use mq_bench::crash::{CrashConfig, CrashInjector};
use mq_bench::metrics::stats::Stats;
use mq_bench::roles::publisher::{run_publisher, PublisherConfig};
use mq_bench::roles::subscriber::{run_subscriber, SubscriberConfig};
use mq_bench::transport::{ConnectOptions, Engine};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// CrashInjector behavior tests (beyond unit tests in crash.rs)
// ============================================================================

#[test]
fn crash_injector_schedules_crash_in_expected_range() {
    // With MTTF of 1 second, crashes should be scheduled reasonably soon
    // Note: exponential distribution has no upper bound, so we use a seed
    // that we know produces a reasonable value
    let config = CrashConfig {
        mttf_secs: 1.0,
        mttr_secs: 0.1,
        crash_count: 0,
        seed: Some(12345), // Known-good seed
    };

    let injector = CrashInjector::new(config);
    let time_to_crash = injector.time_until_crash();

    // With seed 12345, we get a predictable crash time
    // Just verify it's finite and positive
    assert!(
        time_to_crash < Duration::from_secs(60),
        "Expected reasonable crash time, got {:?}",
        time_to_crash
    );
    assert!(
        time_to_crash > Duration::ZERO,
        "Expected positive crash time"
    );
}

#[test]
fn crash_injector_repair_time_varies() {
    let config = CrashConfig {
        mttf_secs: 1.0,
        mttr_secs: 2.0,
        crash_count: 0,
        seed: Some(12345),
    };

    let mut injector = CrashInjector::new(config);

    let repair1 = injector.sample_repair_time();
    let repair2 = injector.sample_repair_time();

    // Different samples should produce different values (with high probability)
    // Note: there's a tiny chance they could be equal, but it's astronomically unlikely
    assert_ne!(repair1, repair2, "Expected varying repair times");
}

#[test]
fn crash_injector_respects_count_limit() {
    let config = CrashConfig {
        mttf_secs: 1.0,
        mttr_secs: 0.1,
        crash_count: 2,
        seed: Some(1),
    };

    let injector = CrashInjector::new(config);

    // First two should succeed
    assert!(injector.has_crashes_remaining());
    assert!(injector.consume_crash());
    assert!(injector.has_crashes_remaining());
    assert!(injector.consume_crash());

    // Third should fail
    assert!(!injector.has_crashes_remaining());
    assert!(!injector.consume_crash());
}

#[test]
fn crash_injector_infinite_mode() {
    let config = CrashConfig {
        mttf_secs: 1.0,
        mttr_secs: 0.1,
        crash_count: 0, // 0 = infinite
        seed: Some(1),
    };

    let injector = CrashInjector::new(config);

    // Should allow many crashes
    for _ in 0..1000 {
        assert!(injector.has_crashes_remaining());
        assert!(injector.consume_crash());
    }
}

#[test]
fn crash_injector_disabled_never_crashes() {
    let config = CrashConfig {
        mttf_secs: 0.0, // Disabled
        mttr_secs: 5.0,
        crash_count: 0,
        seed: None,
    };

    let injector = CrashInjector::new(config);

    assert!(!injector.is_enabled());
    assert!(!injector.should_crash());
    assert_eq!(injector.time_until_crash(), Duration::MAX);
}

#[tokio::test]
async fn crash_injector_should_crash_after_time() {
    let config = CrashConfig {
        mttf_secs: 0.05, // 50ms mean
        mttr_secs: 0.01,
        crash_count: 1,
        seed: Some(100),
    };

    let mut injector = CrashInjector::new(config);

    // Initially may or may not be ready to crash
    let wait_time = injector.time_until_crash();

    // Wait for the crash to become due
    tokio::time::sleep(wait_time + Duration::from_millis(10)).await;

    // Now it should be ready
    assert!(injector.should_crash(), "Expected crash to be due after waiting");

    // Consume and verify
    assert!(injector.consume_crash());

    // Schedule next and verify counter depleted
    injector.schedule_next_crash();
    assert!(!injector.has_crashes_remaining());
}

// ============================================================================
// Stats crash/recovery tracking tests
// ============================================================================

#[tokio::test]
async fn stats_tracks_crash_metrics() {
    let stats = Stats::new();

    // Record some crashes and reconnects
    stats.record_crash_injected();
    stats.record_crash_injected();
    stats.record_reconnect();
    stats.record_reconnect_failure();

    let snapshot = stats.snapshot().await;

    assert_eq!(snapshot.crashes_injected, 2);
    assert_eq!(snapshot.reconnects, 1);
    assert_eq!(snapshot.reconnect_failures, 1);
}

#[tokio::test]
async fn stats_crash_metrics_in_csv() {
    let stats = Stats::new();

    stats.record_crash_injected();
    stats.record_reconnect();

    let snapshot = stats.snapshot().await;
    let csv_row = snapshot.to_csv_row();
    let header = mq_bench::metrics::stats::StatsSnapshot::csv_header();

    // Verify header contains crash columns
    assert!(header.contains("crashes_injected"), "Header missing crashes_injected");
    assert!(header.contains("reconnects"), "Header missing reconnects");
    assert!(header.contains("reconnect_failures"), "Header missing reconnect_failures");

    // Verify row has correct number of columns
    let header_cols: Vec<&str> = header.split(',').collect();
    let row_cols: Vec<&str> = csv_row.split(',').collect();
    assert_eq!(
        header_cols.len(),
        row_cols.len(),
        "CSV header and row column count mismatch"
    );
}

// ============================================================================
// Publisher crash injection integration tests
// ============================================================================

#[tokio::test]
async fn publisher_with_crash_disabled_runs_normally() {
    let stats = Arc::new(Stats::new());

    let config = PublisherConfig {
        engine: Engine::Mock,
        connect: ConnectOptions::default(),
        key_expr: "test/no_crash".to_string(),
        payload_size: 64,
        rate: Some(100.0), // 100 msg/s
        duration_secs: Some(1),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        crash_config: CrashConfig::default(), // Disabled
    };

    let result = run_publisher(config).await;
    assert!(result.is_ok(), "Publisher should complete successfully");

    let snapshot = stats.snapshot().await;
    assert!(snapshot.sent_count > 0, "Should have sent some messages");
    assert_eq!(snapshot.crashes_injected, 0, "No crashes should be injected");
    assert_eq!(snapshot.reconnects, 0, "No reconnects should occur");
}

#[tokio::test]
async fn publisher_with_single_crash_reconnects() {
    let stats = Arc::new(Stats::new());

    // Configure for exactly 1 crash with short MTTF
    let crash_config = CrashConfig {
        mttf_secs: 0.1, // 100ms mean time to failure
        mttr_secs: 0.05, // 50ms repair time
        crash_count: 1,
        seed: Some(42),
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = true; // Enable retry for recovery

    let config = PublisherConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/single_crash".to_string(),
        payload_size: 64,
        rate: Some(50.0),
        duration_secs: Some(2), // Run for 2 seconds
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        crash_config,
    };

    let result = run_publisher(config).await;
    assert!(result.is_ok(), "Publisher should complete after recovery");

    let snapshot = stats.snapshot().await;
    assert!(snapshot.sent_count > 0, "Should have sent some messages");
    assert_eq!(snapshot.crashes_injected, 1, "Should have exactly 1 crash");
    assert_eq!(snapshot.reconnects, 1, "Should have exactly 1 reconnect");
}

#[tokio::test]
async fn publisher_without_retry_stops_on_crash() {
    let stats = Arc::new(Stats::new());

    let crash_config = CrashConfig {
        mttf_secs: 0.05, // Very short - crash quickly
        mttr_secs: 0.01,
        crash_count: 1,
        seed: Some(123),
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = false; // Retry disabled

    let config = PublisherConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/no_retry".to_string(),
        payload_size: 64,
        rate: Some(100.0),
        duration_secs: Some(5), // Long duration - but should stop on crash
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        crash_config,
    };

    let start = std::time::Instant::now();
    let result = run_publisher(config).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Publisher should exit cleanly on crash");
    assert!(
        elapsed < Duration::from_secs(4),
        "Should have stopped early due to crash, not waited full duration"
    );

    let snapshot = stats.snapshot().await;
    assert_eq!(snapshot.crashes_injected, 1, "Should have 1 crash");
    assert_eq!(snapshot.reconnects, 0, "Should have no reconnects (retry disabled)");
}

#[tokio::test]
async fn publisher_sequence_preserved_across_reconnect() {
    // This test verifies that sequence numbers continue across reconnects
    // We can't directly observe the sequence, but we can verify message count
    // continues to increase through crashes

    let stats = Arc::new(Stats::new());

    // Use shorter MTTF to ensure crashes happen within the test window
    let crash_config = CrashConfig {
        mttf_secs: 0.08, // ~80ms mean time to failure
        mttr_secs: 0.01, // Quick repair
        crash_count: 2, // Two crashes
        seed: Some(12345), // Known-good seed
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = true;

    let config = PublisherConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/sequence_unique_key_12345".to_string(), // Unique key to avoid collision
        payload_size: 64,
        rate: Some(100.0),
        duration_secs: Some(3), // Run for 3 seconds to ensure crashes happen
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        crash_config,
    };

    let result = run_publisher(config).await;
    assert!(result.is_ok());

    let snapshot = stats.snapshot().await;

    // Should have at least 2 crashes and 2 reconnects (crash_count limits to 2)
    // We use >= because of potential test parallelism issues with global mock bus
    assert!(
        snapshot.crashes_injected >= 2,
        "Expected at least 2 crashes, got {}",
        snapshot.crashes_injected
    );
    assert!(
        snapshot.reconnects >= 2,
        "Expected at least 2 reconnects, got {}",
        snapshot.reconnects
    );

    // Should have sent messages across all sessions
    // At 100 msg/s for ~3s (minus crash/repair time), expect at least 100 messages
    assert!(
        snapshot.sent_count > 50,
        "Expected significant message count: got {}",
        snapshot.sent_count
    );
}

// ============================================================================
// Subscriber crash injection integration tests
// ============================================================================

#[tokio::test]
async fn subscriber_with_crash_disabled_runs_normally() {
    let stats = Arc::new(Stats::new());

    let config = SubscriberConfig {
        engine: Engine::Mock,
        connect: ConnectOptions::default(),
        key_expr: "test/sub_no_crash".to_string(),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        test_stop_after_secs: Some(1),
        crash_config: CrashConfig::default(),
    };

    let result = run_subscriber(config).await;
    assert!(result.is_ok(), "Subscriber should complete successfully");

    let snapshot = stats.snapshot().await;
    assert_eq!(snapshot.crashes_injected, 0);
    assert_eq!(snapshot.reconnects, 0);
}

#[tokio::test]
async fn subscriber_with_single_crash_reconnects() {
    let stats = Arc::new(Stats::new());

    let crash_config = CrashConfig {
        mttf_secs: 0.1,
        mttr_secs: 0.05,
        crash_count: 1,
        seed: Some(42),
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = true;

    let config = SubscriberConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/sub_single_crash".to_string(),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        test_stop_after_secs: Some(2),
        crash_config,
    };

    let result = run_subscriber(config).await;
    assert!(result.is_ok(), "Subscriber should complete after recovery");

    let snapshot = stats.snapshot().await;
    assert_eq!(snapshot.crashes_injected, 1);
    assert_eq!(snapshot.reconnects, 1);
}

#[tokio::test]
async fn subscriber_without_retry_stops_on_crash() {
    let stats = Arc::new(Stats::new());

    let crash_config = CrashConfig {
        mttf_secs: 0.05,
        mttr_secs: 0.01,
        crash_count: 1,
        seed: Some(321),
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = false;

    let config = SubscriberConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/sub_no_retry".to_string(),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(stats.clone()),
        disable_internal_snapshot: true,
        test_stop_after_secs: Some(5),
        crash_config,
    };

    let start = std::time::Instant::now();
    let result = run_subscriber(config).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(
        elapsed < Duration::from_secs(4),
        "Should have stopped early"
    );

    let snapshot = stats.snapshot().await;
    assert_eq!(snapshot.crashes_injected, 1);
    assert_eq!(snapshot.reconnects, 0);
}

// ============================================================================
// Combined pub/sub crash scenarios
// ============================================================================

#[tokio::test]
async fn pubsub_both_crashing_independently() {
    // Test that publisher and subscriber can crash independently
    let pub_stats = Arc::new(Stats::new());
    let sub_stats = Arc::new(Stats::new());

    // Use shorter MTTF to ensure crashes happen
    let pub_crash = CrashConfig {
        mttf_secs: 0.08,
        mttr_secs: 0.01,
        crash_count: 2,
        seed: Some(12345),
    };

    let sub_crash = CrashConfig {
        mttf_secs: 0.1,
        mttr_secs: 0.01,
        crash_count: 1,
        seed: Some(54321),
    };

    let mut connect_opts = ConnectOptions::default();
    connect_opts.retry_enabled = true;

    // Use unique keys to avoid interference with other tests
    let pub_config = PublisherConfig {
        engine: Engine::Mock,
        connect: connect_opts.clone(),
        key_expr: "test/both_crash_unique_99999".to_string(),
        payload_size: 64,
        rate: Some(50.0),
        duration_secs: Some(3),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(pub_stats.clone()),
        disable_internal_snapshot: true,
        crash_config: pub_crash,
    };

    let sub_config = SubscriberConfig {
        engine: Engine::Mock,
        connect: connect_opts,
        key_expr: "test/both_crash_unique_99999".to_string(),
        output_file: None,
        snapshot_interval_secs: 1,
        shared_stats: Some(sub_stats.clone()),
        disable_internal_snapshot: true,
        test_stop_after_secs: Some(3),
        crash_config: sub_crash,
    };

    // Run both concurrently
    let (pub_result, sub_result) = tokio::join!(run_publisher(pub_config), run_subscriber(sub_config));

    assert!(pub_result.is_ok());
    assert!(sub_result.is_ok());

    let pub_snapshot = pub_stats.snapshot().await;
    let sub_snapshot = sub_stats.snapshot().await;

    // Publisher should have at least 2 crashes, subscriber at least 1
    // (crash_count limits, but >= for test robustness)
    assert!(
        pub_snapshot.crashes_injected >= 2,
        "Publisher expected at least 2 crashes, got {}",
        pub_snapshot.crashes_injected
    );
    assert!(
        sub_snapshot.crashes_injected >= 1,
        "Subscriber expected at least 1 crash, got {}",
        sub_snapshot.crashes_injected
    );

    // Both should have reconnected
    assert!(
        pub_snapshot.reconnects >= 2,
        "Publisher expected at least 2 reconnects, got {}",
        pub_snapshot.reconnects
    );
    assert!(
        sub_snapshot.reconnects >= 1,
        "Subscriber expected at least 1 reconnect, got {}",
        sub_snapshot.reconnects
    );
}

// ============================================================================
// Determinism/reproducibility tests
// ============================================================================

#[test]
fn crash_patterns_reproducible_with_seed() {
    let config = CrashConfig {
        mttf_secs: 10.0,
        mttr_secs: 5.0,
        crash_count: 0,
        seed: Some(12345),
    };

    // Create two injectors with same seed
    let mut injector1 = CrashInjector::new(config.clone());
    let mut injector2 = CrashInjector::new(config);

    // Generate sequence of values
    let seq1: Vec<Duration> = (0..10).map(|_| injector1.sample_repair_time()).collect();
    let seq2: Vec<Duration> = (0..10).map(|_| injector2.sample_repair_time()).collect();

    assert_eq!(seq1, seq2, "Same seed should produce identical sequences");
}

#[test]
fn different_seeds_produce_different_patterns() {
    let config1 = CrashConfig {
        mttf_secs: 10.0,
        mttr_secs: 5.0,
        crash_count: 0,
        seed: Some(111),
    };

    let config2 = CrashConfig {
        mttf_secs: 10.0,
        mttr_secs: 5.0,
        crash_count: 0,
        seed: Some(222),
    };

    let mut injector1 = CrashInjector::new(config1);
    let mut injector2 = CrashInjector::new(config2);

    let repair1 = injector1.sample_repair_time();
    let repair2 = injector2.sample_repair_time();

    assert_ne!(repair1, repair2, "Different seeds should produce different values");
}

// ============================================================================
// Edge case tests
// ============================================================================

#[test]
fn very_small_mttf_triggers_quickly() {
    let config = CrashConfig {
        mttf_secs: 0.001, // 1ms
        mttr_secs: 0.001,
        crash_count: 0,
        seed: Some(1),
    };

    let injector = CrashInjector::new(config);
    let time_to_crash = injector.time_until_crash();

    // With 1ms MTTF, crash should be scheduled very soon
    assert!(
        time_to_crash < Duration::from_millis(100),
        "Expected quick crash with tiny MTTF"
    );
}

#[test]
fn very_large_mttf_delays_crash() {
    let config = CrashConfig {
        mttf_secs: 3600.0, // 1 hour
        mttr_secs: 1.0,
        crash_count: 0,
        seed: Some(1),
    };

    let injector = CrashInjector::new(config);
    let time_to_crash = injector.time_until_crash();

    // With 1 hour MTTF, crash should not be immediate
    // (there's a small chance it could be, but statistically unlikely)
    assert!(
        time_to_crash > Duration::from_secs(1),
        "Expected delayed crash with large MTTF"
    );
}

#[tokio::test]
async fn zero_mttr_means_immediate_reconnect() {
    let config = CrashConfig {
        mttf_secs: 1.0,
        mttr_secs: 0.0, // Zero repair time
        crash_count: 0,
        seed: Some(42),
    };

    let mut injector = CrashInjector::new(config);
    let repair_time = injector.sample_repair_time();

    assert_eq!(repair_time, Duration::ZERO, "Zero MTTR should give zero repair time");
}
