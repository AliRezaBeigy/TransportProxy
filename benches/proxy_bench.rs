//! Criterion benchmarks: throughput and latency of KCP/QUIC echo over localhost.
//!
//! Implementations benchmarked:
//! - **kcp_tokio**: kcp-tokio over UDP
//! - **kcp_deepseek**: <https://github.com/deepseeksss/kcp> — core KCP over UDP (bench bridge)
//! - **kcprs**: <https://crates.io/crates/kcprs> — pure Rust KCP over UDP (bench bridge)
//! - **quinn**: <https://github.com/quinn-rs/quinn> — QUIC over UDP (TLS)
//! - **slipstream-picoquic** (optional): QUIC over UDP via C lib
//! - **ys-kcp** (optional, nightly): <https://crates.io/crates/ys-kcp> — over UDP (bench bridge)
//! - **kcp-sys** (optional, libclang): <https://crates.io/crates/kcp-sys> — KCP over UDP
#![allow(dead_code)]

mod common;
mod kcp_deepseek;
mod kcp_sys;
mod kcp_tokio;
mod kcprs;
mod quinn_bench;
mod slipstream;
mod ys_kcp;

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

fn bench_noop(_: &mut Criterion) {}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets =
        kcp_tokio::bench_throughput,
        kcp_tokio::bench_latency,
        kcp_tokio::bench_concurrent_connections,
        quinn_bench::bench_quinn_throughput,
        quinn_bench::bench_quinn_latency,
        quinn_bench::bench_quinn_concurrent,
        kcp_deepseek::bench_kcp_deepseek_throughput,
        kcp_deepseek::bench_kcp_deepseek_latency,
        kcp_deepseek::bench_kcp_deepseek_concurrent,
        kcprs::bench_kcprs_throughput,
        kcprs::bench_kcprs_latency,
        kcprs::bench_kcprs_concurrent
}

#[cfg(feature = "ys-kcp")]
criterion_group! {
    name = ys_kcp_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = ys_kcp::bench_ys_kcp_throughput,
        ys_kcp::bench_ys_kcp_latency,
        ys_kcp::bench_ys_kcp_concurrent
}

#[cfg(not(feature = "ys-kcp"))]
criterion_group! {
    name = ys_kcp_benches;
    config = Criterion::default();
    targets = bench_noop
}

#[cfg(feature = "kcp-sys")]
criterion_group! {
    name = kcp_sys_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = kcp_sys::bench_kcp_sys_throughput,
        kcp_sys::bench_kcp_sys_latency
}

#[cfg(not(feature = "kcp-sys"))]
criterion_group! {
    name = kcp_sys_benches;
    config = Criterion::default();
    targets = bench_noop
}

#[cfg(feature = "slipstream-picoquic")]
criterion_group! {
    name = slipstream_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = slipstream::bench_slipstream_throughput,
        slipstream::bench_slipstream_latency,
        slipstream::bench_slipstream_concurrent
}

#[cfg(not(feature = "slipstream-picoquic"))]
criterion_group! {
    name = slipstream_benches;
    config = Criterion::default();
    targets = bench_noop
}

criterion_main!(benches, ys_kcp_benches, kcp_sys_benches, slipstream_benches);
