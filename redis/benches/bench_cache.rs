use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

use rand::distr::{Bernoulli, Distribution};
use redis::caching::CacheConfig;
use std::{env, sync::Arc};
use tokio::sync::Semaphore;

const CONCURRENT_TASKS: usize = 1024;

async fn benchmark_executer(
    is_cache_enabled: bool,
    read_ratio: f32,
    command_count_per_key: u32,
    key_count: u32,
) {
    let ctx = TestContext::new();
    let con = if is_cache_enabled {
        ctx.async_connection_with_cache_config(CacheConfig::new())
            .await
            .unwrap()
    } else {
        ctx.multiplexed_async_connection_tokio().await.unwrap()
    };

    let mut handles = Vec::new();

    let read_command_count_per_key = (read_ratio * command_count_per_key as f32) as u32;
    let distribution =
        Bernoulli::from_ratio(read_command_count_per_key, command_count_per_key).unwrap();
    let sem = Arc::new(Semaphore::new(CONCURRENT_TASKS));

    for num in 0..key_count {
        let mut con = con.clone();
        let key = format!("{}", num);
        let read_write_distribution: Vec<bool> = distribution
            .sample_iter(&mut rand::rng())
            .take(command_count_per_key as usize)
            .collect();

        let permit = Arc::clone(&sem).acquire_owned().await;

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            for is_read in read_write_distribution {
                let cmd = if is_read {
                    redis::cmd("GET").arg(&key).clone()
                } else {
                    redis::cmd("SET").arg(&key).arg(9).clone() // Random value for key
                };
                let _: () = cmd.query_async(&mut con).await.unwrap();
            }
        }));
    }
    for job_handle in handles {
        job_handle.await.unwrap();
    }
}

fn prepare_benchmark(
    bencher: &mut Bencher,
    thread_num: usize,
    is_cache_enabled: bool,
    read_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap();
    bencher.iter(|| {
        runtime.block_on(benchmark_executer(
            is_cache_enabled,
            read_ratio,
            per_key_command,
            key_count,
        ));
    });
}

fn bench_cache(c: &mut Criterion) {
    if env::var("PROTOCOL").unwrap_or_default() != "RESP3" {
        return;
    }
    let is_cache_enabled = env::var("ENABLE_CLIENT_SIDE_CACHE").unwrap_or_default() == "true";
    let test_cases: Vec<(f32, u32, u32)> = vec![
        (0.99, 100, 10_000),
        (0.90, 100, 10_000),
        (0.5, 100, 10_000),
        (0.1, 100, 10_000),
        (0.5, 5, 100_000),
        (0.5, 1, 100_000),
    ];
    for (read_ratio, per_key_command, total_key_count) in test_cases.clone() {
        let group_name = format!("{read_ratio}-{per_key_command}-{total_key_count}");
        let mut group = c.benchmark_group(group_name);
        group.throughput(Throughput::Elements(
            (per_key_command * total_key_count) as u64,
        ));
        group.sample_size(10);

        for thread_num in [1, 4, 16, 32] {
            group.bench_function(format!("thread-{thread_num}"), |bencher| {
                prepare_benchmark(
                    bencher,
                    thread_num,
                    is_cache_enabled,
                    read_ratio,
                    per_key_command,
                    total_key_count,
                )
            });
        }
        group.finish();
    }
}

criterion_group!(bench, bench_cache);
criterion_main!(bench);
