# Introduction

Distributed Deduplicator is a library for cross-region distributed, lock-free deduplication based on Apache Cassandra storage that offers a high-performance, highly scalable platform with strong data consistency and non-duplicate guarantee.

# System requirements

- JDK 14
- Apache Cassandra

# Dependency

Using JitPack: https://jitpack.io/#stroiker/distributed-deduplicator

[![](https://jitpack.io/v/stroiker/distributed-deduplicator.svg)](https://jitpack.io/#stroiker/distributed-deduplicator)

OR

Using Git Source Control (gradle example):
1) Add to `settings.gradle` additional source mapping
```
sourceControl {
    gitRepository("https://github.com/stroiker/distributed-deduplicator.git") {
        producesModule("com.stroiker:distributed-deduplicator")
    }
}
```
2) Add to `build.gradle` library dependency
```
dependencies {
    implementation "com.stroiker:distributed-deduplicator:${version}"
}
```
3) Run Gradle task `assemble` to generate source classes.

# Quick start

1) Start an Apache Cassandra cluster and create a keyspace, manually parameterized according to your business requirements (replication factor, etc.);
2) Use builder `DeduplicationProviderBuilder.newProviderBuilder()` or `DeduplicationProviderBuilder.newAsyncProviderBuilder()` to create provider instance. You can create a provider with a given Cassandra `CqlSession` object or using Cassandra `application.conf` configuration file from classpath by default. 
If you want to use separate session parameters (like consistency level, etc.) - you can configure a custom profile and pass the profile name during provider creation. Also, you can pass a retry strategy which is used to resolve undefined processing order from implemented strategies (see below) or implement your own strategy.
3) Wrap your business logic which have to protect against duplicates in function `process(...)`. Next arguments have to pass to function:
- `key` - idempotency key which is unique identifier of your business logic unit of work;
- `table` - table to store keys with additional info. You can separate one key between multiple tables according to your business logic. Table will be created automatically during first access attempt;
- `keyspace` - keyspace where tables will be created;
- `ttl` - time-to-live of each record in table. Using to evict expired records if needed (set 0 if you need to store record indefinitely);
- `block` - your business logic block of code, which processed if duplication check would pass successfully. You should pass it as lambda-expression or anonymous-class instance.
4) Handle the following exceptions if they happen. If a chain of exceptions occurs, you can see all previous exceptions by recursively navigating to the `suppressed` field:
- `DuplicateException` - if a given key has already been processed;
- `FailedException` - if writing to Cassandra has failed. If this exception happens during a business logic block invocation throw exception - it will contain the reason in the exception message;
- `RetriesExceededException` - if parallel-processed duplicate keys have an undefined write order, the provider tries to resolve this by repeating write attempts in Cassandra. If the number of retries is exceeded (depending on the retry strategy), an exception will be thrown. 
If this exception occurred without a suppressed exception, you can retry your business logic with your own way. If this exception occurred with a suppressed exception, you need to ensure that your business logic is processed or not and decide to retry with your own way.

# Retry strategies

Retry strategies are necessary to resolve the undefined ordering of duplicate keys in Cassandra caused by high contention due to time-shifted writes using retries.
There are 3 implemented retry strategies:
- `NoRetryStrategy` - doesn't make retries at all;
- `FixedDelayRetryStrategy` - makes given retries count with fixed delay between retries;
- `ExponentialDelayRetryStrategy` - makes given retries count with exponential delay between retries;

# Async

You can use async flow with following class `DeduplicationProviderAsync`. It provides similar functionality as `DeduplicationProvider` (include creation mechanism) with few differences:
1) Main function `processAsync(...)` returns `CompletableFuture` object which can be used for building asynchronous processing;
2) Asynchronous provider uses async retry strategies same as synchronous versions. These strategies provide non-blocking approach and offer better throughput when duplicate contention is high. You can pass your own thread pool for async retries or use default `ForkJoinPool` implementation;

# Multiple datacenters

Library is ready-to-work in cross-datacenters mode on read/write workloads and offers the same guaranties as in a single-datacenter mode. There is no extra configuration needed. All you need is up multiple Apache Cassandra clusters and provide appropriate paths to cluster nodes through session configuration.
Consistency levels are configured automatically to reduce suffering from latency between datacenters. 

# Burst absorber

Each provider can offer duplicate burst-absorber which greatly reduces number of retries caused by inner-process duplicate contention and reduces overall number of read request to storage, especially between datacenters. Duplicate burst absorber disabled by default, but you can configure it during provider creation if you faced with significant duplicates contention.
