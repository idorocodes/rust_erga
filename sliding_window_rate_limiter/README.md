## 🏗️ Design Note: Rate Limiter Architecture

### 1. Data Structure Choice

* **Primary Store (`DashMap<Uuid, BucketState>`):** We use `DashMap` as our concurrent hash map to associate client UUIDs with their rate-limiting state. Compared to a standard `std::collections::HashMap` wrapped in a global `RwLock` or `Mutex`, `DashMap` provides lock-sharding (stripe locking). This enables concurrent reads and writes across distinct key buckets without locking the entire dictionary.
* **In-Memory Bucket State (`BucketState`):** Each client entry stores `tokens: u32` and `last_updated: Instant`. Rather than running background threads or timers to replenish tokens periodically, token updates are calculated **lazily on-demand** whenever a request hits the `gate_man` function. Time drift is mitigated by advancing `last_updated` only by consumed discrete intervals (`new_tokens * refill_interval_ms`).

### 2. Concurrency Model

* **Async-First Execution (Tokio Task Multiplexing):** The server runs on the asynchronous `tokio` runtime. Middleware executions run as lightweight async green tasks multiplexed over a CPU-core-sized thread pool, avoiding thread-per-request OS overhead.
* **Fine-Grained Entry Locking:** During the middleware pass, `.entry(uuid)` acquires an exclusive write lock *only* on the specific shard containing that user’s bucket. Other client requests targeting different shards execute concurrently without blocking.
* **Short-Circuit Pipeline:** Unauthenticated requests or rate-limited requests are dropped directly within the `tower` middleware layer before allocating resources or routing to application handlers.

### 3. Algorithm Complexity

* **Time Complexity:**
* **Lookup & Update (`gate_man`):** **$O(1)$ average time**. Hash lookup, entry insertion, time delta arithmetic, and token adjustments all execute in constant time.
* **Middleware Interception:** **$O(1)$** token string manipulation and UUID parsing.


* **Space Complexity:**
* **Overall Memory Footprint:** **$O(N)$** where $N$ is the number of active, unique client UUIDs currently tracked in the map.
* **Memory per Client:** Each `BucketState` consumes minimal memory (~24 bytes for `Instant` + `u32`), ensuring millions of active client keys can be maintained in RAM with low memory overhead.