#  Daily Rust & Backend Challenges

A collection of daily coding challenges, hands-on experiments, and system design projects assigned by ChatGPT to master high-performance backend architecture, systems programming in Rust, and concurrent systems design.

---

##  Project Showcase

| Day / Project | Description | Core Tech / Concepts | Repository |
| :--- | :--- | :--- | :--- |
| **`sliding_window_rate_limiter`** | High-throughput, concurrent token bucket / sliding window rate limiter. | Rust, Tokio, DashMap, Lock-Sharded State | [View Code](./sliding_window_rate_limiter) |
| *[Project 2]* | *Brief description of the project* | *Tech stack / concepts* | *`./project-2`* |
| *[Project 3]* | *Brief description of the project* | *Tech stack / concepts* | *`./project-3`* |

---

##  Repository Layout

```text
.
├── sliding_window_rate_limiter/   # Lock-free/Sharded rate limiter middleware
├── project-2/                     # [Description]
├── project-3/                     # [Description]
└── README.md                      # Index & Portfolio Overview

```

---

##  How to Run Any Sub-Project

1. **Clone the monorepo:**
```bash
git clone [https://github.com/idorocodes/rust_erga.git](https://github.com/idorocodes/rust_erga.git)
cd rust_erga

```


2. **Navigate into a project directory:**
```bash
cd sliding_window_rate_limiter

```


3. **Build and run:**
```bash
cargo test
cargo run

```



