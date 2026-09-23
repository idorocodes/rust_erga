use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
};

use tower::util::ServiceExt;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::error::RateLimiterError;

mod error;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_burst: u32,
    pub refill_interval_ms: u64,
}

#[derive(Clone)]
pub struct RateLimiter {
    config: RateLimitConfig,
    state_map: Arc<DashMap<Uuid, BucketState>>,
}

#[derive(Debug, Clone, Copy)]
pub struct BucketState {
    pub last_updated: Instant,
    pub tokens: u32,
}

impl RateLimiter {
    pub fn new(max_burst: u32, refill_interval_ms: u64) -> Self {
        Self {
            config: RateLimitConfig {
                max_burst,
                refill_interval_ms,
            },
            state_map: Arc::new(DashMap::new()),
        }
    }

    pub fn gate_man(&self, key: Uuid) -> Result<(), RateLimiterError> {
        let now = Instant::now();

        // Mutably lock the specific client's entry in DashMap
        let mut client_bucket = self.state_map.entry(key).or_insert(BucketState {
            last_updated: now,
            tokens: self.config.max_burst,
        });

        let elapsed = now.duration_since(client_bucket.last_updated).as_millis() as u64;

        // Calculate tokens earned
        let new_tokens = elapsed / self.config.refill_interval_ms;

        if new_tokens > 0 {
            client_bucket.tokens =
                (client_bucket.tokens + new_tokens as u32).min(self.config.max_burst);

            // FIX: Only advance time by the exact discrete intervals we consumed to prevent time drift math loss
            let consumed_time_ms = new_tokens * self.config.refill_interval_ms;
            client_bucket.last_updated += Duration::from_millis(consumed_time_ms);
        }

        if client_bucket.tokens >= 1 {
            client_bucket.tokens -= 1;
            Ok(())
        } else {
            Err(RateLimiterError::TooManyRequests)
        }
    }
}

async fn logger(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = Instant::now();
    let response = next.run(req).await;
    println!(
        "{} {} → {} ({}ms)",
        method,
        uri,
        response.status(),
        start.elapsed().as_millis()
    );
    response
}
// Convert middleware errors safely into clean HTTP Responses
async fn rate_limiter_middleware(
    State(state): State<RateLimiter>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // 1. Extract authorization header value safely
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?; 

    // 2. Strip the "Bearer " scheme prefix safely
    let raw_uuid_str = auth_header
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::BAD_REQUEST)?; // 400 if it doesn't start with "Bearer "

    // 3. Safely parse the pure UUID string slice
    let uuid_key = Uuid::parse_str(raw_uuid_str).map_err(|_| StatusCode::BAD_REQUEST)?;

    // 4. Evaluate rate limit bucket status
    match state.gate_man(uuid_key) {
        Ok(_) => Ok(next.run(req).await),
        Err(RateLimiterError::TooManyRequests) => Err(StatusCode::TOO_MANY_REQUESTS), 
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    response: String,
}

async fn public_route() -> Json<Message> {
    let uuid = Uuid::new_v4().to_string();
    Json(Message { response: uuid })
}

async fn rate() -> impl IntoResponse {
    Json(Message {
        response: String::from("rate limiter doing it's work"),
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 1. Initialize your shared state
    let rate_limiter = RateLimiter::new(5, 10000); // 5 bursts, 1 token every 10 seconds
    println!("Server rate limiter initialized.");

    // 2. Build the protected route sub-router using `from_fn_with_state`
    let rate_router =
        Router::new()
            .route("/rate", get(rate))
            .route_layer(middleware::from_fn_with_state(
                rate_limiter.clone(),
                rate_limiter_middleware,
            ));

    // 3. Build the root app router, merging the public route and the rate-limited sub-router
    let app = Router::new()
        .route("/public", get(public_route))
        .merge(rate_router)
        .layer(middleware::from_fn(logger));

    // 4. Bind and run the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Server running on http://127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}




#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use futures::future::join_all;
    use reqwest::Client;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use tower::ServiceExt; // Provides .oneshot() for testing routes without a socket
    use uuid::Uuid;

    // Helper function to build the test router in-memory
    fn setup_test_app(max_burst: u32, refill_interval_ms: u64) -> Router {
        let rate_limiter = RateLimiter::new(max_burst, refill_interval_ms);

        let rate_router = Router::new()
            .route("/rate", get(rate))
            .route_layer(middleware::from_fn_with_state(
                rate_limiter.clone(),
                rate_limiter_middleware,
            ));

        Router::new()
            .route("/public", get(public_route))
            .merge(rate_router)
    }

    // =========================================================================
    // 1. UNIT TESTS (Testing RateLimiter Struct Directly)
    // =========================================================================

    #[test]
    fn test_unit_rate_limiter_burst_capacity() {
        let max_burst = 3;
        let limiter = RateLimiter::new(max_burst, 1000);
        let user_id = Uuid::new_v4();

        // Should allow up to `max_burst` calls
        assert!(limiter.gate_man(user_id).is_ok());
        assert!(limiter.gate_man(user_id).is_ok());
        assert!(limiter.gate_man(user_id).is_ok());

        // 4th call must be blocked
        assert!(matches!(
            limiter.gate_man(user_id),
            Err(RateLimiterError::TooManyRequests)
        ));
    }

    #[test]
    fn test_unit_rate_limiter_isolation() {
        let limiter = RateLimiter::new(1, 1000);
        let user_a = Uuid::new_v4();
        let user_b = Uuid::new_v4();

        // User A exhausts their limit
        assert!(limiter.gate_man(user_a).is_ok());
        assert!(limiter.gate_man(user_a).is_err());

        // User B should still be allowed
        assert!(limiter.gate_man(user_b).is_ok());
    }

    #[test]
    fn test_unit_rate_limiter_token_refill() {
        let limiter = RateLimiter::new(1, 100); // 1 token, refills every 100ms
        let user_id = Uuid::new_v4();

        // Consume initial token
        assert!(limiter.gate_man(user_id).is_ok());
        assert!(limiter.gate_man(user_id).is_err());

        // Wait for refill interval
        sleep(Duration::from_millis(110));

        // Token should be refilled now
        assert!(limiter.gate_man(user_id).is_ok());
    }

    // =========================================================================
    // 2. MIDDLEWARE & HTTP TESTS (In-Memory Without Real Sockets)
    // =========================================================================

    #[tokio::test]
    async fn test_unprotected_route_bypasses_middleware() {
        let app = setup_test_app(1, 1000);

        let req = Request::builder()
            .uri("/public")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_missing_auth_header_returns_401() {
        let app = setup_test_app(5, 1000);

        let req = Request::builder()
            .uri("/rate")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_invalid_bearer_prefix_returns_400() {
        let app = setup_test_app(5, 1000);

        let req = Request::builder()
            .uri("/rate")
            .header(header::AUTHORIZATION, "Basic 123456789")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_uuid_returns_400() {
        let app = setup_test_app(5, 1000);

        let req = Request::builder()
            .uri("/rate")
            .header(header::AUTHORIZATION, "Bearer not-a-valid-uuid")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_rate_limiter_middleware_allows_burst_then_blocks() {
        let max_burst = 2;
        let app = setup_test_app(max_burst, 10_000);
        let valid_uuid = Uuid::new_v4().to_string();
        let auth_header = format!("Bearer {valid_uuid}");

        // Request 1: Allowed (OK)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rate")
                    .header(header::AUTHORIZATION, &auth_header)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Request 2: Allowed (OK)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rate")
                    .header(header::AUTHORIZATION, &auth_header)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Request 3: Exceeded burst -> 429 Too Many Requests
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/rate")
                    .header(header::AUTHORIZATION, &auth_header)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    // =========================================================================
    // 3. 10,000 CONCURRENT STRESS TESTS (Against Running Server)
    // =========================================================================

    const SERVER_URL: &str = "http://127.0.0.1:3000/rate";
    const TOTAL_REQUESTS: usize = 10_000;

    #[tokio::test]
    #[ignore = "Requires running server: cargo run"]
    async fn stress_test_10k_single_user_burst() {
        let client = Client::builder()
            .pool_max_idle_per_host(500)
            .build()
            .unwrap();

        let single_user_uuid = Uuid::new_v4().to_string();
        let auth_header_single = format!("Bearer {single_user_uuid}");

        let ok_count = Arc::new(AtomicUsize::new(0));
        let rate_limited_count = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));

        println!("\n🔥 Launching {TOTAL_REQUESTS} concurrent requests (Single User)...");
        let start = Instant::now();
        let mut tasks = Vec::with_capacity(TOTAL_REQUESTS);

        for _ in 0..TOTAL_REQUESTS {
            let client = client.clone();
            let auth_header = auth_header_single.clone();
            let ok_counter = Arc::clone(&ok_count);
            let limit_counter = Arc::clone(&rate_limited_count);
            let err_counter = Arc::clone(&error_count);

            tasks.push(tokio::spawn(async move {
                let res = client
                    .get(SERVER_URL)
                    .header("Authorization", auth_header)
                    .send()
                    .await;

                match res {
                    Ok(response) => match response.status() {
                        reqwest::StatusCode::OK => {
                            ok_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        reqwest::StatusCode::TOO_MANY_REQUESTS => {
                            limit_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            err_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        err_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        join_all(tasks).await;
        let duration = start.elapsed();

        let total_ok = ok_count.load(Ordering::Relaxed);
        let total_limited = rate_limited_count.load(Ordering::Relaxed);
        let total_errors = error_count.load(Ordering::Relaxed);

        println!("--------------------------------------------------");
        println!("RESULTS FOR SINGLE USER BURST:");
        println!("⏱️  Total Time Taken    : {:.2?}", duration);
        println!("🚀 Requests Per Second : {:.0} req/sec", TOTAL_REQUESTS as f64 / duration.as_secs_f64());
        println!("✅ 200 OK Allowed       : {total_ok}");
        println!("🛑 429 Rate Limited     : {total_limited}");
        println!("💥 Network/Server Errors: {total_errors}");
        println!("--------------------------------------------------");

        // Assuming max_burst = 5 in main()
        assert_eq!(total_ok, 5);
        assert_eq!(total_limited, TOTAL_REQUESTS - 5);
        assert_eq!(total_errors, 0);
    }

    #[tokio::test]
    #[ignore = "Requires running server: cargo run"]
    async fn stress_test_10k_unique_users() {
        let client = Client::builder()
            .pool_max_idle_per_host(500)
            .build()
            .unwrap();

        let ok_count = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));

        println!("\n🔥 Launching {TOTAL_REQUESTS} requests from 10,000 UNIQUE users...");
        let start = Instant::now();
        let mut tasks = Vec::with_capacity(TOTAL_REQUESTS);

        for _ in 0..TOTAL_REQUESTS {
            let client = client.clone();
            let auth_header = format!("Bearer {}", Uuid::new_v4());
            let ok_counter = Arc::clone(&ok_count);
            let err_counter = Arc::clone(&error_count);

            tasks.push(tokio::spawn(async move {
                let res = client
                    .get(SERVER_URL)
                    .header("Authorization", auth_header)
                    .send()
                    .await;

                if let Ok(response) = res {
                    if response.status() == reqwest::StatusCode::OK {
                        ok_counter.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                err_counter.fetch_add(1, Ordering::Relaxed);
            }));
        }

        join_all(tasks).await;
        let duration = start.elapsed();

        let total_ok = ok_count.load(Ordering::Relaxed);
        let total_errors = error_count.load(Ordering::Relaxed);

        println!("--------------------------------------------------");
        println!("RESULTS FOR 10,000 UNIQUE USERS:");
        println!("⏱️  Total Time Taken    : {:.2?}", duration);
        println!("🚀 Requests Per Second : {:.0} req/sec", TOTAL_REQUESTS as f64 / duration.as_secs_f64());
        println!("✅ 200 OK Allowed       : {total_ok}");
        println!("💥 Errors / Blocked     : {total_errors}");
        println!("--------------------------------------------------");

        assert_eq!(total_ok, TOTAL_REQUESTS);
        assert_eq!(total_errors, 0);
    }
}