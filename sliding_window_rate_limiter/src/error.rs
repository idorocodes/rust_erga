use thiserror::Error;



#[derive(Error,Debug)]
pub enum  RateLimiterError {
    #[error("Too many requests, try again later........")]
    TooManyRequests
}