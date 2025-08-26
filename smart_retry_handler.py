import asyncio
import time
import random
from typing import Optional, Callable, Any, Dict, List, Union
from dataclasses import dataclass
from enum import Enum
import httpx
from .logger import get_logger
from .constants import (
    CIRCUIT_BREAKER_MAX_ATTEMPTS, CIRCUIT_BREAKER_BASE_DELAY, CIRCUIT_BREAKER_MAX_DELAY,
    CIRCUIT_BREAKER_EXPONENTIAL_FACTOR, CIRCUIT_BREAKER_JITTER, CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT, CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS,
    CIRCUIT_BREAKER_RATE_LIMIT_RECOVERY_MULTIPLIER
)

logger = get_logger()

class ErrorType(Enum):
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    UNKNOWN = "unknown"

@dataclass
class SmartRetryConfig:
    max_attempts: int = CIRCUIT_BREAKER_MAX_ATTEMPTS
    base_delay: float = CIRCUIT_BREAKER_BASE_DELAY
    max_delay: float = CIRCUIT_BREAKER_MAX_DELAY
    exponential_factor: float = CIRCUIT_BREAKER_EXPONENTIAL_FACTOR
    jitter: bool = CIRCUIT_BREAKER_JITTER
    # Circuit breaker settings - much more lenient for APIs
    failure_threshold: int = CIRCUIT_BREAKER_FAILURE_THRESHOLD  # Higher threshold
    recovery_timeout: float = CIRCUIT_BREAKER_RECOVERY_TIMEOUT  # Shorter recovery time
    half_open_max_calls: int = CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS  # Test with limited calls
    rate_limit_recovery_multiplier: float = CIRCUIT_BREAKER_RATE_LIMIT_RECOVERY_MULTIPLIER  # Extra time for rate limits

@dataclass
class SmartCircuitBreakerState:
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    half_open_calls: int = 0
    consecutive_rate_limits: int = 0  # Track rate limit specific failures
    last_rate_limit_time: Optional[float] = None

class SmartRetryHandler:
    """
    Enhanced retry handler designed specifically for API services with intelligent
    circuit breaker logic that differentiates between different types of failures.
    """
    
    def __init__(self, config: Optional[SmartRetryConfig] = None):
        self.config = config or SmartRetryConfig()
        self.circuit_breakers: Dict[str, SmartCircuitBreakerState] = {}
    
    def classify_error(self, error: Exception) -> ErrorType:
        """Classify error type for appropriate retry strategy."""
        error_str = str(error).lower()
        
        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code
            if status_code == 429:
                return ErrorType.RATE_LIMIT
            elif status_code in [502, 503, 504]:
                return ErrorType.SERVICE_UNAVAILABLE
            elif 500 <= status_code < 600:
                return ErrorType.SERVER_ERROR
        elif isinstance(error, (httpx.TimeoutException, asyncio.TimeoutError)):
            return ErrorType.TIMEOUT_ERROR
        elif isinstance(error, (httpx.NetworkError, httpx.ConnectError)):
            return ErrorType.NETWORK_ERROR
        elif "rate limit" in error_str or "quota" in error_str:
            return ErrorType.RATE_LIMIT
        elif "service unavailable" in error_str or "overloaded" in error_str:
            return ErrorType.SERVICE_UNAVAILABLE
        elif any(code in error_str for code in ["500", "502", "503", "504", "507"]):
            return ErrorType.SERVER_ERROR
        
        return ErrorType.UNKNOWN
    
    def extract_retry_after(self, error: Exception) -> Optional[float]:
        """Extract retry-after delay from rate limit errors."""
        error_str = str(error)
        
        # Look for "retry after X seconds" pattern
        import re
        retry_patterns = [
            r"retry after (\d+) seconds?",
            r"please retry after (\d+)",
            r"retry-after[:\s]+(\d+)"
        ]
        
        for pattern in retry_patterns:
            match = re.search(pattern, error_str, re.IGNORECASE)
            if match:
                return float(match.group(1))
        
        # Check HTTP headers if it's an HTTP error
        if isinstance(error, httpx.HTTPStatusError):
            retry_after = error.response.headers.get("retry-after")
            if retry_after:
                try:
                    return float(retry_after)
                except ValueError:
                    pass
        
        return None
    
    def calculate_delay(self, attempt: int, error_type: ErrorType, retry_after: Optional[float] = None) -> float:
        """Calculate delay before next retry attempt."""
        if retry_after is not None and error_type == ErrorType.RATE_LIMIT:
            # Use the suggested retry-after time with multiplier for safety
            delay = retry_after * self.config.rate_limit_recovery_multiplier
        else:
            # Exponential backoff
            delay = self.config.base_delay * (self.config.exponential_factor ** (attempt - 1))
        
        # Cap the delay
        delay = min(delay, self.config.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.config.jitter:
            jitter_range = delay * 0.25  # 25% jitter
            jitter = random.uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay + jitter)  # Minimum 100ms delay
        
        return delay
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if we should retry based on error type and attempt count."""
        if attempt >= self.config.max_attempts:
            return False
        
        error_type = self.classify_error(error)
        
        # Always retry on these errors
        retryable_errors = [
            ErrorType.RATE_LIMIT,
            ErrorType.SERVER_ERROR,
            ErrorType.NETWORK_ERROR,
            ErrorType.TIMEOUT_ERROR,
            ErrorType.SERVICE_UNAVAILABLE
        ]
        
        return error_type in retryable_errors
    
    def check_circuit_breaker(self, key: str) -> bool:
        """Check if circuit breaker allows the request with smart logic."""
        if key not in self.circuit_breakers:
            self.circuit_breakers[key] = SmartCircuitBreakerState()
        
        breaker = self.circuit_breakers[key]
        current_time = time.time()
        
        if breaker.state == "OPEN":
            # Check if we should try to recover
            # Handle both regular failures and rate-limit-caused openings
            failure_time = breaker.last_failure_time
            if failure_time is None and breaker.last_rate_limit_time is not None:
                # Circuit breaker was opened due to rate limits, use last_rate_limit_time
                failure_time = breaker.last_rate_limit_time
            
            if failure_time is not None and current_time - failure_time > self.config.recovery_timeout:
                breaker.state = "HALF_OPEN"
                breaker.half_open_calls = 0
                logger.info(f"Circuit breaker {key} transitioning to HALF_OPEN")
                return True
            elif failure_time is None:
                # Edge case: Both last_failure_time and last_rate_limit_time are None
                # This should not happen in normal operation, but if it does, we should
                # transition to HALF_OPEN to prevent permanently stuck circuit breakers
                logger.warning(f"Circuit breaker {key} has no failure timestamp - transitioning to HALF_OPEN to prevent deadlock")
                breaker.state = "HALF_OPEN"
                breaker.half_open_calls = 0
                return True
            return False
        elif breaker.state == "HALF_OPEN":
            # Allow limited calls in half-open state
            if breaker.half_open_calls < self.config.half_open_max_calls:
                breaker.half_open_calls += 1
                return True
            return False
        
        return True
    
    def record_success(self, key: str):
        """Record successful operation for circuit breaker with smart logic."""
        if key in self.circuit_breakers:
            breaker = self.circuit_breakers[key]
            breaker.success_count += 1
            breaker.last_success_time = time.time()
            
            # Reset rate limit specific counters on success
            breaker.consecutive_rate_limits = 0
            
            if breaker.state == "HALF_OPEN":
                # If we have enough successful calls in half-open, close the breaker
                if breaker.success_count >= 2:  # Require at least 2 successes
                    breaker.state = "CLOSED"
                    breaker.failure_count = 0
                    logger.info(f"Circuit breaker {key} recovered to CLOSED")
            elif breaker.state == "CLOSED":
                # Gradually reduce failure count on success
                breaker.failure_count = max(0, breaker.failure_count - 1)
    
    def record_failure(self, key: str, error_type: ErrorType):
        """Record failed operation for circuit breaker with smart error handling."""
        if key not in self.circuit_breakers:
            self.circuit_breakers[key] = SmartCircuitBreakerState()
        
        breaker = self.circuit_breakers[key]
        current_time = time.time()
        
        # Handle rate limits differently - they're not "real" failures
        if error_type == ErrorType.RATE_LIMIT:
            breaker.consecutive_rate_limits += 1
            breaker.last_rate_limit_time = current_time
            
            # Only count as failure if we have many consecutive rate limits
            if breaker.consecutive_rate_limits > 15:  # Very high threshold for rate limits
                breaker.failure_count += 1
        else:
            # Real failures (server errors, network issues, etc.)
            breaker.failure_count += 1
            breaker.last_failure_time = current_time
        
        # Open circuit breaker only for persistent real failures
        if breaker.failure_count >= self.config.failure_threshold:
            if breaker.state != "OPEN":
                breaker.state = "OPEN"
                logger.warning(f"Circuit breaker {key} OPENED after {breaker.failure_count} failures")
        elif breaker.state == "HALF_OPEN":
            # Failed in half-open state, go back to open
            breaker.state = "OPEN"
            breaker.last_failure_time = current_time
            logger.warning(f"Circuit breaker {key} failed in HALF_OPEN, returning to OPEN")
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        circuit_breaker_key: Optional[str] = None,
        max_attempts: Optional[int] = None,
        **kwargs
    ) -> Any:
        """Execute function with smart retry logic and circuit breaker."""
        
        # Check circuit breaker if key provided
        if circuit_breaker_key and not self.check_circuit_breaker(circuit_breaker_key):
            raise RuntimeError(f"Circuit breaker {circuit_breaker_key} is OPEN - service temporarily unavailable")
        
        last_error = None
        effective_max_attempts = max_attempts or self.config.max_attempts
        
        for attempt in range(1, effective_max_attempts + 1):
            try:
                logger.debug(f"Attempt {attempt}/{effective_max_attempts} for {circuit_breaker_key or 'unknown'}")
                
                result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
                
                # Record success for circuit breaker
                if circuit_breaker_key:
                    self.record_success(circuit_breaker_key)
                
                if attempt > 1:
                    logger.info(f"Operation succeeded on attempt {attempt} for {circuit_breaker_key or 'unknown'}")
                
                return result
                
            except Exception as error:
                last_error = error
                error_type = self.classify_error(error)
                
                logger.warning(f"Attempt {attempt} failed with {error_type.value}: {str(error)}")
                
                # Record failure for circuit breaker
                if circuit_breaker_key:
                    self.record_failure(circuit_breaker_key, error_type)
                
                # Check if we should retry
                if not self.should_retry(error, attempt):
                    logger.error(f"Max retries exceeded or non-retryable error: {str(error)}")
                    break
                
                # Don't wait if this is the last attempt
                if attempt >= effective_max_attempts:
                    break
                
                # Calculate delay
                retry_after = self.extract_retry_after(error)
                delay = self.calculate_delay(attempt, error_type, retry_after)
                
                logger.info(f"Retrying in {delay:.2f} seconds (attempt {attempt + 1}/{effective_max_attempts})")
                await asyncio.sleep(delay)
        
        # All retries failed
        if last_error is not None:
            raise last_error
        else:
            raise RuntimeError("All retry attempts failed but no specific error was captured")
    
    def get_circuit_breaker_status(self, key: str) -> Dict:
        """Get detailed circuit breaker status."""
        if key not in self.circuit_breakers:
            return {"state": "CLOSED", "failure_count": 0, "success_count": 0}
        
        breaker = self.circuit_breakers[key]
        return {
            "state": breaker.state,
            "failure_count": breaker.failure_count,
            "success_count": breaker.success_count,
            "consecutive_rate_limits": breaker.consecutive_rate_limits,
            "last_failure_time": breaker.last_failure_time,
            "last_success_time": breaker.last_success_time,
            "last_rate_limit_time": breaker.last_rate_limit_time
        }

# Global smart retry handler instance
SMART_RETRY_HANDLER = SmartRetryHandler() 
