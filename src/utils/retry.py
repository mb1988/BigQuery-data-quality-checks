"""
Retry decorator with exponential backoff.
Production-grade retry logic for transient failures.
"""
import time
import functools
import logging
from typing import Callable, Type, Tuple, Optional

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None,
):
    """
    Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback function called on each retry

    Example:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        def my_function():
            # Code that might fail
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    logger.warning(
                        f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )

                    if on_retry:
                        on_retry(attempt, e)

                    time.sleep(current_delay)
                    current_delay *= backoff

            # This should never be reached, but just in case
            raise last_exception

        return wrapper
    return decorator


class RetryableError(Exception):
    """Exception that should be retried."""
    pass
