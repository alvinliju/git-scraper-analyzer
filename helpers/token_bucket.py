import time
import threading

class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        """
        Initializes a token bucket for rate limiting.

        :param capacity: The maximum number of tokens the bucket can hold.
        :param refill_rate: The rate at which tokens are added to the bucket (tokens per second).
        """
        self.capacity = float(capacity)
        self._tokens = float(capacity)  # Start with a full bucket
        self.refill_rate = float(refill_rate)
        self.last_refill_time = time.monotonic()  # Use monotonic time for consistency
        self.lock = threading.Lock()  # For thread-safe operations

    def _refill_tokens(self):
        """Refills the tokens based on elapsed time since the last refill."""
        now = time.monotonic()
        elapsed_time = now - self.last_refill_time
        tokens_to_add = elapsed_time * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self.last_refill_time = now

    def consume(self, tokens_needed: int = 1) -> bool:
        """
        Attempts to consume a specified number of tokens.

        :param tokens_needed: The number of tokens required for the operation.
        :return: True if tokens were successfully consumed, False otherwise.
        """
        with self.lock:
            self._refill_tokens()  # Always refill before attempting to consume
            if self._tokens >= tokens_needed:
                self._tokens -= tokens_needed
                return True
            return False

# Example Usage:
if __name__ == "__main__":
    # Create a bucket with capacity 10 and a refill rate of 2 tokens/second
    bucket = TokenBucket(capacity=10, refill_rate=2)

    print("Attempting to consume 5 tokens...")
    if bucket.consume(5):
        print("Consumed 5 tokens.")
    else:
        print("Failed to consume 5 tokens.")

    print(f"Current tokens: {bucket._tokens:.2f}")

    print("\nWaiting for 2 seconds to allow tokens to refill...")
    time.sleep(2)

    print(f"Current tokens after waiting: {bucket._tokens:.2f}") # Tokens should have refilled

    print("\nAttempting to consume 8 tokens...")
    if bucket.consume(8):
        print("Consumed 8 tokens.")
    else:
        print("Failed to consume 8 tokens.")

    print(f"Current tokens: {bucket._tokens:.2f}")

    print("\nAttempting to consume 5 tokens again (should fail)...")
    if bucket.consume(5):
        print("Consumed 5 tokens.")
    else:
        print("Failed to consume 5 tokens.")

    print(f"Current tokens: {bucket._tokens:.2f}")