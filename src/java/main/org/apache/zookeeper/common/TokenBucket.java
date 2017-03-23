package org.apache.zookeeper.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple rate limiter based on a token bucket.
  */
public class TokenBucket implements RateLimiter {

    // VisibleForTesting
    long refreshPeriodNanos = TimeUnit.SECONDS.toNanos(1L);
    // VisibleForTesting
    volatile long nextRefillTime;
    private AtomicLong tokens;
    private long capacity;
    private long tokensPerPeriod;

    public TokenBucket() { }

    // VisibleForTesting
    public TokenBucket(long capacity, long tokensPerSecond, long initialTokens) {
        this.tokens = new AtomicLong(initialTokens);
        this.capacity = Math.max(capacity, tokensPerSecond);
        this.tokensPerPeriod = tokensPerSecond;
        this.nextRefillTime = System.nanoTime() + refreshPeriodNanos;
    }

    @Override
    public boolean tryAquire() {
        refill();
        long current = tokens.get();
        while (current > 0) {
            if (tokens.compareAndSet(current, current - 1)) {
                return true;
            }
            current = tokens.get();
        }
        return false;
    }

    private void refill() {
        long currentTime = System.nanoTime();
        if (currentTime >= nextRefillTime) {
            synchronized (this) {
                if (currentTime >= nextRefillTime) {
                    long elapsedNanos = currentTime - nextRefillTime + refreshPeriodNanos;
                    long elapsedPeriods = elapsedNanos / refreshPeriodNanos;
                    long newTokens = elapsedPeriods * tokensPerPeriod;
                    tokens.set(Math.min(capacity, tokens.get() + newTokens));
                    nextRefillTime = currentTime + refreshPeriodNanos - (elapsedNanos % refreshPeriodNanos);
                }
            }
        }
    }

    // VisibleForTesting
    long getTokenCount() {
        return tokens.get();
    }

    @Override
    public void configure(int maxClientCnxnRate, int maxClientCnxnBurst) {
        this.tokens = new AtomicLong(maxClientCnxnBurst);
        this.capacity = Math.max(capacity, maxClientCnxnRate);
        this.tokensPerPeriod = maxClientCnxnRate;
        this.nextRefillTime = System.nanoTime() + refreshPeriodNanos;
    }
}