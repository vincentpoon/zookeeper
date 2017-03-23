/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zookeeper.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection rate limiter.
 *
 */
public interface RateLimiter {

    /**
     * If maxClientCnxnRate or maxClientCnxnBurst is set to this value, rate is
     * not limited
     */
    public static int BYPASS = -1;

    /**
     * A {@code RateLimiter} that does not do any rate limiting
     */
    public static RateLimiter BYPASS_RATE_LIMITER = new RateLimiter() {
        @Override
        public boolean tryAquire() {
            return true;
        }

        @Override
        public void configure(int maxClientCnxnRate, int maxClientCnxnBurst) {
            //not needed
        }
    };

    /**
     * Attempts to acquire a permit
     *
     * @return true if a permit was acquired, false otherwise
     */
    public boolean tryAquire();

    /**
     * @param maxClientCnxnRate the max client connection rate
     * @param maxClientCnxnBurst the max client connection burst
     */
    public void configure(int maxClientCnxnRate, int maxClientCnxnBurst);

    public static class Factory {

        private static final Logger LOG = LoggerFactory.getLogger(RateLimiter.Factory.class);

        /**
         * Creates a {@code RateLimiter} with a stable average throughput of
         * {@code averageRate} and a maximum burst size of {@code burstRate}
         *
         * @param rateLimiterImplClass the {@code RateLimiter} implementation to use
         * @param burstSize
         *            The maximum burst size - i.e. max number of permits that
         *            can be acquired in a second
         * @param averageRate
         *            The stable average rate in permits per second
         * @return the {@code RateLimiter}
         */
        public static RateLimiter create(String rateLimiterImplClass, int burstSize,
                int averageRate) {
            if (burstSize == BYPASS || averageRate == BYPASS) {
                return BYPASS_RATE_LIMITER;
            }
            if (rateLimiterImplClass == null) {
                rateLimiterImplClass = TokenBucket.class.getName();
            }
            try {
                RateLimiter limiter = (RateLimiter) Class.forName(rateLimiterImplClass).newInstance();
                limiter.configure(averageRate, burstSize);
                return limiter;
            } catch (Exception e) {
                LOG.error("Error instantiating RateLimiter", e);
                return BYPASS_RATE_LIMITER;
            }
        }
    }

}
