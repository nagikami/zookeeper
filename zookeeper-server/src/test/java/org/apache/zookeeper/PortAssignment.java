/*
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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assign ports to tests */
public final class PortAssignment {

    private static final Logger LOG = LoggerFactory.getLogger(PortAssignment.class);

    // The available port range that we use stays away from the ephemeral port
    // range, which the OS will assign to client socket connections.  We can't
    // coordinate with the OS on the assignment of those ports, so it's best to
    // stay out of that range to avoid conflicts.  Typical ranges for ephemeral
    // ports are:
    // - IANA suggests 49152 - 65535
    // - Linux typically uses 32768 - 61000
    // - FreeBSD modern versions typically use the IANA suggested range
    // - Windows modern versions typically use the IANA suggested range
    private static final int GLOBAL_BASE_PORT = 11221;
    private static final int GLOBAL_MAX_PORT = 32767;

    private static PortRange portRange = null;
    private static int nextPort;

    /**
     * Assign a new, unique port to the test.  This method works by assigning
     * ports from a valid port range as identified by the total number of
     * concurrent test processes and the ID of this test process.  Each
     * concurrent test process uses an isolated range, so it's not possible for
     * multiple test processes to collide on the same port.  Within the port
     * range, ports are assigned in monotonic（单调） increasing order, wrapping around
     * to the beginning of the range if needed.  As an extra precaution, the
     * method attempts to bind to the port and immediately close it before
     * returning it to the caller.  If the port cannot be bound, then it tries
     * the next one in the range.  This provides some resiliency in case the port
     * is otherwise occupied, such as a developer running other servers on the
     * machine running the tests.
     * 为每一个并发测试线程提供指定范围的端口号，在测试端口号可绑定后返回
     * @return port
     */
    public static synchronized int unique() {
        if (portRange == null) {
            Integer threadId = Integer.getInteger("zookeeper.junit.threadid");
            portRange = setupPortRange(
                System.getProperty("test.junit.threads"),
                threadId != null ? "threadid=" + threadId : System.getProperty("sun.java.command"));
            // 获取区间左边界
            nextPort = portRange.getMinimum();
        }
        int candidatePort = nextPort;
        for (; ; ) {
            ++candidatePort;
            // 区间遍历完后，置为左边界值
            if (candidatePort > portRange.getMaximum()) {
                candidatePort = portRange.getMinimum();
            }
            if (candidatePort == nextPort) {
                throw new IllegalStateException(String.format(
                    "Could not assign port from range %s.  The entire range has been exhausted.",
                    portRange));
            }
            try {
                // 尝试绑定端口，成功则返回
                ServerSocket s = new ServerSocket(candidatePort);
                s.close();
                nextPort = candidatePort;
                LOG.info("Assigned port {} from range {}.", nextPort, portRange);
                return nextPort;
            } catch (IOException e) {
                LOG.debug(
                    "Could not bind to port {} from range {}.  Attempting next port.",
                    candidatePort,
                    portRange,
                    e);
            }
        }
    }

    /**
     * Sets up the port range to be used.  In typical usage, Ant invokes JUnit,
     * possibly using multiple JUnit processes to execute multiple test suites
     * concurrently.  The count of JUnit processes is passed from Ant as a system
     * property named "test.junit.threads".  Ant's JUnit runner receives the
     * thread ID as a command line argument of the form threadid=N, where N is an
     * integer in the range [1, ${test.junit.threads}].  It's not otherwise
     * accessible, so we need to parse it from the command line.  This method
     * uses these 2 pieces of information to split the available ports into
     * disjoint ranges.  Each JUnit process only assigns ports from its own range
     * in order to prevent bind errors during concurrent test runs.  If any of
     * this information is unavailable or unparseable, then the default behavior
     * is for this process to use the entire available port range.  This is
     * expected when running tests outside of Ant.
     * ant在调用JUnit时会传入test.junit.threads系统变量和threadid=N参数
     *
     * @param strProcessCount string representation of integer process count,
     *         typically taken from system property test.junit.threads
     * @param cmdLine command line containing threadid=N argument, typically
     *         taken from system property sun.java.command
     * @return port range to use
     */
    static PortRange setupPortRange(String strProcessCount, String cmdLine) {
        Integer processCount = null;
        if (strProcessCount != null && !strProcessCount.isEmpty()) {
            try {
                processCount = Integer.valueOf(strProcessCount);
            } catch (NumberFormatException e) {
                LOG.warn("Error parsing test.junit.threads = {}.", strProcessCount, e);
            }
        }

        Integer threadId = null;
        if (processCount != null) {
            if (cmdLine != null && !cmdLine.isEmpty()) {
                Matcher m = Pattern.compile("threadid=(\\d+)").matcher(cmdLine);
                if (m.find()) {
                    try {
                        threadId = Integer.valueOf(m.group(1));
                    } catch (NumberFormatException e) {
                        LOG.warn("Error parsing threadid from {}.", cmdLine, e);
                    }
                }
            }
        }

        final PortRange newPortRange;
        if (processCount != null && processCount > 1 && threadId != null) {
            // We know the total JUnit process count and this test process's ID.
            // Use these values to calculate the valid range for port assignments
            // within this test process.  We lose a few possible ports to the
            // remainder, but that's acceptable.
            // 计算端口可选范围（总区间 / 区间数）
            int portRangeSize = (GLOBAL_MAX_PORT - GLOBAL_BASE_PORT) / processCount;
            // 获取子区间左边界
            int minPort = GLOBAL_BASE_PORT + ((threadId - 1) * portRangeSize);
            // 获取子区间右边界
            int maxPort = minPort + portRangeSize - 1;
            newPortRange = new PortRange(minPort, maxPort);
            LOG.info("Test process {}/{} using ports from {}.", threadId, processCount, newPortRange);
        } else {
            // If running outside the context of Ant or Ant is using a single
            // test process, then use all valid ports.
            // 不通过ant，或者单独运行一个测试线程，返回总区间
            newPortRange = new PortRange(GLOBAL_BASE_PORT, GLOBAL_MAX_PORT);
            LOG.info("Single test process using ports from {}.", newPortRange);
        }

        return newPortRange;
    }

    /**
     * Contains the minimum and maximum (both inclusive) in a range of ports.
     */
    static final class PortRange {

        private final int minimum;
        private final int maximum;

        /**
         * Creates a new PortRange.
         *
         * @param minimum lower bound port number
         * @param maximum upper bound port number
         */
        PortRange(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        /**
         * Returns maximum port in the range.
         *
         * @return maximum
         */
        int getMaximum() {
            return maximum;
        }

        /**
         * Returns minimum port in the range.
         *
         * @return minimum
         */
        int getMinimum() {
            return minimum;
        }

        @Override
        public String toString() {
            return String.format("%d - %d", minimum, maximum);
        }

    }

    /**
     * There is no reason to instantiate this class.
     */
    private PortAssignment() {
    }

}
