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

package org.apache.zookeeper.server.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */

public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{}={}", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{}={}", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */ long leader;

        /*
         * zxid of the proposed leader
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * current state of sender
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */ long peerEpoch;

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    public static class ToSend {

        enum mType {
            crequest,
            challenge,
            notification,
            ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         * 通知时提议的leader
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * Current state;
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         */ long peerEpoch;

    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        // 从消息接收队列获取选举响应（消息）
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        // 获取消息大小
                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        // 消息至少为28字节
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        // buffer标识恢复为初始状态，position置为0，limit置为capacity，mark丢弃置为-1
                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        // 实例化通知
                        Notification n = new Notification();

                        // 发送方服务器状态
                        int rstate = response.buffer.getInt();
                        // 读取leader服务器id
                        long rleader = response.buffer.getLong();
                        // 读取消息发送服务器事务id
                        long rzxid = response.buffer.getLong();
                        // 读取选举朝代
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {
                            if (!backCompatibility28) {
                                // 读取消息发送服务器朝代
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */
                                    // 读取通知版本
                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                // 从事务id获取朝代
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            // 版本大于1，消息包含配置数据
                            if (version > 0x1) {
                                // 读取配置数据字节数
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                                                        response.sid, capacity, version, configLength));
                                }

                                // 读取配置数据
                                byte[] b = new byte[configLength];
                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        // 校验接收到的quorum配置
                                        rqv = self.configFromString(new String(b, UTF_8));
                                        // 获取当前服务器的quorum配置
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        // 接收到的quorum版本比自己的版本高
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}",
                                                     self.getId(),
                                                     Long.toHexString(rqv.getVersion()),
                                                     Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            // 当前服务器状态为LOOKING
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                // 更新配置到接收到的高版本配置
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    // 配置升级失败，重启选举
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                     response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         * 如果消息来非quorum节点（例如观察者），快速响应，通知对方自己为leader
                         */
                        if (!validVoter(response.sid)) {
                            // 获取当前投票信息
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            // 构造通知消息
                            ToSend notmsg = new ToSend(
                                ToSend.mType.notification,
                                // leader为自己
                                current.getId(),
                                current.getZxid(),
                                logicalclock.get(),
                                self.getPeerState(),
                                response.sid,
                                current.getPeerEpoch(),
                                qv.toString().getBytes(UTF_8));

                            // 添加消息到sendqueue，等待被workerSender添加到对应的消息推送队列
                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message 消息来自quorum节点，为投票消息响应
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message 获取消息发送方服务器状态
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }

                            // 为通知赋值
                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, "
                                    + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                self.getPeerState(),
                                n.sid,
                                n.state,
                                n.leader,
                                Long.toHexString(n.electionEpoch),
                                Long.toHexString(n.peerEpoch),
                                Long.toHexString(n.zxid),
                                Long.toHexString(n.version),
                                (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             * 如果当前服务器状态为LOOKING，发送选举提议
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // 添加通知到接收队列
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 * 如果发送方状态也是LOOKING且选举朝代低于自己，构造新通知发送回去，
                                 * 将自己投票信息（包含leader、zxid）通知对方
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                    && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        v.getId(),
                                        v.getZxid(),
                                        logicalclock.get(),
                                        self.getPeerState(),
                                        response.sid,
                                        v.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 * 如果当前服务器状态不是LOOKING，但发送方是LOOKING，
                                 * 将自己记录的leader发送给它
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        // 报告LOOKING状态的服务器id
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                        "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                        self.getId(),
                                        response.sid,
                                        Long.toHexString(current.getZxid()),
                                        current.getId(),
                                        Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        // 自己投票信息，记录的leader
                                        current.getId(),
                                        current.getZxid(),
                                        current.getElectionEpoch(),
                                        self.getPeerState(),
                                        response.sid,
                                        current.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }
                        // 发送选举消息（添加消息到接收方的消息推送队列）
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                // 构造消息
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);

                // 发送消息（添加消息到接收方的消息推送队列）
                manager.toSend(m.sid, requestBuffer);

            }

        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         * 信使（用于接发消息）
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {
            // 创建消息发送线程，从sendqueue获取消息并添加消息到接收方的消息推送队列）
            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            // 创建响应接收线程·
            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;

    /**
     * Returns the current value of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        // buffer置为初始状态
        requestBuffer.clear();
        // 写入服务器状态
        requestBuffer.putInt(state);
        // 写入服务器记录的当前leader
        requestBuffer.putLong(leader);
        // 写入服务器的最后事务id
        requestBuffer.putLong(zxid);
        // 写入服务器记录的选举朝代
        requestBuffer.putLong(electionEpoch);
        // 写入服务器的朝代
        requestBuffer.putLong(epoch);
        // 写入通知版本
        requestBuffer.putInt(Notification.CURRENTVERSION);
        // 写入配置数据长度
        requestBuffer.putInt(configData.length);
        // 写入配置数据
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        // 初始化消息队列
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        // 消息发送队列
        sendqueue = new LinkedBlockingQueue<ToSend>();
        // 消息接收队列
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug(
            "About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
            v.getId(),
            Long.toHexString(v.getZxid()),
            self.getId(),
            self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;
    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     * 广播提议
     */
    private void sendNotifications() {
        // 遍历所有quorum节点的sid
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // 构造通知（消息）
            ToSend notmsg = new ToSend(
                ToSend.mType.notification,
                proposedLeader,
                proposedZxid,
                logicalclock.get(),
                QuorumPeer.ServerState.LOOKING,
                sid,
                proposedEpoch,
                qv.toString().getBytes(UTF_8));

            LOG.debug(
                "Sending Notification: {} (n.leader), 0x{} (n.zxid), 0x{} (n.round), {} (recipient),"
                    + " {} (myid), 0x{} (n.peerEpoch) ",
                proposedLeader,
                Long.toHexString(proposedZxid),
                Long.toHexString(logicalclock.get()),
                sid,
                self.getId(),
                Long.toHexString(proposedEpoch));

            // 添加到选举消息发送队列，等待sendworker处理
            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug(
            "id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}",
            newId,
            curId,
            Long.toHexString(newZxid),
            Long.toHexString(curZxid));

        // 如果投票的leader的权重等于0，当前服务器赢得投票
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         * 下列情形，投票中的leader赢得投票：
         * 1、投票的朝代更高
         * 2、朝代相同，投票的事务id更大
         * 3、朝代和事务id相同，投票的服务器id更大
         */

        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                    && ((newZxid > curZxid)
                        || ((newZxid == curZxid)
                            && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
            && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            // 如果接收到的投票和自己的投票相同，则将投票的服务器id添加到响应集合
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     * 如果选举出leader，必须保证leader参与了投票并且响应了结果，避免leader发生分区时重复无效选举
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         * 如果所有的节点都认为自己是leader，自己就必须是leader
         * 如果自己不是leader，自己必须接收到leader的投票，且leader的状态为LEADING
         */

        // 自己不是leader
        if (leader != self.getId()) {
            // 自己没有接收到leader的投票，leader无效
            if (votes.get(leader) == null) {
                predicate = false;
                // 自己接收到的leader的投票，但是leader的状态不是LEADING，leader无效
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
            // 自己是leader但是自己的朝代和选举朝代不相同，leader无效
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug(
            "Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)",
            leader,
            Long.toHexString(zxid),
            proposedLeader,
            Long.toHexString(proposedZxid));

        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     * 基于当前服务器的提议leader更新节点状态
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        // 如果自己是leader，状态改为LEADING，否则为学习状态（FOLLOWING、OBSERVING）
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();
        // 更新当前节点的状态
        self.setPeerState(ss);
        // 如果自己是leader，记录此次选举的投票集合
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     * 当服务器状态变为LOOKING时开启新一轮选举，并广播到其他节点
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        // 保存选举开始时间
        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce（推导出） on whether a majority
             * of participants has voted for it.
             * 如果接收到的投票的选举朝代等于当前服务器的选举朝代（logicalclock），则将投票加入recvset，
             * 由此判断当前服务器在当前选举朝代是否拥有过半投票
             */
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             * 之前选举周期和当前选举周期的来自FOLLOWING或者LEADING的投票添加到outofelection，
             * 当前服务器加入较晚（选举已经结束，当前服务器加入时为LOOKING状态会发起一次提议
             * 并增加自己的选举朝代logicalclock，导致朝代大于选举出leader的朝代，
             * 因此接收到的通知属于旧朝代）时，据此学习谁是当前leader
             */
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = minNotificationInterval;

            synchronized (this) {
                // 当前服务器选举朝代自增
                logicalclock.incrementAndGet();
                // 更新选举提议信息，leader为自己，id为自己的id，参与者zxid、epoch为记录的值，其他角色（观察者）都为初始值-2^63
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info(
                "New election. My id = {}, proposed zxid=0x{}",
                self.getId(),
                Long.toHexString(proposedZxid));
            // 广播提议（leader为自己）
            sendNotifications();

            SyncedLearnerTracker voteSet;

            /*
             * Loop in which we exchange notifications until we find a leader
             * 循环，直到选举出leader
             */

            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 * 从选举消息接收队列获取通知
                 */
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // 未拉取到新通知，说明还未接收到所有节点的通知至少一次，需要继续发送消息或者检查连接
                if (n == null) {
                    // 消息已经发送（至少有一个推送队列为空，bug？）
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        // 尝试与所有节点保持连接（有则检查，无则创建）
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = Math.min(tmpTimeOut, maxNotificationInterval);
                    LOG.info("Notification time out: {}", notTimeout);
                } else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     * 接收到投票，只处理投票节点和提议leader在quorum的投票
                     */
                    switch (n.state) {
                        // 通知来自于LOOKING节点
                    case LOOKING:
                        if (getInitLastLoggedZxid() == -1) {
                            LOG.debug("Ignoring notification as our zxid is -1");
                            break;
                        }
                        if (n.zxid == -1) {
                            LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                            break;
                        }
                        // If notification > current, replace and send messages out
                        // 通知的选举朝代大于自己的选举朝代，代表自己落后于集群（可能是断掉之后重连的）
                        if (n.electionEpoch > logicalclock.get()) {
                            // 更新自己的选举朝代为通知的选举朝代
                            logicalclock.set(n.electionEpoch);
                            // 清空recvset，在当前选举朝代，自己还未接收到投票
                            recvset.clear();
                            // 投票中的leader赢得这次投票
                            if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                // 更新自己的提议为投票的值
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                // 当前服务器赢得这次投票，更新提议为自己的信息
                                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                            }
                            // 广播新提议（投票结果）
                            sendNotifications();
                        } else if (n.electionEpoch < logicalclock.get()) {
                            // 通知的朝代小于自己的朝代，代表自己节点状态有问题（可能发生分区，自己进行了多轮无效选举），跳出循环
                                LOG.debug(
                                    "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                    Long.toHexString(n.electionEpoch),
                                    Long.toHexString(logicalclock.get()));
                            break;
                            // 通知的朝代和自己相同，并且投票中的leader赢得这次投票
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                            // 更新自己的提议为投票的值
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            // 广播新提议（投票结果）
                            sendNotifications();
                        }
                        // 此时，通知的朝代和自己相同（落后时将朝代设为通知的，超前时会在上面跳出），
                        // 且自己的提议状态为最新（输了投票，更新为收到的leader，自己赢得投票则无需更新）

                        LOG.debug(
                            "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                            n.sid,
                            n.leader,
                            Long.toHexString(n.zxid),
                            Long.toHexString(n.electionEpoch));

                        // don't care about the version if it's in LOOKING state
                        // 添加投票到当前选举朝代投票集合
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                        // 如果之前接收到的投票和自己当前的投票相同（有服务器的意图和自己达成一致），
                        // 则将投票的服务器id添加到响应集合
                        voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));

                        // 所有quorum节点至少有一个投票和自己的某个提议相同并通知到自己
                        if (voteSet.hasAllQuorums()) {

                            // Verify if there is any change in the proposed leader
                            // 接收可能的新投票，检查提议是否需要改变
                            while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                // 新投票中的leader赢得这次投票，需要改变提议
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                    // 添加新投票到接收队列，继续处理
                                    recvqueue.put(n);
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             * 每次提议变更都会广播新提议，当接收队列无消息时就代表所有节点的提议都为最新的
                             */
                            // 没有接收到新投票，选举结束
                            if (n == null) {
                                // 更新自己的节点状态
                                setPeerState(proposedLeader, voteSet);
                                // 构造选举结束时的投票信息
                                Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                // 记录选举结果，清空选举队列
                                leaveInstance(endVote);
                                // 返回选举结果
                                return endVote;
                            }
                        }
                        break;
                        // 通知来自观察者，记录日志
                    case OBSERVING:
                        LOG.debug("Notification from observer: {}", n.sid);
                        break;
                        // FOLLOWING和LEADING处理相同
                    case FOLLOWING:
                    case LEADING:
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         * 通知来自于FOLLOWING和LEADING，说明此时是自己加入已有quorum，自己需要和集群达成共识
                         * 可以认为所有通知来自于相同的选举朝代（因为此时集群leader应该来自于最近一次得到有效选举，
                         * 此时除自己以外的节点是达成共识的）
                         */
                        // 通知的选举朝代和自己的朝代相同，说明自己仅落后一个朝代（可能是在其他节点选举时，自己未响应，此时发起选举）
                        if (n.electionEpoch == logicalclock.get()) {
                            // 将投票添加到当前投票集合
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            // 如果接收到的投票和之前接收到的投票相同（有服务器的意图达成一致），
                            // 则将投票的服务器id添加到响应集合
                            voteSet = getVoteTracker(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            // 所有quorum节点至少有一个投票和自己的某个提议相同并通知到自己并且选举出的leader有效
                            if (voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {
                                // 更新节点状态，如果自己是leader，保存此次选举的投票
                                setPeerState(n.leader, voteSet);
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                // 记录选举结果，清空选举队列
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }

                        /*
                         * Before joining an established ensemble, verify that
                         * a majority are following the same leader.
                         * 在加入集群前，保证大多数节点达成共识
                         * Note that the outofelection map also stores votes from the current leader election.
                         * See ZOOKEEPER-1732 for more information.
                         */
                        // 通知的选举朝代和自己的不同，记录投票信息
                        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                        // 如果接收到的投票和之前接收到的投票相同（有服务器的意图达成一致），
                        // 则将投票的服务器id添加到响应集合
                        voteSet = getVoteTracker(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

                        // 所有节点投票相同并且leader有效，说明集群有效，自己加入集群
                        if (voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                            synchronized (this) {
                                // 更新自己的朝代为选举结束的朝代
                                logicalclock.set(n.electionEpoch);
                                // 更细节点状态
                                setPeerState(n.leader, voteSet);
                            }
                            Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                            // 记录投票结果，清空接收队列
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;
                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {}(n.sid)", n.state, n.sid);
                        break;
                    }
                } else {
                    // 忽略集群外的无效投票
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
