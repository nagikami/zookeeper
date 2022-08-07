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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocket;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share
 * a good deal of code which is moved into Peer to avoid duplication.
 */
public class Learner {

    static class PacketInFlight {

        TxnHeader hdr;
        Record rec;
        TxnDigest digest;

    }

    QuorumPeer self;
    LearnerZooKeeperServer zk;

    protected BufferedOutputStream bufferedOutput;

    protected Socket sock;
    protected MultipleAddresses leaderAddr;
    protected AtomicBoolean sockBeingClosed = new AtomicBoolean(false);

    /**
     * Socket getter
     */
    public Socket getSocket() {
        return sock;
    }

    LearnerSender sender = null;
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;

    private static final int BUFFERED_MESSAGE_SIZE = 10;
    protected final MessageTracker messageTracker = new MessageTracker(BUFFERED_MESSAGE_SIZE);

    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    /**
     * Time to wait after connection attempt with the Leader or LearnerMaster before this
     * Learner tries to connect again.
     */
    private static final int leaderConnectDelayDuringRetryMs = Integer.getInteger("zookeeper.leaderConnectDelayDuringRetryMs", 100);

    private static final boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");

    public static final String LEARNER_ASYNC_SENDING = "zookeeper.learner.asyncSending";
    private static boolean asyncSending =
        Boolean.parseBoolean(ConfigUtils.getPropertyBackwardCompatibleWay(LEARNER_ASYNC_SENDING));
    public static final String LEARNER_CLOSE_SOCKET_ASYNC = "zookeeper.learner.closeSocketAsync";
    public static final boolean closeSocketAsync = Boolean
        .parseBoolean(ConfigUtils.getPropertyBackwardCompatibleWay(LEARNER_CLOSE_SOCKET_ASYNC));

    static {
        LOG.info("leaderConnectDelayDuringRetryMs: {}", leaderConnectDelayDuringRetryMs);
        LOG.info("TCP NoDelay set to: {}", nodelay);
        LOG.info("{} = {}", LEARNER_ASYNC_SENDING, asyncSending);
        LOG.info("{} = {}", LEARNER_CLOSE_SOCKET_ASYNC, closeSocketAsync);
    }

    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations = new ConcurrentHashMap<Long, ServerCnxn>();

    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }

    // for testing
    protected static void setAsyncSending(boolean newMode) {
        asyncSending = newMode;
        LOG.info("{} = {}", LEARNER_ASYNC_SENDING, asyncSending);

    }
    protected static boolean getAsyncSending() {
        return asyncSending;
    }
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout) throws IOException {
        LOG.info("Revalidating client: 0x{}", Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos.toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "To validate session 0x" + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }

    /**
     * write a packet to the leader.
     *
     * This method is called by multiple threads. We need to make sure that only one thread is writing to leaderOs at a time.
     * When packets are sent synchronously, writing is done within a synchronization block.
     * When packets are sent asynchronously, sender.queuePacket() is called, which writes to a BlockingQueue, which is thread-safe.
     * Reading from this BlockingQueue and writing to leaderOs is the learner sender thread only.
     * So we have only one thread writing to leaderOs at a time in either case.
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        if (asyncSending) {
            sender.queuePacket(pp);
        } else {
            writePacketNow(pp, flush);
        }
    }

    void writePacketNow(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                // 向消息追踪缓冲区写入数据包类型
                messageTracker.trackSent(pp.getType());
                // 写入数据包数据到包装的输出流
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                // 将缓冲数据写入包装的输出流
                bufferedOutput.flush();
            }
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the leader
     */
    protected void startSendingThread() {
        sender = new LearnerSender(this);
        sender.start();
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            // 从leader的响应读取一个数据包
            leaderIs.readRecord(pp, "packet");
            // 记录读取的类型和时间
            messageTracker.trackReceived(pp.getType());
        }
        if (LOG.isTraceEnabled()) {
            final long traceMask =
                (pp.getType() == Leader.PING) ? ZooTrace.SERVER_PING_TRACE_MASK
                    : ZooTrace.SERVER_PACKET_TRACE_MASK;

            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }

    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        if (request.isThrottled()) {
            LOG.error("Throttled request sent to leader: {}. Exiting", request);
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte[] b = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos.toByteArray(), request.authInfo);
        writePacket(qp, true);
    }

    /**
     * Returns the address of the node we think is the leader.
     * 找到leader，并处理DNS
     */
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect. 处理DNS
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = {}", current.getId());
        }
        return leaderServer;
    }

    /**
     * Overridable helper method to return the System.nanoTime().
     * This method behaves identical to System.nanoTime().
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Overridable helper method to simply call sock.connect(). This can be
     * overriden in tests to fake connection success/failure for connectToLeader.
     */
    protected void sockConnect(Socket sock, InetSocketAddress addr, int timeout) throws IOException {
        sock.connect(addr, timeout);
    }

    /**
     * Establish a connection with the LearnerMaster found by findLearnerMaster.
     * Followers only connect to Leaders, Observers can connect to any active LearnerMaster.
     * Retries until either initLimit time has elapsed or 5 tries have happened.
     * @param multiAddr - the address of the Peer to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * if there is an authentication failure while connecting to leader
     */
    protected void connectToLeader(MultipleAddresses multiAddr, String hostname) throws IOException {

        this.leaderAddr = multiAddr;
        Set<InetSocketAddress> addresses;
        if (self.isMultiAddressReachabilityCheckEnabled()) {
            // even if none of the addresses are reachable, we want to try to establish connection
            // see ZOOKEEPER-3758
            addresses = multiAddr.getAllReachableAddressesOrAll();
        } else {
            addresses = multiAddr.getAllAddresses();
        }
        ExecutorService executor = Executors.newFixedThreadPool(addresses.size());
        CountDownLatch latch = new CountDownLatch(addresses.size());
        AtomicReference<Socket> socket = new AtomicReference<>(null);
        // 异步创建连接，只会有一个连接创建成功（CAS + volatile）
        addresses.stream().map(address -> new LeaderConnector(address, socket, latch)).forEach(executor::submit);

        try {
            // 等待连接创建完
            latch.await();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while trying to connect to Leader", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOG.error("not all the LeaderConnector terminated properly");
                }
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while terminating LeaderConnector executor.", ie);
            }
        }

        if (socket.get() == null) {
            throw new IOException("Failed connect to " + multiAddr);
        } else {
            sock = socket.get();
            sockBeingClosed.set(false);
        }

        self.authLearner.authenticate(sock, hostname);

        // 包装来自leader的输入流
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        // 包装发往leader的输入流
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
        if (asyncSending) {
            startSendingThread();
        }
    }

    class LeaderConnector implements Runnable {

        private AtomicReference<Socket> socket;
        private InetSocketAddress address;
        private CountDownLatch latch;

        LeaderConnector(InetSocketAddress address, AtomicReference<Socket> socket, CountDownLatch latch) {
            this.address = address;
            this.socket = socket;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Thread.currentThread().setName("LeaderConnector-" + address);
                // 和leader建立连接
                Socket sock = connectToLeader();

                if (sock != null && sock.isConnected()) {
                    // 更新socket（存在多ip时会和其他线程竞争）
                    if (socket.compareAndSet(null, sock)) {
                        LOG.info("Successfully connected to leader, using address: {}", address);
                    } else {
                        LOG.info("Connection to the leader is already established, close the redundant connection");
                        sock.close();
                    }
                }

            } catch (Exception e) {
                LOG.error("Failed connect to {}", address, e);
            } finally {
                latch.countDown();
            }
        }

        private Socket connectToLeader() throws IOException, X509Exception, InterruptedException {
            // 创建socket设置默认超时时间
            Socket sock = createSocket();

            // leader connection timeout defaults to tickTime * initLimit
            int connectTimeout = self.tickTime * self.initLimit;

            // but if connectToLearnerMasterLimit is specified, use that value to calculate
            // timeout instead of using the initLimit value
            if (self.connectToLearnerMasterLimit > 0) {
                connectTimeout = self.tickTime * self.connectToLearnerMasterLimit;
            }

            int remainingTimeout;
            long startNanoTime = nanoTime();

            for (int tries = 0; tries < 5 && socket.get() == null; tries++) {
                try {
                    // recalculate the init limit time because retries sleep for 1000 milliseconds
                    remainingTimeout = connectTimeout - (int) ((nanoTime() - startNanoTime) / 1_000_000);
                    if (remainingTimeout <= 0) {
                        LOG.error("connectToLeader exceeded on retries.");
                        throw new IOException("connectToLeader exceeded on retries.");
                    }

                    // 建立连接
                    sockConnect(sock, address, Math.min(connectTimeout, remainingTimeout));
                    if (self.isSslQuorum()) {
                        ((SSLSocket) sock).startHandshake();
                    }
                    // 设置TCP_NODELAY选项
                    sock.setTcpNoDelay(nodelay);
                    break;
                } catch (IOException e) {
                    remainingTimeout = connectTimeout - (int) ((nanoTime() - startNanoTime) / 1_000_000);

                    if (remainingTimeout <= leaderConnectDelayDuringRetryMs) {
                        LOG.error(
                          "Unexpected exception, connectToLeader exceeded. tries={}, remaining init limit={}, connecting to {}",
                          tries,
                          remainingTimeout,
                          address,
                          e);
                        throw e;
                    } else if (tries >= 4) {
                        LOG.error(
                          "Unexpected exception, retries exceeded. tries={}, remaining init limit={}, connecting to {}",
                          tries,
                          remainingTimeout,
                          address,
                          e);
                        throw e;
                    } else {
                        LOG.warn(
                          "Unexpected exception, tries={}, remaining init limit={}, connecting to {}",
                          tries,
                          remainingTimeout,
                          address,
                          e);
                        sock = createSocket();
                    }
                }
                Thread.sleep(leaderConnectDelayDuringRetryMs);
            }

            return sock;
        }
    }

    /**
     * Creating a simple or and SSL socket.
     * This can be overridden in tests to fake already connected sockets for connectToLeader.
     */
    protected Socket createSocket() throws X509Exception, IOException {
        Socket sock;
        if (self.isSslQuorum()) {
            sock = self.getX509Util().createSSLSocket();
        } else {
            sock = new Socket();
        }
        // read()阻塞时间（空闲超时）
        sock.setSoTimeout(self.tickTime * self.initLimit);
        return sock;
    }

    /**
     * Once connected to the leader or learner master, perform the handshake
     * protocol to establish a following / observing connection.
     * 和新leader完成三次握手（FOLLOWERINFO，LEADERINFO，ACKEPOCH），将自己的事务id和旧朝代变更次数发送给新leader，并获取新leader朝代
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException {
        /*
         * Send follower info, including last zxid and sid
         */
        long lastLoggedZxid = self.getLastLoggedZxid();
        // 创建quorum数据包记录，通过指定包数据的编码格式，避免TCP分包的粘包问题
        QuorumPacket qp = new QuorumPacket();
        // 设置消息类型FOLLOWERINFO
        qp.setType(pktType);
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));

        /*
         * Add sid to payload
         * 构造learner信息
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000, self.getQuorumVerifier().getVersion());
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        // 向字节数组写入learner信息
        boa.writeRecord(li, "LearnerInfo");
        // 设置数据包数据为从字节数组读取的learner信息
        qp.setData(bsid.toByteArray());

        // 发送数据包到leader
        writePacket(qp, true);
        // 从新leader读取LEADERINFO消息
        readPacket(qp);
        // 获取新leader的朝代（任期）事务id
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
        // 消息类型为LEADERINFO（17，代表follower第一次从新leader接收的消息，包含协议版本和leader新朝代的事务id）
        if (qp.getType() == Leader.LEADERINFO) {
            // we are connected to a 1.0 server so accept the new epoch and read the next packet
            // 读取协议版本号
            leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
            byte[] epochBytes = new byte[4];
            final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
            // 新leader的朝代大于当前保存的旧leader的朝代
            if (newEpoch > self.getAcceptedEpoch()) {
                // 将自己的当前（旧）朝代的变更次数写入缓冲
                wrappedEpochBytes.putInt((int) self.getCurrentEpoch());
                // 保存新leader朝代
                self.setAcceptedEpoch(newEpoch);
                // 新leader朝代等于当前保存的旧leader的朝代
            } else if (newEpoch == self.getAcceptedEpoch()) {
                // since we have already acked an epoch equal to the leaders, we cannot ack
                // again, but we still need to send our lastZxid to the leader so that we can
                // sync with it if it does assume leadership of the epoch.
                // the -1 indicates that this reply should not count as an ack for the new epoch
                // 已经响应过一次朝代和此leader相同的leader，无需再响应（-1表示不算响应计数），只需要发送zxid
                wrappedEpochBytes.putInt(-1);
            } else {
                throw new IOException("Leaders epoch, "
                                      + newEpoch
                                      + " is less than accepted epoch, "
                                      + self.getAcceptedEpoch());
            }
            // 构造响应数据包
            QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
            // 向leader发送消息，类型为ACKEPOCH
            writePacket(ackNewEpoch, true);
            // 返回zixd（朝代为新朝代，数据变更次数为0）
            return ZxidUtils.makeZxid(newEpoch, 0);
            // 其他消息类型
        } else {
            // 从leader读取的朝代大于当前保存的来自leader的朝代
            if (newEpoch > self.getAcceptedEpoch()) {
                // 保存从leader读取到的朝代
                self.setAcceptedEpoch(newEpoch);
            }
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            // 返回从leader读取的事务id
            return qp.getZxid();
        }
    }

    /**
     * Finally, synchronize our history with the Leader (if Follower)
     * or the LearnerMaster (if Observer).
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    protected void syncWithLeader(long newLeaderZxid) throws Exception {
        // 构建响应消息，类型为ACK
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        // 获取从leader读取的朝代
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);

        QuorumVerifier newLeaderQV = null;

        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        // DIFF不需要创建snapshot，因为同步到的状态会比任何快照都新，SNAP和TRUNC则需要创建snapshot保存当前状态
        // 是否创建快照
        boolean snapshotNeeded = true;
        // 是否立刻从page cache刷新到硬盘
        boolean syncSnapshot = false;
        // 从leader数据同步消息
        readPacket(qp);
        Deque<Long> packetsCommitted = new ArrayDeque<>();
        Deque<PacketInFlight> packetsNotCommitted = new ArrayDeque<>();
        synchronized (zk) {
            // 同步类型为DIFF
            if (qp.getType() == Leader.DIFF) {
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                // 设置同步类型为DIFF
                self.setSyncMode(QuorumPeer.SyncMode.DIFF);
                // 开启选举后强制创建snapshot
                if (zk.shouldForceWriteInitialSnapshotAfterLeaderElection()) {
                    LOG.info("Forcing a snapshot write as part of upgrading from an older Zookeeper. This should only happen while upgrading.");
                    snapshotNeeded = true;
                    syncSnapshot = true;
                } else {
                    snapshotNeeded = false;
                }
                // 同步类型为SNAP
            } else if (qp.getType() == Leader.SNAP) {
                // 设置同步类型为SNAP
                self.setSyncMode(QuorumPeer.SyncMode.SNAP);
                LOG.info("Getting a snapshot from leader 0x{}", Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // db is clear as part of deserializeSnapshot()
                // 加载从leader读取的snapshot
                zk.getZKDatabase().deserializeSnapshot(leaderIs);
                // ZOOKEEPER-2819: overwrite config node content extracted
                // from leader snapshot with local config, to avoid potential
                // inconsistency of config node content during rolling restart.
                // 使用本地节点配置覆盖snapshot的节点配置
                if (!self.isReconfigEnabled()) {
                    LOG.debug("Reset config node content from local config after deserialization of snapshot.");
                    zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
                }
                // 验证签名
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got {}", signature);
                    throw new IOException("Missing signature");
                }
                // 设置自己的zxid为从leader读取的snapshot的zxid
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

                // immediately persist the latest snapshot when there is txn log gap
                syncSnapshot = true;
                // 同步类型为TRUNC，截断zxid之后的日志
            } else if (qp.getType() == Leader.TRUNC) {
                //we need to truncate the log to the lastzxid of the leader
                self.setSyncMode(QuorumPeer.SyncMode.TRUNC);
                LOG.warn("Truncating log to get in sync with the leader 0x{}", Long.toHexString(qp.getZxid()));
                // 截断zxid之后的日志
                boolean truncated = zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log 0x{}", Long.toHexString(qp.getZxid()));
                    ServiceUtils.requestSystemExit(ExitCode.QUORUM_PACKET_ERROR.getValue());
                }
                // 设置自己的zxid为从leader读取的zxid
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

            } else {
                LOG.error("Got unexpected packet from leader: {}, exiting ... ", LearnerHandler.packetToString(qp));
                ServiceUtils.requestSystemExit(ExitCode.QUORUM_PACKET_ERROR.getValue());
            }
            // 初始化数据库配置
            zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
            zk.createSessionTracker();

            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;
            TxnLogEntry logEntry;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) {
                // 从leader读取数据包
                readPacket(qp);
                switch (qp.getType()) {
                    // 消息类型为提议（接收数据）
                case Leader.PROPOSAL:
                    PacketInFlight pif = new PacketInFlight();
                    // 读取接收到的日志节点
                    logEntry = SerializeUtils.deserializeTxn(qp.getData());
                    pif.hdr = logEntry.getHeader();
                    pif.rec = logEntry.getTxn();
                    pif.digest = logEntry.getDigest();
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                        LOG.warn(
                            "Got zxid 0x{} expected 0x{}",
                            Long.toHexString(pif.hdr.getZxid()),
                            Long.toHexString(lastQueued + 1));
                    }
                    // 记录读取到的zxid
                    lastQueued = pif.hdr.getZxid();

                    if (pif.hdr.getType() == OpCode.reconfig) {
                        SetDataTxn setDataTxn = (SetDataTxn) pif.rec;
                        QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData(), UTF_8));
                        self.setLastSeenQuorumVerifier(qv, true);
                    }

                    // 添加日志节点到未提交队列
                    packetsNotCommitted.add(pif);
                    break;
                    // 消息类型为提交（处理接收到的数据）
                case Leader.COMMIT:
                case Leader.COMMITANDACTIVATE:
                    // 返回队列里的第一个条消息，但不删除
                    pif = packetsNotCommitted.peekFirst();
                    // 当前读取到的事务id等于第一条消息（如果是配置则只会缓存一条消息）的事务id，且消息类型为提交并激活
                    if (pif.hdr.getZxid() == qp.getZxid() && qp.getType() == Leader.COMMITANDACTIVATE) {
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) pif.rec).getData(), UTF_8));
                        // 根据接收到的配置信息修改配置
                        boolean majorChange = self.processReconfig(
                            qv,
                            ByteBuffer.wrap(qp.getData()).getLong(), qp.getZxid(),
                            true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    }
                    // snapshotNeeded为true
                    if (!writeToTxnLog) {
                        // 缓存的第一条消息的事务id不等于当前消息的事务id
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn(
                                "Committing 0x{}, but next proposal is 0x{}",
                                Long.toHexString(qp.getZxid()),
                                Long.toHexString(pif.hdr.getZxid()));
                            // 缓存的第一条消息的事务id等于当前消息的事务id（commit指定的事务id）
                        } else {
                            // 处理缓存的消息（日志）
                            zk.processTxn(pif.hdr, pif.rec);
                            // 删除处理完的消息
                            packetsNotCommitted.remove();
                        }
                    } else {
                        // snapshotNeeded为false，直接添加消息的事务id到已提交队列
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                    // 消息类型为通知（通知观察者提交提议）
                case Leader.INFORM:
                case Leader.INFORMANDACTIVATE:
                    PacketInFlight packet = new PacketInFlight();

                    // 消息类型为通知并激活
                    if (qp.getType() == Leader.INFORMANDACTIVATE) {
                        ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                        // 读取leader id
                        long suggestedLeaderId = buffer.getLong();
                        byte[] remainingdata = new byte[buffer.remaining()];
                        buffer.get(remainingdata);
                        // 读取日志节点
                        logEntry = SerializeUtils.deserializeTxn(remainingdata);
                        packet.hdr = logEntry.getHeader();
                        packet.rec = logEntry.getTxn();
                        packet.digest = logEntry.getDigest();
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) packet.rec).getData(), UTF_8));
                        boolean majorChange = self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                        // 消息类型为通知
                    } else {
                        // 读取日志节点
                        logEntry = SerializeUtils.deserializeTxn(qp.getData());
                        packet.rec = logEntry.getTxn();
                        packet.hdr = logEntry.getHeader();
                        packet.digest = logEntry.getDigest();
                        // Log warning message if txn comes out-of-order
                        if (packet.hdr.getZxid() != lastQueued + 1) {
                            LOG.warn(
                                "Got zxid 0x{} expected 0x{}",
                                Long.toHexString(packet.hdr.getZxid()),
                                Long.toHexString(lastQueued + 1));
                        }
                        // 更新记录的事务id
                        lastQueued = packet.hdr.getZxid();
                    }
                    // snapshotNeeded为true
                    if (!writeToTxnLog) {
                        // Apply to db directly if we haven't taken the snapshot
                        // 直接加载日志到数据库
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        // 添加消息到未提交队列
                        packetsNotCommitted.add(packet);
                        // 添加事务id到已提交队列
                        packetsCommitted.add(qp.getZxid());
                    }

                    break;
                    // 消息类型为UPTODATE，表示半数以上follower的数据已经为最新版本，且leader服务初始化完成
                case Leader.UPTODATE:
                    LOG.info("Learner received UPTODATE message");
                    if (newLeaderQV != null) {
                        boolean majorChange = self.processReconfig(newLeaderQV, null, null, true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    }
                    // zab版本为1.0
                    if (isPreZAB1_0) {
                        // 创建快照
                        zk.takeSnapshot(syncSnapshot);
                        // 设置新朝代
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.setZooKeeperServer(zk);
                    self.adminServer.setZooKeeperServer(zk);
                    // 跳出到外循环
                    break outerLoop;
                    // 消息类型为NEWLEADER，接收leader的zxid
                case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery
                    // means this is Zab 1.0
                    LOG.info("Learner received NEWLEADER message");
                    if (qp.getData() != null && qp.getData().length > 1) {
                        try {
                            // 更新quorum配置
                            QuorumVerifier qv = self.configFromString(new String(qp.getData(), UTF_8));
                            self.setLastSeenQuorumVerifier(qv, true);
                            newLeaderQV = qv;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    // 需要创建snapshot
                    if (snapshotNeeded) {
                        // 创建快照
                        zk.takeSnapshot(syncSnapshot);
                    }

                    // 设置新朝代
                    self.setCurrentEpoch(newEpoch);
                    writeToTxnLog = true;
                    //Anything after this needs to go to the transaction log, not applied directly in memory
                    isPreZAB1_0 = false;

                    // ZOOKEEPER-3911: make sure sync the uncommitted logs before commit them (ACK NEWLEADER).
                    sock.setSoTimeout(self.tickTime * self.syncLimit);
                    self.setSyncMode(QuorumPeer.SyncMode.NONE);
                    // 初始化服务
                    zk.startupWithoutServing();
                    if (zk instanceof FollowerZooKeeperServer) {
                        FollowerZooKeeperServer fzk = (FollowerZooKeeperServer) zk;
                        // 处理leader发送的未提交日志
                        for (PacketInFlight p : packetsNotCommitted) {
                            fzk.logRequest(p.hdr, p.rec, p.digest);
                        }
                        packetsNotCommitted.clear();
                    }

                    // 向leader发送对NEWLEADER的响应消息，类型为ACK
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        // 数据同步完成
        // 设置zxid为新leader朝代
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        // 向leader发送响应，类型为ACK
        writePacket(ack, true);
        // 启动服务
        zk.startServing();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         *
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         * 更新自己的投票，保证所有节点向新加入的节点发送相同的通知
         */
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        // 记录snapshot和最新状态的lag
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer) zk;
            // 将同步数据时接收的提议添加到请求处理队列，异步处理请求
            for (PacketInFlight p : packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec, p.digest);
            }
            // 完成提交
            for (Long zxid : packetsCommitted) {
                fzk.commit(zxid);
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn(
                        "Committing 0x{}, but next proposal is 0x{}",
                        Long.toHexString(zxid),
                        Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(), p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.setTxn(p.rec);
                request.setHdr(p.hdr);
                request.setTxnDigest(p.digest);
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }

    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations.remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x{} for validation", Long.toHexString(sessionId));
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "Session 0x" + Long.toHexString(sessionId) + " is valid: " + valid);
        }
    }

    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Map<Long, Integer> touchTable = zk.getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }

        QuorumPacket pingReply = new QuorumPacket(qp.getType(), qp.getZxid(), bos.toByteArray(), qp.getAuthinfo());
        writePacket(pingReply, true);
    }

    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        self.setZooKeeperServer(null);
        self.closeAllConnections();
        self.adminServer.setZooKeeperServer(null);

        if (sender != null) {
            sender.shutdown();
        }

        closeSocket();
        // shutdown previous zookeeper
        if (zk != null) {
            // If we haven't finished SNAP sync, force fully shutdown
            // to avoid potential inconsistency
            zk.shutdown(self.getSyncMode().equals(QuorumPeer.SyncMode.SNAP));
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    void closeSocket() {
        if (sock != null) {
            if (sockBeingClosed.compareAndSet(false, true)) {
                if (closeSocketAsync) {
                    final Thread closingThread = new Thread(() -> closeSockSync(), "CloseSocketThread(sid:" + zk.getServerId());
                    closingThread.setDaemon(true);
                    closingThread.start();
                } else {
                    closeSockSync();
                }
            }
        }
    }

    void closeSockSync() {
        try {
            long startTime = Time.currentElapsedTime();
            if (sock != null) {
                sock.close();
                sock = null;
            }
            ServerMetrics.getMetrics().SOCKET_CLOSING_TIME.add(Time.currentElapsedTime() - startTime);
        } catch (IOException e) {
            LOG.warn("Ignoring error closing connection to leader", e);
        }
    }

}
