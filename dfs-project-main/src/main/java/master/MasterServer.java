package master;

import common.Protocol;
import common.Utils;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MasterServer with replication worker (Master-directed PUSH) and debug logs.
 */
public class MasterServer {
    // config
    private final int port;
    private final int replicationFactor;
    private final ExecutorService pool;

    // heartbeat detection params
    private static final long MASTER_TIMEOUT_MS = 12_000L;
    private static final long MASTER_CHECK_INTERVAL_MS = 5_000L;

    // replication settings
    private static final int REPL_WORKER_COUNT = 2;
    private static final int REPLICATE_CMD_TIMEOUT_MS = 15_000;

    // metadata
    private final ConcurrentMap<String, List<String>> files = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CopyOnWriteArrayList<String>> chunkLocations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeInfo> nodes = new ConcurrentHashMap<>();

    // queues
    private final BlockingQueue<ReplicationTask> replicationTasksQueue = new LinkedBlockingQueue<>();

    public MasterServer(int port, int replicationFactor, int poolSize) {
        this.port = port;
        this.replicationFactor = replicationFactor;
        this.pool = Executors.newFixedThreadPool(poolSize);
    }

    public void start() throws IOException {
        // start failure detector
        Thread detector = new Thread(this::failureDetectorLoop);
        detector.setDaemon(true);
        detector.start();

        // start replication workers
        for (int i = 0; i < REPL_WORKER_COUNT; i++) {
            Thread w = new Thread(this::replicationWorkerLoop);
            w.setDaemon(true);
            w.start();
        }

        ServerSocket serverSocket = new ServerSocket(port);
        Utils.log("Master", "Master started on port " + port + " (replication=" + replicationFactor + ")");
        while (true) {
            Socket s = serverSocket.accept();
            pool.submit(() -> handleConnection(s));
        }
    }

    private void handleConnection(Socket s) {
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {

            String line = in.readLine();
            if (line == null) return;
            String[] parts = line.trim().split("\\s+");
            String cmd = parts[0];

            switch (cmd) {
                case Protocol.CMD_REGISTER:
                    handleRegister(parts, out);
                    break;
                case Protocol.CMD_STORE_REQUEST:
                    handleStoreRequest(parts, out);
                    break;
                case Protocol.CMD_LOCATE:
                    handleLocate(parts, out);
                    break;
                case Protocol.CMD_HEARTBEAT:
                    handleHeartbeat(parts, out);
                    break;
                case Protocol.RESP_REPL_OK:
                case Protocol.RESP_REPL_ERR:
                    Utils.log("Master", "Replication response: " + line);
                    out.write(Protocol.RESP_OK + "\n");
                    out.flush();
                    break;
                default:
                    out.write(Protocol.RESP_ERR + " unknown-cmd\n");
                    out.flush();
            }

        } catch (Exception e) {
            Utils.log("Master", "Exception in handler: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    private void handleRegister(String[] parts, BufferedWriter out) throws IOException {
        String host = parts[1];//parts = ["REGISTER", "10.0.0.1", "5001"]
        String port = parts[2];
        String nodeId = host + ":" + port;
        nodes.computeIfAbsent(nodeId, id -> new NodeInfo(host, Integer.parseInt(port)));
        nodes.get(nodeId).markAlive();
        out.write(Protocol.RESP_OK + "\n");
        out.flush();
        Utils.log("Master", "Registered node " + nodeId + " (total=" + nodes.size() + ")");
        // After this master stores all the active nodes details and total nodes
    }
    // handleStoreRequest(String[] parts, BufferedWriter out) — allocates chunkId(s), picks nodes, writes reply
    private void handleStoreRequest(String[] parts, BufferedWriter out) throws IOException {
        // STORE movie.mp4 3
        String filename = parts[1];//filename = "movie.mp4"
        int numChunks = Integer.parseInt(parts[2]);//numChunks = 3

        List<String> chunkIds = new ArrayList<>();
        for (int i = 0; i < numChunks; i++) {
            String chunkId = UUID.randomUUID().toString();
            chunkIds.add(chunkId);
            // pickNodesForChunk(int replicationFactor) — picks nodes for a chunk
            List<String> chosen = pickNodesForChunk(replicationFactor);
            chunkLocations.put(chunkId, new CopyOnWriteArrayList<>(chosen));
        }
        files.put(filename, chunkIds);
        // metadata fields: files, chunkLocations, nodes
        StringBuilder sb = new StringBuilder();
        sb.append(Protocol.RESP_STORAGENODES).append(" ").append(numChunks);
        for (String chunkId : chunkIds) {
            List<String> locs = chunkLocations.getOrDefault(chunkId, new CopyOnWriteArrayList<>());
            sb.append(" | ").append(chunkId).append(" ").append(locs.size());
            for (String nid : locs) {
                String[] hp = nid.split(":");
                sb.append(" ").append(hp[0]).append(" ").append(hp[1]);
            }
        }
        sb.append("\n");
        out.write(sb.toString());
        out.flush();
        Utils.log("Master", "STORE_REQ for '" + filename + "' -> " + numChunks + " chunks");
    }

    private void handleLocate(String[] parts, BufferedWriter out) throws IOException {
        String filename = parts[1];
        List<String> chunkIds = files.get(filename);
        if (chunkIds == null) {
            out.write(Protocol.RESP_ERR + " no-file\n");
            out.flush();
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(Protocol.RESP_STORAGENODES).append(" ").append(chunkIds.size());
        for (String chunkId : chunkIds) {
            List<String> locs = chunkLocations.getOrDefault(chunkId, new CopyOnWriteArrayList<>());
            sb.append(" | ").append(chunkId).append(" ").append(locs.size());
            for (String nid : locs) {
                String[] hp = nid.split(":");
                sb.append(" ").append(hp[0]).append(" ").append(hp[1]);
            }
        }
        sb.append("\n");
        out.write(sb.toString());
        out.flush();
        Utils.log("Master", "LOCATE for '" + filename + "' -> " + chunkIds.size() + " chunks");
    }

    private void handleHeartbeat(String[] parts, BufferedWriter out) throws IOException {
        String host = parts[1];
        String port = parts[2];
        String nodeId = host + ":" + port;
        NodeInfo info = nodes.computeIfAbsent(nodeId, id -> new NodeInfo(host, Integer.parseInt(port)));
        info.lastSeen = System.currentTimeMillis();
        info.freeSpaceBytes = Long.parseLong(parts[3]);
        info.numChunks = Integer.parseInt(parts[4]);
        info.alive = true;
        out.write(Protocol.RESP_OK + "\n");
        out.flush();
    }

    // === failure detector (with correct order + logs) ===
    private void failureDetectorLoop() {
        Utils.log("Master", "Failure detector started (timeout=" + MASTER_TIMEOUT_MS + "ms)");
        while (true) {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, NodeInfo> e : nodes.entrySet()) {
                NodeInfo n = e.getValue();
                if (n.alive && (now - n.lastSeen > MASTER_TIMEOUT_MS)) {
                    n.alive = false;
                    Utils.log("Master", "Node " + n.id() + " DEAD");
                    removeNodeFromLocations(n.id());
                    generateReplicationTasksForDeadNode(n.id());
                }
            }
            sleepMs(MASTER_CHECK_INTERVAL_MS);
        }
    }

    private void generateReplicationTasksForDeadNode(String deadNodeId) {
        int enqueued = 0;
        for (Map.Entry<String, CopyOnWriteArrayList<String>> e : chunkLocations.entrySet()) {
            String chunkId = e.getKey();
            CopyOnWriteArrayList<String> locs = e.getValue();
            int current = locs.size();
            if (current < replicationFactor) {
                ReplicationTask task = new ReplicationTask(chunkId, new ArrayList<>(locs));
                replicationTasksQueue.offer(task);
                enqueued++;
                Utils.log("Master", "Enqueued repl-task for " + chunkId + " (replicas=" + current + ")");
            }
        }
        Utils.log("Master", "generateReplicationTasksForDeadNode finished, enqueued=" + enqueued);
    }

    // === replication worker ===
    private void replicationWorkerLoop() {
        Utils.log("Master", "Replication worker started");
        while (true) {
            try {
                ReplicationTask task = replicationTasksQueue.take();
                replicateChunk(task);
            } catch (InterruptedException ignored) {}
        }
    }

    private void replicateChunk(ReplicationTask task) {
        String chunkId = task.chunkId;
        Utils.log("Master", "Processing repl-task for " + chunkId + " current=" + task.currentReplicas);
        String source = pickAliveNodeFrom(task.currentReplicas);
        String target = pickTargetNodeExcluding(task.currentReplicas);
        if (source == null || target == null) {
            Utils.log("Master", "No source/target for chunk " + chunkId);
            return;
        }
        String[] sHp = source.split(":");
        String[] tHp = target.split(":");
        boolean ok = sendReplicateCommand(sHp[0], Integer.parseInt(sHp[1]), chunkId, tHp[0], Integer.parseInt(tHp[1]));
        if (ok) {
            chunkLocations.computeIfAbsent(chunkId, k -> new CopyOnWriteArrayList<>()).add(target);
            Utils.log("Master", "Re-replicated chunk " + chunkId + " from " + source + " -> " + target);
        } else {
            Utils.log("Master", "Replication failed for chunk " + chunkId);
        }
    }

    private boolean sendReplicateCommand(String sourceHost, int sourcePort, String chunkId, String targetHost, int targetPort) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(sourceHost, sourcePort), 5000);
            s.setSoTimeout(REPLICATE_CMD_TIMEOUT_MS);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            out.write(Protocol.CMD_REPLICATE + " " + chunkId + " " + targetHost + " " + targetPort + "\n");
            out.flush();
            String resp = in.readLine();
            return resp != null && (resp.startsWith(Protocol.RESP_OK) || resp.startsWith(Protocol.RESP_REPL_OK));
        } catch (Exception e) {
            Utils.log("Master", "sendReplicateCommand error: " + e.getMessage());
            return false;
        }
    }

    // === helpers ===
    private String pickAliveNodeFrom(Collection<String> candidates) {
        List<String> live = new ArrayList<>();
        for (String id : candidates) {
            NodeInfo ni = nodes.get(id);
            if (ni != null && ni.alive) live.add(id);
        }
        if (live.isEmpty()) return null;
        Collections.shuffle(live);
        return live.get(0);
    }

    private String pickTargetNodeExcluding(Collection<String> exclude) {
        for (Map.Entry<String, NodeInfo> e : nodes.entrySet()) {
            if (e.getValue().alive && !exclude.contains(e.getKey())) return e.getKey();
        }
        return null;
    }

    private void removeNodeFromLocations(String nodeId) {
        for (Map.Entry<String, CopyOnWriteArrayList<String>> e : chunkLocations.entrySet()) {
            e.getValue().remove(nodeId);
        }
    }

    private List<String> pickNodesForChunk(int replicate) {
        List<String> liveNodes = new ArrayList<>();
        for (Map.Entry<String, NodeInfo> e : nodes.entrySet()) {
            if (e.getValue().alive) liveNodes.add(e.getKey());
        }
        Collections.shuffle(liveNodes);
        return liveNodes.subList(0, Math.min(replicate, liveNodes.size()));
    }

    private void sleepMs(long m) {
        try { Thread.sleep(m); } catch (InterruptedException ignored) {}
    }

    // inner classes
    // NodeInfo (make it public so StatusHttpServer can read it)
    public static class NodeInfo {
        public final String host;
        public final int port;
        public volatile long lastSeen;
        public volatile long freeSpaceBytes;
        public volatile int numChunks;
        public volatile boolean alive;

        public NodeInfo(String host, int port) {
            this.host = host;
            this.port = port;
            this.lastSeen = System.currentTimeMillis();
            this.freeSpaceBytes = 0;
            this.numChunks = 0;
            this.alive = true;
        }

        public void markAlive() {
            this.lastSeen = System.currentTimeMillis();
            this.alive = true;
        }

        public String id() { return host + ":" + port; }
    }


    private static class ReplicationTask {
        final String chunkId; final List<String> currentReplicas;
        ReplicationTask(String chunkId, List<String> currentReplicas) {
            this.chunkId = chunkId; this.currentReplicas = currentReplicas;
        }
    }

    // main
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java master.MasterServer <port> <replicationFactor> <threadPoolSize>");
            return;
        }
        new MasterServer(Integer.parseInt(args[0]),
                Integer.parseInt(args[1]),
                Integer.parseInt(args[2])).start();
    }
}
