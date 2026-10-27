package client;

import common.Protocol;
import common.Utils;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * DFSClient
 *
 * Usage:
 *   java client.DFSClient <masterHost> <masterPort> put <pathToFile>
 *   java client.DFSClient <masterHost> <masterPort> get <filename> <destPath>
 *
 * Behavior:
 *  - put: splits file into CHUNK_SIZE, asks Master for allocation (STORE_REQ),
 *         then streams each chunk to the assigned StorageNodes (replicas).
 *  - get: asks Master for chunk->nodes mapping (LOCATE), fetches each chunk
 *         from one available node, and reassembles the file.
 */
public class DFSClient {

    private final String masterHost;
    private final int masterPort;
    private final int CHUNK_SIZE = 256 * 1024; // 256 KB

    public DFSClient(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    /* -------------------- High level PUT -------------------- */
    public void putFile(String pathStr) throws Exception {
        Path path = Paths.get(pathStr);
        if (!Files.exists(path) || Files.isDirectory(path)) {
            throw new IllegalArgumentException("File not found: " + pathStr);
        }

        long fileSize = Files.size(path);
        int numChunks = (int) ((fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
        String filename = path.getFileName().toString();

        Utils.log("Client", "Putting file " + filename + " size=" + fileSize + " bytes, chunks=" + numChunks);

        // 1) Ask Master for allocation
        // sendLineToMaster(...) — helper that sends one-line request to Master and returns reply
        String resp = sendLineToMaster(Protocol.CMD_STORE_REQUEST + " " + filename + " " + numChunks);
        if (!resp.startsWith(Protocol.RESP_STORAGENODES)) {
            throw new RuntimeException("Bad master response: " + resp);
        }

        // parse response: format: NODES <numChunks> | chunkId nodeCount host port ...
        // parseMasterNodesResponse(String resp) — builds ChunkPlans (chunkId + node list)
        List<ChunkPlan> plans = parseMasterNodesResponse(resp);

        // 2) Stream file and upload chunk-by-chunk
        try (InputStream fis = Files.newInputStream(path)) {
            byte[] buf = new byte[CHUNK_SIZE];
            for (int i = 0; i < plans.size(); i++) {
                ChunkPlan plan = plans.get(i);
                // Robust chunk read: read up to CHUNK_SIZE bytes into a ByteArrayOutputStream
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int remaining = CHUNK_SIZE;
                while (remaining > 0) {
                    int r = fis.read(buf, 0, Math.min(buf.length, remaining));
                    if (r == -1) break; // EOF
                    baos.write(buf, 0, r);
                    remaining -= r;
                }
                byte[] chunkBytes = baos.toByteArray();

                Utils.log("Client", "Uploading chunk " + plan.chunkId + " size=" + chunkBytes.length + " to " + plan.nodes.size() + " nodes");

                // send chunk to each assigned node (replication)
                //sendChunkToNode(InetSocketAddress node, String chunkId, byte[] bytes) — sends header + raw bytes to a storage node
                for (InetSocketAddress node : plan.nodes) {
                    sendChunkToNode(node, plan.chunkId, chunkBytes);
                }
            }
        }

        Utils.log("Client", "PUT complete for " + filename);
    }

    /* -------------------- High level GET -------------------- */
    public void getFile(String filename, String destPath) throws Exception {
        Utils.log("Client", "Getting file " + filename);

        String resp = sendLineToMaster("LOCATE " + filename);
        if (!resp.startsWith(Protocol.RESP_STORAGENODES)) {
            throw new RuntimeException("Master locate failed: " + resp);
        }
        List<ChunkPlan> plans = parseMasterNodesResponse(resp);

        Path output = Paths.get(destPath);
        Files.createDirectories(output.getParent() == null ? Paths.get(".") : output.getParent());

        try (OutputStream fos = Files.newOutputStream(output, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (ChunkPlan plan : plans) {
                // pick the first node for simplicity (could try others on failure)
                boolean fetched = false;
                Exception lastEx = null;
                for (InetSocketAddress node : plan.nodes) {
                    try {
                        byte[] chunk = fetchChunkFromNode(node, plan.chunkId);
                        fos.write(chunk);
                        fetched = true;
                        break;
                    } catch (Exception e) {
                        lastEx = e;
                        Utils.log("Client", "Failed fetch from " + node + " for " + plan.chunkId + " -> " + e.getMessage());
                    }
                }
                if (!fetched) {
                    throw new RuntimeException("Failed to fetch chunk " + plan.chunkId + ", lastEx=" + lastEx);
                }
            }
        }

        Utils.log("Client", "GET complete, saved to " + destPath);
    }

    /* -------------------- Helpers -------------------- */

    // Simple struct to hold chunk mapping
    private static class ChunkPlan {
        String chunkId;
        List<InetSocketAddress> nodes;
    }

    // Parse the Master's NODES response into a list of ChunkPlan ordered by file chunk order
    private List<ChunkPlan> parseMasterNodesResponse(String resp){
        // resp expected like: NODES <numChunks> | chunkId nodeCount host port host port | chunk2 ...
        // Ex:- NODES 3 | c1 2 10.0.0.1 5001 10.0.0.2 5002 | c2 2 10.0.0.3 5003 10.0.0.4 5004 | c3 1 10.0.0.5 5005
        String[] pipeParts = resp.split("\\|");
        List<ChunkPlan> plans = new ArrayList<>();
        for (int i = 1; i < pipeParts.length; i++) {
            String part = pipeParts[i].trim();
            if (part.isEmpty()) continue;
            String[] tok = part.split("\\s+");
            ChunkPlan cp = new ChunkPlan();

            cp.chunkId = tok[0];
            int nodeCount = Integer.parseInt(tok[1]);
            cp.nodes = new ArrayList<>();
            int idx = 2;
            for (int n = 0; n < nodeCount; n++) {
                String host = tok[idx++];
                int port = Integer.parseInt(tok[idx++]);
                cp.nodes.add(new InetSocketAddress(host, port));
            }
            plans.add(cp);
        }
        return plans;
    }

    // Ask master for a single-line response
    private String sendLineToMaster(String line) throws IOException {
        try (Socket s = new Socket(masterHost, masterPort);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
            out.write(line + "\n");
            out.flush();
            return in.readLine();
        }
    }

    // Send chunk bytes to a storage node (STORECHUNK)
    private void sendChunkToNode(InetSocketAddress node, String chunkId, byte[] bytes) throws IOException {
        try (Socket s = new Socket(node.getHostString(), node.getPort());
             OutputStream outRaw = s.getOutputStream();
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outRaw, StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {

            out.write(Protocol.CMD_STORE_CHUNK + " " + chunkId + " " + bytes.length + "\n");
            out.flush();
            // Sending the Actual Chunk Data
            outRaw.write(bytes);
            outRaw.flush();

            String resp = in.readLine();
            if (resp == null || !resp.startsWith(Protocol.RESP_OK)) {
                throw new IOException("Node " + node + " reply=" + resp);
            }
        }
    }

    // Fetch chunk bytes from a storage node (FETCH)
    private byte[] fetchChunkFromNode(InetSocketAddress node, String chunkId) throws IOException {
        try (Socket s = new Socket(node.getHostString(), node.getPort());
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
             InputStream inRaw = s.getInputStream();
             BufferedReader in = new BufferedReader(new InputStreamReader(inRaw, StandardCharsets.UTF_8))) {

            out.write(Protocol.CMD_FETCH_CHUNK + " " + chunkId + "\n");
            out.flush();
            String header = in.readLine();
            if (header == null) throw new IOException("No header from node");
            if (header.startsWith(Protocol.RESP_ERR)) throw new IOException("Node error: " + header);
            // header: OK <size>
            String[] parts = header.split("\\s+");
            int size = Integer.parseInt(parts[1]);
            byte[] buf = new byte[size];
            int read = 0;
            while (read < size) {
                int r = inRaw.read(buf, read, size - read);
                if (r < 0) throw new EOFException("Short read");
                read += r;
            }
            return buf;
        }
    }

    /* -------------------- Main -------------------- */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage:");
            System.out.println("  java client.DFSClient <masterHost> <masterPort> put <pathToFile>");
            System.out.println("  java client.DFSClient <masterHost> <masterPort> get <filename> <destPath>");
            return;
        }
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        String cmd = args[2];

        DFSClient client = new DFSClient(masterHost, masterPort);
        if (cmd.equals("put")) {
            client.putFile(args[3]);
        } else if (cmd.equals("get")) {
            if (args.length < 5) {
                System.out.println("get usage: get <filename> <destPath>");
                return;
            }
            client.getFile(args[3], args[4]);
        } else {
            System.out.println("Unknown command: " + cmd);
        }
    }
}
