package storagenode;

import common.Protocol;
import common.Utils;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/**
 * StorageNode with REPLICATE support (acts as source to push chunk to target).
 */
public class StorageNode {
    private final String masterHost;
    private final int masterPort;
    private final int servePort;
    private final Path storageDir;
    private final ExecutorService pool;

    private static final long HEARTBEAT_INTERVAL_MS = 3_000L;
    private volatile boolean running = true;

    public StorageNode(String masterHost, int masterPort, int servePort, String storageDir) throws IOException {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.servePort = servePort;
        this.storageDir = Paths.get(storageDir);
        Files.createDirectories(this.storageDir);
        this.pool = Executors.newFixedThreadPool(8);
    }

    public void start() throws IOException {
        registerToMaster();

        Thread hb = new Thread(this::heartbeatLoop);
        hb.setDaemon(true);
        hb.start();

        ServerSocket serverSocket = new ServerSocket(servePort);
        Utils.log("StorageNode", "Listening on port " + servePort + ", storing at " + storageDir.toAbsolutePath());

        while (running) {
            Socket s = serverSocket.accept();
            pool.submit(() -> handleConnection(s));
        }
    }

    private void registerToMaster() {
        try (Socket sock = new Socket(masterHost, masterPort);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8))) {

            String host = InetAddress.getLocalHost().getHostAddress();
            out.write(Protocol.CMD_REGISTER + " " + host + " " + servePort + "\n");
            out.flush();

            String resp = in.readLine();
            Utils.log("StorageNode", "Master replied to REGISTER: " + resp);
        } catch (Exception e) {
            Utils.log("StorageNode", "Failed to register to master: " + e.getMessage());
        }
    }

    private void heartbeatLoop() {
        while (running) {
            try {
                long free = computeFreeSpace();
                int chunks = countChunks();
                String host = InetAddress.getLocalHost().getHostAddress();
                String msg = Protocol.CMD_HEARTBEAT + " " + host + " " + servePort + " " + free + " " + chunks;
                try (Socket s = new Socket(masterHost, masterPort);
                     BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {

                    out.write(msg + "\n");
                    out.flush();
                    String resp = in.readLine();
                    Utils.log("StorageNode", "HEARTBEAT sent; master replied: " + resp);
                } catch (Exception ex) {
                    Utils.log("StorageNode", "Heartbeat failed: " + ex.getMessage());
                }
            } catch (Exception e) {
                Utils.log("StorageNode", "Heartbeat loop exception: " + e.getMessage());
            }

            try {
                Thread.sleep(HEARTBEAT_INTERVAL_MS);
            } catch (InterruptedException ignored) {}
        }
    }

    private long computeFreeSpace() {
        try {
            FileStore fs = Files.getFileStore(storageDir);
            return fs.getUsableSpace();
        } catch (IOException e) {
            return 0L;
        }
    }

    private int countChunks() {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(storageDir)) {
            int c = 0;
            for (Path p : ds) c++;
            return c;
        } catch (IOException e) {
            return 0;
        }
    }

    private void handleConnection(Socket s) {
        String remote = s.getRemoteSocketAddress() == null ? "unknown" : s.getRemoteSocketAddress().toString();
        Utils.log("StorageNode", "Accepted connection from " + remote);
        try (InputStream rawIn = s.getInputStream();
             OutputStream rawOut = s.getOutputStream()) {

            String header = readLineFromStream(rawIn);
            if (header == null) {
                Utils.log("StorageNode", "Empty header from " + remote);
                return;
            }
            String[] parts = header.trim().split("\\s+");
            String cmd = parts[0];

            if (cmd.equals(Protocol.CMD_STORE_CHUNK)) {
                if (parts.length < 3) {
                    writeLine(rawOut, Protocol.RESP_ERR + " bad-storechunk");
                    return;
                }
                String chunkId = parts[1];
                int size = Integer.parseInt(parts[2]);
                Path outPath = storageDir.resolve(chunkId);
                Utils.log("StorageNode", "STORECHUNK " + chunkId + " size=" + size);

                try (OutputStream fos = Files.newOutputStream(outPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    byte[] buf = new byte[8192];
                    int remaining = size;
                    while (remaining > 0) {
                        int toRead = Math.min(buf.length, remaining);
                        int r = rawIn.read(buf, 0, toRead);
                        if (r < 0) throw new EOFException("Unexpected EOF while storing chunk");
                        fos.write(buf, 0, r);
                        remaining -= r;
                    }
                }

                writeLine(rawOut, Protocol.RESP_OK);
                Utils.log("StorageNode", "Stored chunk " + chunkId + " size=" + Files.size(outPath));
            } else if (cmd.equals(Protocol.CMD_FETCH_CHUNK)) {
                if (parts.length < 2) {
                    writeLine(rawOut, Protocol.RESP_ERR + " bad-fetch");
                    return;
                }
                String chunkId = parts[1];
                Path p = storageDir.resolve(chunkId);
                if (!Files.exists(p)) {
                    writeLine(rawOut, Protocol.RESP_ERR + " no-chunk");
                    Utils.log("StorageNode", "FETCH missing chunk " + chunkId);
                    return;
                }
                long size = Files.size(p);
                writeLine(rawOut, Protocol.RESP_OK + " " + size);

                try (InputStream fis = Files.newInputStream(p)) {
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = fis.read(buf)) != -1) {
                        rawOut.write(buf, 0, r);
                    }
                    rawOut.flush();
                }
                Utils.log("StorageNode", "Served FETCH " + chunkId + " size=" + size);
            } else if (cmd.equals(Protocol.CMD_REPLICATE)) {
                // REPLICATE <chunkId> <targetHost> <targetPort>
                if (parts.length < 4) {
                    writeLine(rawOut, Protocol.RESP_ERR + " bad-replicate");
                    return;
                }
                String chunkId = parts[1];
                String targetHost = parts[2];
                int targetPort = Integer.parseInt(parts[3]);
                Utils.log("StorageNode", "REPLICATE request: " + chunkId + " -> " + targetHost + ":" + targetPort);
                Path src = storageDir.resolve(chunkId);
                if (!Files.exists(src)) {
                    writeLine(rawOut, Protocol.RESP_REPL_ERR + " " + chunkId + " no-chunk");
                    Utils.log("StorageNode", "REPLICATE failed: chunk not found " + chunkId);
                    return;
                }

                boolean pushed = pushChunkToTarget(src, chunkId, targetHost, targetPort);
                if (pushed) {
                    writeLine(rawOut, Protocol.RESP_REPL_OK + " " + chunkId);
                    Utils.log("StorageNode", "REPLICATE succeeded for " + chunkId + " -> " + targetHost + ":" + targetPort);
                } else {
                    writeLine(rawOut, Protocol.RESP_REPL_ERR + " " + chunkId + " push-failed");
                    Utils.log("StorageNode", "REPLICATE push failed for " + chunkId);
                }

            } else {
                writeLine(rawOut, Protocol.RESP_ERR + " unknown-cmd");
            }

        } catch (Exception e) {
            Utils.log("StorageNode", "Exception handling connection: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    // push chunk file to target storage node using STORECHUNK protocol
    private boolean pushChunkToTarget(Path srcPath, String chunkId, String targetHost, int targetPort) {
        try (Socket t = new Socket()) {
            t.connect(new InetSocketAddress(targetHost, targetPort), 5000);
            t.setSoTimeout(15000);
            try (OutputStream out = t.getOutputStream();
                 BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                 InputStream inTarget = t.getInputStream();
                 BufferedReader bin = new BufferedReader(new InputStreamReader(inTarget, StandardCharsets.UTF_8))) {

                long size = Files.size(srcPath);
                bout.write(Protocol.CMD_STORE_CHUNK + " " + chunkId + " " + size + "\n");
                bout.flush();

                // stream bytes
                try (InputStream fis = Files.newInputStream(srcPath)) {
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = fis.read(buf)) != -1) {
                        out.write(buf, 0, r);
                    }
                    out.flush();
                }

                // read reply from target
                String resp = bin.readLine();
                if (resp != null && resp.startsWith(Protocol.RESP_OK)) {
                    return true;
                } else {
                    Utils.log("StorageNode", "Target replied: " + resp);
                    return false;
                }
            }
        } catch (Exception e) {
            Utils.log("StorageNode", "pushChunkToTarget error: " + e.getMessage());
            return false;
        }
    }

    // helper: read a line from raw InputStream (until '\n')
    private static String readLineFromStream(InputStream in) throws IOException {
        ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            if (b == '\r') continue;
            lineBuf.write(b);
        }
        if (lineBuf.size() == 0 && b == -1) return null;
        return lineBuf.toString(StandardCharsets.UTF_8.name());
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: java storagenode.StorageNode <masterHost> <masterPort> <servePort> <storageDir>");
            System.exit(1);
        }
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        int servePort = Integer.parseInt(args[2]);
        String storageDir = args[3];

        StorageNode node = new StorageNode(masterHost, masterPort, servePort, storageDir);
        node.start();
    }
}
