package common;

public class Protocol {
    public static final String CMD_REGISTER = "REGISTER";
    public static final String CMD_STORE_CHUNK = "STORECHUNK";
    public static final String CMD_FETCH_CHUNK = "FETCH";
    public static final String CMD_STORE_REQUEST = "STORE_REQ";
    public static final String CMD_LOCATE = "LOCATE";
    public static final String CMD_HEARTBEAT = "HEARTBEAT";

    // replication control
    public static final String CMD_REPLICATE = "REPLICATE";
    public static final String RESP_REPL_OK = "REPL_OK";
    public static final String RESP_REPL_ERR = "REPL_ERR";

    public static final String RESP_OK = "OK";
    public static final String RESP_ERR = "ERR";
    public static final String RESP_STORAGENODES = "NODES";

    private Protocol() {}
}
