package common;


import java.time.Instant;


public class Utils {
    public static String now() {
        return Instant.now().toString();
    }


    public static void log(String who, String msg) {
        System.out.println("[" + now() + "] [" + who + "] " + msg);
    }
}