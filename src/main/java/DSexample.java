import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import io.ipfs.api.*;
import io.ipfs.multihash.Multihash;
import org.json.JSONObject;

public class DSexample {
    public static Map<String, Multihash> hashStorage;
    public static Sub sub;
    public static BlockingQueue<String> msgQueue;

    public void storeHash(Multihash senderId, Multihash receiverId, Multihash objectId) {

    }

    public List<Multihash> getHashes(Multihash receiverId) {
        return null;
    }

    public static String getRequest() throws InterruptedException {
        JSONObject jobj = new JSONObject(msgQueue.take());
        System.out.println("GOT msg");
        System.exit(1);
        String encoded = (String) jobj.get("data");
        byte[] decodedBytes = Base64.getUrlDecoder().decode(encoded);
        return new String(decodedBytes);
    }

    public void sendReply() {

    }

    public static void main(String[] args) throws Exception {
        hashStorage = new HashMap<>();
        msgQueue = new LinkedBlockingQueue<>();
        try {
            sub = new Sub("IPLSDS", "/ip4/127.0.0.1/tcp/5002",
                    msgQueue, true);
            sub.start();

            getRequest();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        System.out.println("IPLS SETUP");
        do {
            // Wait for messages
            //System.out.println(iplsds.getRequest());
            System.out.println("OUT!");
        } while (true);
    }
}