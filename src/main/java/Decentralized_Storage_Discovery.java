import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;


public class Decentralized_Storage_Discovery extends Thread {
    String multiaddr, id;
    IPFS ipfs;
    Semaphore InitSem;
    public Decentralized_Storage_Discovery(String multiaddr, Semaphore InitSem) {
        this.multiaddr = multiaddr;
        this.InitSem = InitSem;
        ipfs = new IPFS(multiaddr);
    }

    public void run() {
        try {
            id = (String) ipfs.id().get("ID");
        } catch (Exception e) {
            e.printStackTrace();
        }
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        byte[] rawMessage;
        Sub sub = new Sub(Constants.Discovery.Topic + "Storage", multiaddr, queue, true);
        sub.start();
        System.out.println("Receiver Initialized");
        InitSem.release();
        while (true) {
            try {
                rawMessage = Utils.getRawMessage(queue);
                if (Utils.getTag(rawMessage) == Constants.MessageTags.Discover) {
                    /* separate message from tag */
                    byte[] msg = Arrays.copyOfRange(rawMessage, 1, rawMessage.length);
                    /* the message is the id of the ipls node trying to discover ipfs nodes */
                    String iplsNodeID = new String(msg);
                    /* reply with this node's ID */
                    ByteBuffer buf = ByteBuffer.allocate(id.length() + 1);
                    buf.put(Constants.MessageTags.DiscoverReply);
                    buf.put(id.getBytes());
                    ipfs.pubsub.pub(iplsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
                } else {
                    throw new IllegalStateException("Unexpected value: " + Utils.getTag(rawMessage));
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}

