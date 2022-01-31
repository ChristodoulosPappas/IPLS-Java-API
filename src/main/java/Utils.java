import org.json.JSONObject;

import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;

public class Utils {
    public static byte[] getRawMessage(BlockingQueue<String> queue) throws InterruptedException {
        byte[] decodedBytes;
        String raw, encoded;
        raw = queue.take();
        JSONObject obj = new JSONObject(raw);
        encoded = (String) obj.get("data");
        decodedBytes = Base64.getUrlDecoder().decode(encoded);
        decodedBytes = Base64.getUrlDecoder().decode(decodedBytes);
        return decodedBytes;
    }

    public static byte getTag(byte []rawMessage){
        return rawMessage[0];
    }

}
