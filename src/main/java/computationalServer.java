import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class computationalServer {

    public static void main(String args[]){
        String path = args[0];
        IPFS ipfs = new IPFS(path);
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();


        Sub SUB = new Sub("server",path,queue,true);
        SUB.start();
        while(true){
            
        }
    }

}
