import io.ipfs.api.IPFS;
import org.javatuples.Pair;
import org.javatuples.Septet;
import org.javatuples.Sextet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Downloader extends Thread{
    BlockingQueue<Septet<Byte,Byte,Integer,Integer,String,String,String>> queue;
    IPFS ipfs;
    MyIPFSClass ipfsClass;
    public Downloader(BlockingQueue<Septet<Byte,Byte,Integer,Integer,String,String,String>> work){
        queue = work;
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
    }
    public void run(){
        try {

            while(true){
                System.out.println("Waiting");
                // tag,mod, partition,iteration,hash,trainer,aggregator
                Septet<Byte,Byte,Integer,Integer,String,String,String> request = queue.take();
                System.out.println("Mod : " + request.getValue1() + " , P : " + request.getValue2() + " , I " + request.getValue3());

                ipfs.get(request.getValue4());
                List<String> arr = new ArrayList<>();
                arr.add(request.getValue4());
                arr.add(PeerData._ID);
                //System.out.println("Added " + partition_type.get(mod) + ", " + partition + " in iteration " + iteration + " with hash: " + hash + " time : " + (e - b));
                ipfs.pubsub.pub(request.getValue6(),ipfsClass.Marshall_Packet(arr,true,(short) 56));
                ByteBuffer replyMsgBuffer = ByteBuffer.allocate(2 + 3 * Integer.BYTES + request.getValue4().length());
                replyMsgBuffer.put(request.getValue0());
                replyMsgBuffer.put(request.getValue1());
                replyMsgBuffer.putInt(request.getValue2());
                replyMsgBuffer.putInt(request.getValue3());
                replyMsgBuffer.putInt(request.getValue4().length());
                replyMsgBuffer.put(request.getValue4().getBytes());
                Thread.sleep(200);
                ipfs.pubsub.pub(request.getValue5() + "Storage", Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                Thread.sleep(200);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
