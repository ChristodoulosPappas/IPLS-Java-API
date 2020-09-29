package io.ipfs.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sub extends Thread {
    String Topic;
    List<String> TopicL = null;
    String Path;
    BlockingQueue<String> Queue;
    boolean Dynamic;
    public Sub(String topic, String path, BlockingQueue<String> queue,boolean dynamic) {
        Queue = queue;
        Topic = topic;
        Path = path;
        Dynamic = dynamic;

        TerminateConditions.terminate.put(topic,false);

    }

    public void terminate(){
        TerminateConditions.terminate.put(Topic,true);
    }

    public void run() {
        IPFS ipfs = new IPFS(Path);
        try {
            Stream<Map<String, Object>> sub = null;
            sub = ipfs.pubsub.sub(Topic, Queue,Dynamic);

            sub.limit(1).collect(Collectors.toList());
            sub.close();
           
            System.out.println("Subscription FINISHED");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
