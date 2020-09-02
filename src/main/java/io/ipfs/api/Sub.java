package io.ipfs.api;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sub extends Thread {
    String Topic;
    String Path;
    BlockingQueue<String> Queue;

    public Sub(String topic, String path, BlockingQueue<String> queue) {
        Queue = queue;
        Topic = topic;
        Path = path;
    }

    public void run() {
        IPFS ipfs = new IPFS(Path);
        try {
            Stream<Map<String, Object>> sub = ipfs.pubsub.sub(Topic, Queue);
            sub.limit(1).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
