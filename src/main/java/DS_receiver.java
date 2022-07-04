import io.ipfs.api.TerminateConditions;
import org.javatuples.Triplet;

import java.net.*;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class DS_receiver extends Thread{
    BlockingQueue<Triplet<String,String,Integer>> Queue;
    DatagramSocket socket;
    public DS_receiver( BlockingQueue<Triplet<String,String,Integer>> queue) {
        Queue = queue;
    }

    public void reply(String msg,String ip, int port) throws Exception{
        System.out.println("REPLY : " + msg.getBytes().length + " , " + port);
        DatagramSocket ssocket = new DatagramSocket();

        DatagramPacket newpacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, InetAddress.getByName(ip), port);
        ssocket.send(newpacket);
        ssocket.close();
    }

    public void run(){
        byte[] buf = new byte[30000];
        String ip;
        int port;
        DatagramPacket packet;
        Random rand = new Random();
        int num = 0;
        while (true){
            try {
                socket = new DatagramSocket(5555);
                socket.setSoTimeout(5000);
                //System.out.println("Waiting");
                packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String sentence = new String( packet.getData(), 0,
                        packet.getLength() );
                //System.out.println("RECEIVED: " + sentence);
                ip = packet.getAddress().getHostAddress();
                port = packet.getPort();
                System.out.println("Received data");
                if(rand.nextDouble() > 0.05){
                    Queue.add(new Triplet<>(sentence,ip,port));
                }
                socket.close();
                

            }catch (Exception e){
            	socket.close();
            	e.printStackTrace();
            }

        }
    }

}
