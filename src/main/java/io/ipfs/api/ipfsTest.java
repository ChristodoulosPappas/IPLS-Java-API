package io.ipfs.api;

import io.ipfs.api.IPFS;
import io.ipfs.api.NamedStreamable;

import java.io.*;
import java.lang.reflect.Array;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;


import io.ipfs.api.cbor.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.ipfs.multiaddr.MultiAddress;


public class ipfsTest {

}
    /*
    public static class Node1 extends Thread{
        MyIPFSClass ipfs = new MyIPFSClass();

        public void run(){
            double [] arr = {1,2,3,4};
            int i = 0;
            List<Map> results;
            IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
            try {
                while(true) {
                    i++;
                    //System.out.println(Arrays.toString(ipfs.recv(ipfsObj,"12")));
                    System.out.println(i);
                }



            }
            catch (Exception e) {
                System.out.println("error");
                e.printStackTrace();
            }
        }
            }


    public static void main(String args[]) throws Exception {

        Node1 t1 = new Node1();

        t1.start();

        double [] arr = {4,3,2,1};
        int i;
        MyIPFSClass ipfs = new MyIPFSClass();
        IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");

        while(true){
            for(i = 0; i < 4; i++){
                arr[i]++;
            }
            try {
                //System.out.println("SENDER : Going to sleep");
                Thread.sleep(500);
                //System.out.println("SENDER : Woke");

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //System.out.println("SENDER : Publishing");
            try {
                String data = ipfs.Marshall_Gradients(arr);
                System.out.println(data);
                //System.out.println(data.toCharArray());
                Object pub = ipfsObj.pubsub.pub("12",data);

            } catch (Exception e) {
                System.out.println("Error");
                e.printStackTrace();
            }


        }



    }
}


*/