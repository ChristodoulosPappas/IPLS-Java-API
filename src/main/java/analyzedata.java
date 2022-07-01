import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import io.ipfs.multibase.Base58;
import jdk.jfr.Unsigned;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.math.ec.ECFieldElement;
import org.bouncycastle.math.ec.ECPoint;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.json.JSONObject;
import org.web3j.abi.datatypes.Int;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.StreamSupport;


class trainers extends Thread{
    public trainers(){}

    static String getAlphaNumericString(int n) {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGH"
                + "123456789"
                + "ab";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }
    public static void send_data() throws Exception{
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);

        List<String> agg = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        List<byte[]> Aggregators = new ArrayList<>();
        List<byte[]> Hashes = new ArrayList<>();
        List<Integer> Partitions = new ArrayList<>();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            agg.add("12D3KooWFgboGdpgAtivfe2YZL58aCf2Kqdvk5jePzQ58pnwmPfn");
            //agg.add(getAlphaNumericString(20));
            hashes.add(getAlphaNumericString(64));
            Partitions.add(i);
        }

        for(int i = 0; i < PeerData._PARTITIONS; i++){
            Aggregators.add(Base58.decode(agg.get(i)));
            Hashes.add(Base58.decode(hashes.get(i)));
        }
        PeerData.ds_client.test_storeGradients(Aggregators,Partitions,Hashes,0,getAlphaNumericString(32));
    }

    public void run(){
        Random r = new Random();

        for(int i = 0; i < 10; i++){
            try {
                Thread.sleep(r.nextInt(5000));
                send_data();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }



}



public class analyzedata {
    public java.security.spec.EllipticCurve curve;
    public org.bouncycastle.math.ec.ECCurve bcCurve;

    public static BigInteger fieldOrder;
    public static final int FIELD_ELEMENT_BIT_SIZE = 256;


    public static Map<Integer,String> Find_Candidates(){
        List<String> Candidates = new ArrayList<>();

        List<Integer> Least_Replicated = new ArrayList<>();
        Least_Replicated.add(0);
        Least_Replicated.add(1);
        Least_Replicated.add(2);
        Least_Replicated.add(3);
        Least_Replicated.add(4);
        Least_Replicated.add(6);


        //Initialize priority queue, of the form (Peer,Auth_Size), and put all known peers
        PriorityQueue<org.javatuples.Pair<String,Integer>> queue = new PriorityQueue<>(new Comparator<org.javatuples.Pair<String, Integer>>() {
            @Override
            public int compare(org.javatuples.Pair<String, Integer> objects, Pair<String, Integer> t1) {
                return objects.getValue1()-t1.getValue1();
            }
        });
        PriorityQueue<org.javatuples.Pair<String,Integer>> helpQueue = queue;

        List<String> Peers = new ArrayList<>();
        Map<Integer,String> Candidates_Map = new HashMap<>();
        Pair<String,Integer> Peer = null;

        Peers.add("aaa");
        Peers.add("bbb");
        Peers.add("vvv");
        Peers.add("ass");

        for(int i = 0; i < Peers.size(); i++){
            queue.add(new Pair<>(Peers.get(i),i+1));
        }


        for (int i = 0; i < Least_Replicated.size(); i++) {
            for (int j = 0; j < queue.size(); j++) {
                Peer = helpQueue.remove();
                if (true) {
                    Candidates_Map.put(Least_Replicated.get(i), Peer.getValue0());
                    queue.remove(Peer);
                    queue.add(new Pair<>(Peer.getValue0(), Peer.getValue1() + 1));
                    break;
                }
            }
            helpQueue = queue;
        }

        for(int i = 0; i < Peers.size(); i++){
            System.out.println(queue.remove());
        }

        return Candidates_Map;
    }


    private ECFieldElement getFieldElement(BigInteger i)
    {
        return bcCurve.fromBigInteger(i);
    }

    public static BigInteger randomIntInField(boolean nonzero)
    {
        final int ARGUMENT_IS_LARGER = -1;
        SecureRandom rand = new SecureRandom();
        BigInteger result;
        int watchDog = 1000;

        do {
            result = new BigInteger(FIELD_ELEMENT_BIT_SIZE, rand);

            if (--watchDog == 0)
                throw new RuntimeException("Damn what are the odds?");
        }
        while ( nonzero && result.equals(BigInteger.ZERO) || result.compareTo(fieldOrder)!= ARGUMENT_IS_LARGER);

        return result;

    }


    public static ECPoint[] generate_parameters(ECPoint generator,int parameters){
        ECPoint[] public_parameters = new ECPoint[parameters];
        BigInteger r;
        for(int i = 0; i < parameters; i++){
            r = randomIntInField(false);
            public_parameters[i] = generator.multiply(r);
        }
        return  public_parameters;
    }

    public static ECPoint pedersen_vector_commitment(ECPoint[] pp,BigInteger[] arr){
        ECPoint commitment = pp[0].multiply(arr[0]);

        for(int i = 1; i < arr.length; i++){
            commitment = commitment.add(pp[i].multiply(arr[i]));
        }

        return commitment;
    }

    public static void test_encoding(){
        int len = 100000;
        System.out.println(BigDecimal.valueOf(Math.PI).toBigInteger());
        double[] arr = new double[len];
        double[] arr2 = new double[len];
        double sum1 = 0;
        long sum2 = 0;
        Random r = new Random();
        BigInteger num = BigInteger.valueOf(len);
        BigInteger sum = BigInteger.valueOf(0);
        for(int i = 0; i < arr.length; i++){
            arr[i] = -10.0 + (20.0)*r.nextDouble();
            arr2[i] = new Double(arr[i]*Math.pow(10,12)).longValue();
            //arr2[i] = new Double(10000000000.0*arr[i]).longValue();//.add(BigInteger.valueOf(Long.MAX_VALUE));
            sum1+= arr[i];
            sum2 +=arr2[i];
            sum = sum.add(BigInteger.valueOf(new Double(arr2[i]).longValue())).add(BigInteger.valueOf(Long.MAX_VALUE));
        }
        System.out.println(sum1);
        //System.out.println(BigInteger.valueOf(new Double(sum1).longValue()));
        System.out.println((double)sum2/(double)Math.pow(10,12));
        System.out.println(sum2);
        System.out.println(sum.subtract(num.multiply(BigInteger.valueOf(Long.MAX_VALUE))));
    }


    public static void mean(List<Long> arr){
        double avg = 0.0;
        for(int i = 0; i < arr.size(); i++){
            avg+= arr.get(i);
        }
        System.out.println(avg/arr.size());
    }
    public static void get_statistics(MyIPFSClass ipfsClass) throws Exception{
        String folder = "16_direct";
        List<Long> aggr = (List<Long>) ipfsClass.read_file(folder+"/12D3KooWRP5Hc4YgSE3QQEyXCKLiuSPKEaHw4jBz8p8Lq6X2hmr3_Aggregation");
        List<Long> pure_aggr = (List<Long>)ipfsClass.read_file(folder+"/12D3KooWRP5Hc4YgSE3QQEyXCKLiuSPKEaHw4jBz8p8Lq6X2hmr3_Pure_Aggregation");
        List<Long> data_received = (List<Long>) ipfsClass.read_file(folder+"/12D3KooWRP5Hc4YgSE3QQEyXCKLiuSPKEaHw4jBz8p8Lq6X2hmr3_Data_Received");

        System.out.println("Aggr");
        mean(aggr);
        for(int i = 0; i < 10; i++){
            System.out.println(aggr.get(i));
        }
        System.out.println("==========");
        System.out.println("Pure aggr");
        mean(pure_aggr);
        for(int i = 0; i < 10; i++){
            System.out.println(pure_aggr.get(i));
        }
        System.out.println("Received");
        mean(data_received);

        List<Long> Upload = (List<Long>) ipfsClass.read_file(folder+"/12D3KooWAGQbdjL7P72jhgeW6mAECWZQBWLhwa8fkFqLFDBB24zN_Trainer");
        System.out.println("================================================================================================");
        //System.out.println(ipfsClass.read_file("measure/12D3KooWAGQbdjL7P72jhgeW6mAECWZQBWLhwa8fkFqLFDBB24zN_Trainer"));

        //System.out.println(ipfsClass.read_file("measure/12D3KooWAs5WaveE4hxcoUBBWt1LfeyQz5TnfTLUbHVX58vFWVRE_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWAs5WaveE4hxcoUBBWt1LfeyQz5TnfTLUbHVX58vFWVRE_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWBYU9wT4udg5EZX3yoPrwetobZhjCk2UFXD14TV5oPmjP_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWBYU9wT4udg5EZX3yoPrwetobZhjCk2UFXD14TV5oPmjP_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWFs2JCD21N7wduJ1FXUULvYpLVUmZtj68DHB25v8tKxyr_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWFs2JCD21N7wduJ1FXUULvYpLVUmZtj68DHB25v8tKxyr_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWGhsZ1j7KAcBry4KTWPjj3hg5ToY8wUsz8UX7v7PjditG_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWGhsZ1j7KAcBry4KTWPjj3hg5ToY8wUsz8UX7v7PjditG_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWGkeFfnhyGww7vZgn8bAQixT4TvpkLd7XHVv9gpWCmUvQ_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWGkeFfnhyGww7vZgn8bAQixT4TvpkLd7XHVv9gpWCmUvQ_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWHSJZQmKkiSX9D3T7WSCbESxcrY3ESydkoLq4LDdYVrkR_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWHSJZQmKkiSX9D3T7WSCbESxcrY3ESydkoLq4LDdYVrkR_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWKD3hpPv8N9mD1SFJUHKVuHda4C1ExveCARw8MRxZDVQc_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWKD3hpPv8N9mD1SFJUHKVuHda4C1ExveCARw8MRxZDVQc_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWLC2WCrdoeKmSSgaUdNZnwgufcyFK6sseLnWsQtAE81TJ_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWLC2WCrdoeKmSSgaUdNZnwgufcyFK6sseLnWsQtAE81TJ_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWLjTfGghPMZoPFTrG9FVHkcd2riTxXMWT14bsvMmWkra2_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWLjTfGghPMZoPFTrG9FVHkcd2riTxXMWT14bsvMmWkra2_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWLZAdLAU1uwiEU2gioX9DS2ptZLNAni3uTZmYuuN1j6FK_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWLZAdLAU1uwiEU2gioX9DS2ptZLNAni3uTZmYuuN1j6FK_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWMqF4ttdzk6xVqry2nZhqsSrajaDUDqSoJoUdxJdQMqPd_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWMqF4ttdzk6xVqry2nZhqsSrajaDUDqSoJoUdxJdQMqPd_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWNGY8hs4hptM4K3LMiQwE5e8YkUSh6s2MQVx23Dj6bzJF_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWNGY8hs4hptM4K3LMiQwE5e8YkUSh6s2MQVx23Dj6bzJF_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWNnqFoUUkN9Gwzz5imNPvRhpKn7URR3r8tg41fbsUqSPk_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWNnqFoUUkN9Gwzz5imNPvRhpKn7URR3r8tg41fbsUqSPk_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWP3wN9uwLmcyYDUKdzh6xNDJWTemcBzSYMcdsxsBkJEHb_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWP3wN9uwLmcyYDUKdzh6xNDJWTemcBzSYMcdsxsBkJEHb_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWPAg22dAL2xUdw5zAVhtgV92nbDn7XBn6YszLYN6n6tAs_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWPAg22dAL2xUdw5zAVhtgV92nbDn7XBn6YszLYN6n6tAs_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWPFZTLHv2y5CLWdf3RagzN68G7r8pZRTZAtbzyQa9FPx3_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWPFZTLHv2y5CLWdf3RagzN68G7r8pZRTZAtbzyQa9FPx3_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWQMF74Tag5GJAHnokJGbuq1pSpNbNQqVzBRVm3jqL7Eii_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWQMF74Tag5GJAHnokJGbuq1pSpNbNQqVzBRVm3jqL7Eii_Trainer"));
        //System.out.println(ipfsClass.read_file("measure/12D3KooWRM2mcpFy3KyKis19iyJ2gz4qmFuvToJELWYYdSQBSoUd_Trainer"));
        Upload.addAll((List<Long>)ipfsClass.read_file(folder+"/12D3KooWRM2mcpFy3KyKis19iyJ2gz4qmFuvToJELWYYdSQBSoUd_Trainer"));

        System.out.println(Upload.size());
        System.out.println("upload times");
        mean(Upload);
        int counter = 0;
        for(int i = 0 ; i < Upload.size()/10; i++){
            for(int j = 0; j < 10; j++){
                System.out.println(Upload.get(counter));
                counter++;
            }
            System.out.println("=====================");
        }
        System.out.println((long)((long)(ipfsClass.get_curr_time())*1000) + " , " + System.currentTimeMillis());

    }

    public static void read_data() throws Exception {
        String str = "a";
        String msg = "";
        List<Long> aggr_time = new ArrayList<>();
        List<Long> Sync_time = new ArrayList<>();
        List<Long> communication = new ArrayList<>();
        String folder = "16_aggr";
        String[] file = new String[16];
        file[0] =folder+ "/12D3KooWCqYLqPeeRBH9D4P4DpFCEDYqHWVyFxanWnMEEhXK1L8Q_Aggregation";
        file[1] =folder+ "/12D3KooWE7ZqBGhM51B8Z2jmL1mcLwzUvSAx4iRAykHnkzswkXdM_Aggregation";
        file[2] =folder+ "/12D3KooWMv3hRwczLt2KLj4ahrUTNGFrD76TDEuYJ6By2GXdY3UD_Aggregation";
        file[3] =folder+ "/12D3KooWS7xR46PDSB4GxhK3fNP7d3oGN7g2AeKoV46qyQqMkpHD_Aggregation";
        file[4] =folder+ "/12D3KooWGkTREtxJowJmkKWsqNLvQZZNk3WvBw11n3SvdMTLwLyb_Aggregation";
        file[5] =folder+ "/12D3KooWHZFdaE328pRuHF1eFX4E1BeHYs2EUFL3WLVcykShpYiM_Aggregation";
        file[6] =folder+"/12D3KooWSNbrsys2sZVutVzgLKQbbj9C1wGdurN25geGKZsDah2u_Aggregation";
        file[7] =folder+"/12D3KooWRNRx83zgZ4sm2bW533SM1UnonxkEF8jCAGVfGo7tiEhB_Aggregation";
        file[8] =folder+"/12D3KooWHo3ddkfqptYVs2k4HavVxyt81tbq4cXJTwLd3poMeuut_Aggregation";
        file[9] =folder+"/12D3KooWLfUPReNZppo1WG9UcXQonb2pn8DJ7TcByinLe8nb7Nos_Aggregation";
        file[10] =folder+"/12D3KooWMcJQwqpHL7dqw2ba715mnbzC2sPW4gCdpfpcXYowN5b1_Aggregation";
        file[11] =folder+"/12D3KooWRFQB6uqttcB9o4ADHMT1mFBydGSqmnFUXHqyGBwTKfg7_Aggregation";
        file[12] =folder+ "/12D3KooWLW5japSaZrmEbNHz664rsTB1wwTy3jUAHUvFYgC4RQYa_Aggregation";
        file[13] =folder+"/12D3KooWJudyFtcg8fSk1dWFERyVqV3XMyB2XvdKN6V9SvwbsY7p_Aggregation";
        file[14] =folder+"/12D3KooWJJRBqJ6ZB9PMZyAhaMCrcSykDvxnHBpzRU5Y14CEw3QE_Aggregation";
        file[15] =folder+"/12D3KooWSrmkYN7k2ptj5B7pUHu44hHBuoRWN5eHrHMkMuJ2nngv_Aggregation";



        for(int i = 0; i< file.length; i++){
            FileInputStream fileIn = new FileInputStream(file[i]);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Map<String,List<Long>> data = (Map<String,List<Long>>) objectIn.readObject();
            //System.out.println(data);
            for(int j = 0; j < data.get("pure_aggregation").size(); j++){
                aggr_time.add(data.get("pure_aggregation").get(j));
                Sync_time.add(data.get("iter_time").get(j) - data.get("aggregation_time").get(j));
                communication.add(data.get("data_received").get(j));
            }

            objectIn.close();
            fileIn.close();

        }
        System.out.println("====================");
        for(int i = 0; i < aggr_time.size(); i++){
            System.out.println(aggr_time.get(i));
        }
        System.out.println("====================");
        for(int i = 0; i < Sync_time.size(); i++){
            System.out.println(Sync_time.get(i));
        }
        System.out.println("====================");
        for(int i = 0; i < communication.size(); i++){
            System.out.println(communication.get(i));
        }
    }



    static String getAlphaNumericString(int n) {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGH"
                + "123456789"
                + "ab";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    public static void write_data(Bootstraper_Services schedule_daemon) throws Exception{
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);

        List<String> agg = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        List<byte[]> Aggregators = new ArrayList<>();
        List<byte[]> Hashes = new ArrayList<>();
        List<Integer> Partitions = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            agg.add("12D3KooWFgboGdpgAtivfe2YZL58aCf2Kqdvk5jePzQ58pnwmPfn");
            //agg.add(getAlphaNumericString(20));
            hashes.add(getAlphaNumericString(64));
            Partitions.add(i);
        }

        for(int i = 0; i < 10; i++){
            Aggregators.add(Base58.decode(agg.get(i)));
            Hashes.add(Base58.decode(hashes.get(i)));
        }
        schedule_daemon.storedirectory(getAlphaNumericString(20).getBytes(),Aggregators,Partitions,Hashes);
    }

    public static void send_data(IPFS ipfs) throws Exception{
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);

        List<String> agg = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        List<byte[]> Aggregators = new ArrayList<>();
        List<byte[]> Hashes = new ArrayList<>();
        List<Integer> Partitions = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            agg.add("12D3KooWFgboGdpgAtivfe2YZL58aCf2Kqdvk5jePzQ58pnwmPfn");
            //agg.add(getAlphaNumericString(20));
            hashes.add(getAlphaNumericString(20));
            Partitions.add(i);
        }

        for(int i = 0; i < 10; i++){
            Aggregators.add(Base58.decode(agg.get(i)));
            Hashes.add(Base58.decode(hashes.get(i)));
        }
        PeerData.ds_client.storeGradients(Aggregators,Partitions,Hashes,0);
    }

    public static void poll(IPFS ipfs) throws Exception{
        int curr_iter = 0;
        int print_counter = 0;
        boolean first_commitment = false;
        boolean print_flag = false;
        List<Triplet> black_list = new ArrayList<>();
        List<org.javatuples.Pair<String,Integer>> committed_gradients = new ArrayList<>();

        Map<Integer, List<io.ipfs.api.Pair<byte[], byte[]>>> gradient_commitments = new HashMap<>();
        for(int i = 0; i < 10; i++){
            PeerData.Auth_List.add(i);
        }

        while(true){
            Thread.sleep(1000);
            // Download the gradient commitments that didn't downloaded yet
            try {
                gradient_commitments = PeerData.ds_client.getGradients(ipfs, new HashSet<>(PeerData.Auth_List), curr_iter);
            }catch (Exception e){
                gradient_commitments = null;
            }
            // Pass those commitments to the download scheduler
            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                if(gradient_commitments == null || gradient_commitments.get(PeerData.Auth_List.get(i)) == null){
                    continue;
                }
                for (io.ipfs.api.Pair<byte[], byte[]> p : gradient_commitments.get(PeerData.Auth_List.get(i))) {
                    //System.out.println(PeerData.aggregation_download_scheduler);
                    //FSystem.out.println(new Quintet(Base58.encode(p.right),new String(p.left),curr_iter,PeerData.Auth_List.get(i),PeerData._ID));
                    //System.out.println(p.right + " , " + p.left + " , ");

                }
            }


        }
    }


    public static void main(String[] argc) throws Exception {

        MyIPFSClass ipfsClass = new MyIPFSClass(argc[0]);
        IPFS ipfs = new IPFS(argc[0]);
        String is_client = argc[1];
        PeerData._PARTITIONS = new Integer(argc[2]);
        PeerData.Path = argc[0];
        PeerData._ID = "12D3KooWCyJZJphf9z1Dbd2sJKYc11PVV2RBVA9HQjNz26oMANgR";

        if(is_client.equals("1")){
            System.out.println("OK");
            Bootstraper_Services schedule_daemon = new Bootstraper_Services(argc[0],1,10000,10000,10000);
            schedule_daemon.start();
            //Thread.sleep(1000);
            //for(int i = 0; i < 1000; i++){
            //    write_data(schedule_daemon);
            //}
            //System.out.println("Data writen");
            while(true){Thread.yield();}
        }
        else if(is_client.equals("2")){
            PeerData.ds_client = new IPLS_DS_Client(ipfs);
            PeerData.ds_client.start();
            poll(ipfs);
        }
        else{
            PeerData.ds_client = new IPLS_DS_Client(ipfs);
            PeerData.ds_client.start();
            Thread.sleep(100);

            for(int i = 0; i <33; i++){
                trainers t = new trainers();
                t.start();
            }
            //send_data(ipfs);
            System.out.println("OK");
            //send_data(ipfs);
            while (true){Thread.yield();}
        }



        //get_statistics(ipfsClass);
        /*
        FileInputStream fileIn = new FileInputStream("measure/12D3KooWE7ZqBGhM51B8Z2jmL1mcLwzUvSAx4iRAykHnkzswkXdM_Aggregation");
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        //ByteArrayInputStream bis = new ByteArrayInputStream(data);
        //ObjectInput in = new ObjectInputStream(bis);
        Map<String,List<Double>> data = (Map<String,List<Double>>) objectIn.readObject();
        objectIn.close();
        fileIn.close();
        System.out.println(data);
         */

        /*
        if(argc[1].equals(argc[0])) {
            for (int i = 0; i < 5000; i++) {
                str += "a";
            }
            for (int i = 0; i < 50; i++) {
                msg += str;
            }

            for (int i = 0; i < 100; i++) {

                //String Hash = ipfsClass.add_file("data_l/" + new Integer(i).toString()).toString();
                //ByteBuffer buf = ByteBuffer.allocate(Hash.length());
                //buf.put(Hash.getBytes());
                System.out.print(ipfs.pubsub.pub("a", msg));
                //System.gc();
                System.out.println(i);
            }
            Thread.sleep(100000);
        }
        else{
            BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            Sub sub = new Sub("a", argc[0], queue, true);
            sub.start();
            int j = 0;

            while (true){
                queue.take();
                System.out.println("Took data " + j);
                j++;
            }
        }



         */
        /*
        ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec("secp256r1");;
        fieldOrder = new BigInteger ("ffffffff00000001000000000000000000000000ffffffffffffffffffffffff", 16);
        BigInteger[] arr1 = new BigInteger[5];
        BigInteger[] arr2 = new BigInteger[5];
        BigInteger[] arr3 = new BigInteger[5];

        for(int i = 0; i < 5; i++){
            arr1[i] = BigInteger.valueOf(i+1);
            arr2[i] = BigInteger.valueOf(2*i+1);
            arr3[i] = BigInteger.valueOf(3*i+2);;
        }


        double num = 1213.12;
        long l = new Double(num).longValue();
        ECPoint g = spec.getG();
        ECPoint[] pp = generate_parameters(g,5);

        test_encoding();
        //assertEquals(gC, sum);

         */
        /*
        ECFieldElement fA, fB, fC;
        ECPoint gA, gB, gC, sum;

        fA = getFieldElement(a);
        fB = getFieldElement(b);
        fC = fA.add(fB);

        gA = getG().multiply(a);
        gB = getG().multiply(b);
        gC = getG().multiply(fC.toBigInteger());
        sum = gA.add(gB);


         */
        /*
        String[] view = new String[5];
        List<Integer> l = new ArrayList<>();
        l.add(1);
        l.add(2);
        view[0] = "fizaras";
        view[1] = "mizaras";
        view[2] = "flexaras";
        view[3] = "sxizaras";
        view[4] = "vice";

        ipfsClass.update_file("Storage_View",view);
        Multihash Hash = ipfsClass.add_file("Storage_View");
    	String schedule_hash = ipfsClass._Upload_File(new Pair<>(l,Hash.toString()),"Storage_View").toString();

        System.out.println("schedule hash : " + schedule_hash + " , " + Hash);
        System.out.println(ipfsClass.getSchedule(schedule_hash));
        String[] arr = ipfsClass.GetView(ipfsClass.getSchedule(schedule_hash).getValue1());
        for(int i = 0; i < arr.length; i++){
            System.out.println(arr[i]);
        }


         */
        /*
    	List<Integer> Auth = new ArrayList<>();
        List<String> Peers = new ArrayList<>();

        Auth.add(1);
        Auth.add(2);
        Auth.add(3);

        Peers.add("aaa");
        Peers.add("bbb");
        Peers.add("vvv");
        Peers.add("ass");
        Peers.add("xxx");

        MyIPFSClass ipfsClass = new MyIPFSClass("/ip4/127.0.0.1/tcp/5001");
        String decodedString = ipfsClass.Marshall_Packet(Auth,Peers);

        byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
        int OriginPeerSize,PeerSize,pid;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        String Renting_Peer = null,Origin_Peer= null;
        //Get Pid
        pid = rbuff.getShort();
        System.out.println(ipfsClass.Get_RMap(rbuff,bytes_array));
        
        */
        
        /*
        List<Double> data1 = new ArrayList<>();


        FileInputStream file = new FileInputStream("ChartData");
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        data1 = (List<Double>) in.readObject();
        for(int i = 0; i < data1.size(); i++){
            System.out.println(data1.get(i));
        }
        */


    }
}
