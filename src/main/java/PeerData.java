//import io.ipfs.api.Pair;
import io.ipfs.api.Peer;
import io.ipfs.multihash.Multihash;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.web3j.abi.datatypes.Int;

import javax.naming.InsufficientResourcesException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class PeerData {
    public static Semaphore InitSem = new Semaphore(0);
    public static Semaphore mtx = new Semaphore(1);
    public static Semaphore SendMtx = new Semaphore(1);
    public static Semaphore weightsMtx = new Semaphore(1);

    public static Map<Integer,List<String>> workers = new HashMap<>();
    public static Map<Integer,List<String>> Replica_workers = new HashMap<>();

    public static Map<Integer, Double> previous_iter_active_workers = new HashMap<>();

    // The number of partitions that we partition the model
    public static int _PARTITIONS;
    //The minimum number of responsibilities a peer must have
    public static int _MIN_PARTITIONS;
    // The number of parameters of the model
    public static long _MODEL_SIZE;
    // Boolean value inficating if the system is on the initialization phase. Becomes false when it proceed on training phase
    public static boolean First_Iter = true;
    // Boolean value indicating if the system is using SGD or Asynchronous GD
    public static boolean isSynchronous = true;
    public static boolean isBootsraper;
    public static int _Iter_Clock = 0;
    public static String _ID = null;
    public static String MyPublic_Multiaddr = null;
    public static int Index = 0;
    public static int is_alive_counter = 0;
    // The global logical clock of the system indicating the global iteration. This is used only in SGD
    public static int middleware_iteration = 0;
    public static List<String> Members = new ArrayList<>();
    public static int Min_Members;
    public static boolean training_phase = false;
    public static boolean IPNS_Enable = false;


    volatile public static int STATE = 0;

    //These variables are used for analyzing the system
    public static Semaphore Test_mtx = new Semaphore(1);
    public static List<Integer> RecvList = new ArrayList<>();
    public static int DataRecv = 0;

    //Blocking Queue, for any task given to the updater
    public static BlockingQueue<Quintet<String,Integer,Integer, Boolean ,List<Double>>> queue = new LinkedBlockingQueue<Quintet<String,Integer,Integer,Boolean, List<Double>>>();
    //Blocking Queue, for Global Gradients Pool
    public static BlockingQueue<String> GGP_queue = new LinkedBlockingQueue<>();
    //Blocking Queue, for Global Gradients Authority updates
    public static BlockingQueue<Pair<Integer,Integer>> UpdateQueue = new LinkedBlockingQueue<>();


    public static Map<Integer,List<Double>> GradientPartitions = new HashMap<>();
    //public static List<Double> Gradients = new ArrayList<Double>();
    //The gradients received from replica peers
    public static Map<Integer,List<Double>> Replicas_Gradients = new HashMap<>();
    // The gradients received from my clients (peers that are not responsible for my partitions so i am a dealer of them)
    public static Map<Integer,List<Double>> Aggregated_Gradients = new HashMap<>();
    // There is a very very small and almost impossible for realsitic systems chance that a client has finished the next iteration while i am in the previous (just before end the updateGradientMethod)
    // This Structure aggregate those gradients from fututre
    public static Map<Integer,List<Double>> Aggregated_Gradients_from_future = new HashMap<>();
    public static Map<Integer,List<Double>> Aggregated_Weights = new HashMap<>();
    public static Map<Integer,List<Double>> Stored_Gradients = new HashMap<>();
    public static Map<Integer,List<Double>> Weights = new HashMap<Integer, List<Double>>();

    public static boolean sendingGradients = false;
    //List that shows for the peers that have not yet replied
    public static List<Triplet<String,Integer,Integer>> Wait_Ack = new ArrayList<Triplet<String,Integer,Integer>>();
    public static List<Triplet<String,Integer,Integer>> Wait_Ack_from_future = new ArrayList<Triplet<String,Integer,Integer>>();

    //List that shows the clients that have not yet sent gradients. I must wait until Client_Wait_Ack size is 0
    public static List<Triplet<String,Integer,Integer>> Client_Wait_Ack = new ArrayList<>();
    // The same somehow with the Aggregated_Gradients_from_future
    public static List<Triplet<String,Integer,Integer>> Client_Wait_Ack_from_future = new ArrayList<>();
    // The same as Client_Wait_Ack but for the replicas synchronization
    public static List<Triplet<String,Integer,Integer>> Replica_Wait_Ack = new ArrayList<>();
    public static List<Triplet<String,Integer,Integer>> Replica_Wait_Ack_from_future = new ArrayList<>();

    //Hash that give us the latest update of each partition
    public static Map<Integer, Integer> Participants = new HashMap<>();


    public static String  Path;
    //List of partitions that a peer is responsible for
    public static List<Integer> Auth_List = new ArrayList<Integer>();
    //List of string with the unique ids of peers
    public static List<String> Existing_peers = new ArrayList<String>();
    //Peers that left must also be removed from data structures
    public static List<String> Leaving_peers = new ArrayList<String>();
    public static List<String> Bootstrapers = new ArrayList<>();
    public static List<Peer> peers = new ArrayList<Peer>();
    //Hash table in the form : [Authority_id,[Selected_Peers]]
    public static Map<Integer,List<String>> Partition_Availability = new HashMap<Integer,List<String>>();
    // Hash table showing the peers that the daemo communicates
    public static Map<Integer,String> Dealers = new HashMap<Integer, String>();
    //Hash table in the form : [Swarm_Peer,[His Authority]]
    public static Map<String,List<Integer>> Swarm_Peer_Auth = new HashMap<String,List<Integer>>();
    //Hash table that contains that contains the hash value of the file, with key based on partition
    public static Map<Integer,List<Double>> Weight_Address = new HashMap<Integer, List<Double>>();
    //Hash table that contains the clients of a peer, aka those who
    // send to him the gradients. This data structure is been used
    // only in Synchronous SGD
    public static Map<Integer,List<String>> Clients = new HashMap<>();
    //This is a hash table that keeps into account the new clients joined in the
    // system in order to make them officially acceptable after the iteration
    // is finished
    public static Map<Integer,List<String>> New_Clients = new HashMap<>();
    public static Map<Integer,List<String>> New_Replicas = new HashMap<>();
    public static Map<Integer,List<String>> New_Members = new HashMap<>();
    public static Map<Integer,List<String>> Replica_holders = new HashMap<>();
    public static Map<String,Integer> Servers_Iteration = new HashMap<>();
    public static Map<String,Integer> Clients_Iteration = new HashMap<>();

    //For testings
    public static int num;

}
