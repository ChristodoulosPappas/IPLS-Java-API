//import io.ipfs.api.Pair;
import io.ipfs.api.Peer;
import io.ipfs.multihash.Multihash;
import org.javatuples.Pair;
import org.javatuples.Triplet;

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

    public static int _PARTITIONS;
    public static int _MIN_PARTITIONS;
    public static int _MODEL_SIZE;
    public static boolean First_Iter = true;
    public static boolean isBootsraper;
    public static int _Iter_Clock = 0;
    public static String _ID = null;
    public static String MyPublic_Multiaddr = null;
    public static int Index = 0;
    public static int is_alive_counter = 0;


    volatile public static int STATE = 0;

    //These variables are used for analyzing the system
    public static Semaphore Test_mtx = new Semaphore(1);
    public static List<Integer> RecvList = new ArrayList<>();
    public static int DataRecv = 0;

    //Blocking Queue, for any task given to the updater
    public static BlockingQueue<Triplet<String,Integer, List<Double>>> queue = new LinkedBlockingQueue<Triplet<String,Integer, List<Double>>>();
    //Blocking Queue, for Global Gradients Pool
    public static BlockingQueue<String> GGP_queue = new LinkedBlockingQueue<>();
    //Blocking Queue, for Global Gradients Authority updates
    public static BlockingQueue<Pair<Integer,Integer>> UpdateQueue = new LinkedBlockingQueue<>();


    public static Map<Integer,List<Double>> GradientPartitions = new HashMap<>();
    //public static List<Double> Gradients = new ArrayList<Double>();
    public static Map<Integer,List<Double>> Aggregated_Gradients = new HashMap<>();
    public static Map<Integer,List<Double>> Stored_Gradients = new HashMap<>();
    public static Map<Integer,List<Double>> Weights = new HashMap<Integer, List<Double>>();

    public static boolean sendingGradients = false;
    //List that shows for the peers that have not yet replied
    public static List<Pair<String,Integer>> Wait_Ack = new ArrayList<Pair<String,Integer>>();
    //Hash that give us the latest update of each partition
    public static Map<Integer, Multihash> LastUpdate = new HashMap<Integer, Multihash>();

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
    //Hash table in the form : [Swarm_Peer,[His Authority]]
    public static Map<String,List<Integer>> Swarm_Peer_Auth = new HashMap<String,List<Integer>>();
    //Hash table that contains that contains the hash value of the file, with key based on partition
    public static Map<Integer,List<Double>> Weight_Address = new HashMap<Integer, List<Double>>();

    //For testings
    public static int num;

}
