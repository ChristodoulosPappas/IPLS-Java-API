import io.ipfs.api.Sub;
import io.ipfs.multihash.Multihash;
//import jdk.nashorn.internal.ir.Labels;
import org.apache.commons.cli.*;
import org.deeplearning4j.datasets.iterator.INDArrayDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.*;
import org.deeplearning4j.datasets.iterator.impl.LFWDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.gradient.Gradient;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.javatuples.LabelValue;
import org.javatuples.Pair;
import org.javatuples.Triplet;
//import org.nd4j.evaluation.classification.Evaluation;
import org.json.JSONArray;
import org.json.JSONObject;
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.AsyncDataSetIterator;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
//import org.nd4j.shade.guava.primitives.Doubles;
import io.ipfs.api.IPFS;

import io.ipfs.multiaddr.MultiAddress;
import org.nd4j.shade.guava.primitives.Doubles;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.cli.*;

import static org.deeplearning4j.datasets.iterator.impl.EmnistDataSetIterator.Set.LETTERS;


class Console extends Thread{


    public void run(){
        System.out.println("Press Any Key to continue... ");
        while(true){
            try{

                BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
                String s = bufferRead.readLine();
                PeerData.STATE = 1;
                System.out.print("Ok1");

                if(s.equals("exit")) {
                    System.out.println("Exiting");
                    PeerData.STATE = 2;
                    Model.ipls.terminate();
                    System.exit(1);
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();

            }
            catch (NullPointerException e){
                System.out.println("null");
                PeerData.STATE = 2;
                try {
                    Model.ipls.terminate();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }

                //System.exit(1);
                //return;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}


// This is an example of use of IPLS. In this example you can start at most 5 peers just because i created only 5 different datasets.
// The datasets and other related files are on the file MNIST_Partitioned_Dataset
// If you want to run this program, then you should follow these steps:
// 1) Create a private IPFS network
// 2) Start a Model process for the bootstraper process with : -a /ip4/127.0.0.1/tcp/(Port_number of the IPFS API address of that node) -topic 1 -bID XXXX(the peers ID) -p 16 -mp 2 -b true -n 4
// 3) Start the Computational_Server in order not to train ever model in parallel (otherwise it might destroy your PC ) with : /ip4/127.0.0.1/tcp/YYYY
// The start each peer and for each of the 4 peers use the bellow :
// 4) -a /ip4/127.0.0.1/tcp/(Port_number of the IPFS API address of that node) -topic 2 -bID XXXX (Bootstrapers hash id) -p 16 -mp 4 -b false -n 4 -IPNS false
// 5) -a /ip4/127.0.0.1/tcp/(Port_number of the IPFS API address of that node) -topic 3 -bID XXXX (Bootstrapers hash id) -p 16 -mp 4 -b false -n 4 -IPNS false
// 6) -a /ip4/127.0.0.1/tcp/(Port_number of the IPFS API address of that node) -topic 4 -bID XXXX (Bootstrapers hash id) -p 16 -mp 4 -b false -n 4 -IPNS false
// 7) -a /ip4/127.0.0.1/tcp/(Port_number of the IPFS API address of that node) -topic 5 -bID XXXX (Bootstrapers hash id) -p 16 -mp 4 -b false -n 4 -IPNS false


/*
    In order to start an IPLS project, you must define also some important hyper parameters which are:
        * The IPFS Path for the communication of the IPLS with the IPFS daemon.
        * The number of partitions you want to partition the model. All the peers must be informed with the same partition number.
        * The minimum number of responsibilities you want the peers to have. For example you can decide to partition the model in 16 segments and each peer must be responsible for at least 2 of the 16 segments.
        * A boolean value to inform the peer if he is bootstraper of the private network or not.
        * The IPFS hash id of the bootstraper peer.
        * The minimum amount of peers that must be gathered before they proceed to the training phase.
        * A boolean value indicating if you the system runs Synchronous Gradient Descent or Asynchronous Gradient Descent
*/
public class Model {
    public static IPLS ipls;
    public static String topic;
    public  static MultiLayerNetwork model;
    public static List<String> Bootstrapers = new ArrayList<>();
    public static boolean isBootstraper;
    public static String Path;

    public static INDArray GetDiff(INDArray Dumm,INDArray model){
        return model.sub(Dumm);
    }
    public static INDArray GetGrad(MultiLayerNetwork model){
        return model.getGradientsViewArray();
    }


    //For computational server
    public static BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    public static String taskReply;

    public static void remote_fit() throws Exception {
        IPFS ipfs = new IPFS(PeerData.Path);
        FileOutputStream fos = new FileOutputStream(topic);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject((INDArray)model.params());
        System.out.println("old params");
        System.out.println(model.params());
        oos.flush();
        oos.close();
        fos.close();
        ipfs.pubsub.pub("server",topic);
        taskReply = queue.take();
        System.out.println(taskReply);
        FileInputStream bis = new FileInputStream(topic);
        ObjectInput in = new ObjectInputStream(bis);
        model.setParams((INDArray) in.readObject());
        System.out.println(model.params());
        System.out.println("new params");
        in.close();
        bis.close();
    }

    public static void local_fit(DataSetIterator mni){
        model.fit(mni,1);
    }

    public static void parse_arguments(String[] args){

        Options options = new Options();
        Option path = new Option("a", "address", true, "The address of the IPFS API");
        path.setRequired(true);
        options.addOption(path);

        Option partitions = new Option("p", "partitions", true, "The number of partitions you want to partition the model");
        partitions.setRequired(true);
        options.addOption(partitions);

        Option minimum_partitions = new Option("mp", "minimum_partitions", true, "The minimum number of partitions a peer required to be responsible for");
        minimum_partitions.setRequired(true);
        options.addOption(minimum_partitions);


        Option is_bootstraper = new Option("b", "is_bootstraper", true, "If true then the process becomes bootstraper. Note that this process must be the first process of the system");
        is_bootstraper.setRequired(true);
        options.addOption(is_bootstraper);

        Option min_peers = new Option("n", "min_peers", true, "The minimum number of peers required to proceed to training phase");
        min_peers.setRequired(true);
        options.addOption(min_peers);

        // This variable is not of importance but it helps for the file names of the dataset.
        Option my_id_number = new Option("topic", "id_number",true,"The id number of the peer");
        my_id_number.setRequired(false);
        options.addOption(my_id_number);

        Option Bootstraper = new Option("bID", "Bootstraper_ID",true,"The bootstrapers hash id");
        my_id_number.setRequired(true);
        options.addOption(Bootstraper);

        Option IPNS = new Option("IPNS", "IPNS",true,"Provide Indirect communication, instead of using message passing protocols use IPFS file system capabilities");
        my_id_number.setRequired(false);
        options.addOption(IPNS);

        Option asynchronous = new Option("async", "Async",true,"If is true then you turn the protocol in asynchronous mod where you do not have to wait for others to complete the iteration");
        my_id_number.setRequired(false);
        options.addOption(asynchronous);

        DefaultParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            Path = cmd.getOptionValue("address");
            PeerData._PARTITIONS  = new Integer(cmd.getOptionValue("partitions"));
            PeerData._MIN_PARTITIONS = new Integer(cmd.getOptionValue("minimum_partitions"));
            isBootstraper = new Boolean(cmd.getOptionValue("is_bootstraper"));
            PeerData.isBootsraper = isBootstraper;

            PeerData.Min_Members = new Integer(cmd.getOptionValue("min_peers"));
            topic =  cmd.getOptionValue("id_number");
            Bootstrapers.add(cmd.getOptionValue("Bootstraper_ID"));

            if(cmd.getOptionValue("IPNS") != null){
                if(cmd.getOptionValue("IPNS").equals("true")){
                    PeerData.IPNS_Enable = true;
                }
            }
            if(cmd.getOptionValue("async") != null){
                if(cmd.getOptionValue("async").equals("true")){
                    PeerData.isSynchronous = false;
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        final int numRows = 28;
        final int numColumns = 28;
        int outputNum = 10; // number of output classes
        int batchSize = 100; // batch size for each epoch
        int rngSeed = 123; // random number seed for reproducibility
        int numEpochs = 15; // number of epochs to perform
        double rate = 0.0015; // learning rate
        int i;
        DataSetIterator lfw = null,mnistTrain = null,mnistTest = null;
        parse_arguments(args);
        try {

            /*
            FileInputStream fis = new FileInputStream("MNIST_Partitioned_Dataset/" + topic + "TrainDataset");
            ObjectInput fin = new ObjectInputStream(fis);
            mnistTrain = (DataSetIterator) fin.readObject();
            System.out.println(mnistTrain);
            System.out.println("OKKKK");

            fis = new FileInputStream("MNIST_Partitioned_Dataset/"+"MnistTest");
            fin = new ObjectInputStream(fis);
            mnistTest = (DataSetIterator) fin.readObject();

            */

        }
        catch (Exception e){
            System.out.print(e);

            System.out.println("Could not find iterator ");
            System.exit(-1);
        }
        /* =================================================================================== */

        ///                             CONFIGURE YOUR MODEL

        /* =================================================================================== */
        // NOTE!!! You can use whatever framework you want as long as for each training round you can receive the
        // new parameters of the model and also change the weights of the model.
        /*
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(rngSeed) //include a random seed for reproducibility
                .updater(new Sgd(0.1))
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .l2(rate * 0.005) // regularize learning model
                .list()
                .layer(new DenseLayer.Builder() //create the first input layer.
                        .nIn(numRows * numColumns)
                        .nOut(500)
                        .build())
                .layer(new DenseLayer.Builder() //create the second input layer
                        .nIn(500)
                        .nOut(100)
                        .build())
                .layer(new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD) //create hidden layer
                        .activation(Activation.SOFTMAX)
                        .nOut(outputNum)
                        .build())
                .build();

		*/
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(rngSeed) //include a random seed for reproducibility
                .updater(new Sgd(0.1))
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .l2(rate * 0.005) // regularize learning model
                .list()
                .layer(new DenseLayer.Builder() //create the first input layer.
                        .nIn(numRows * numColumns)
                        .nOut(20)
                        .build())
                .layer(new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD) //create hidden layer
                        .activation(Activation.SOFTMAX)
                        .nOut(outputNum)
                        .build())
                .build();


        model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(1));  //print the score with every iteration


        /* =================================================================================== */

        Sub SUB = new Sub(topic+"reply",Path,queue,true);
        SUB.start();



        Console console = new Console();
        console.start();


        INDArray TotalInput = Nd4j.zeros(model.params().length(),784);
        INDArray TotalLabels = Nd4j.zeros(model.params().length(),10);


        INDArray Dumm = Nd4j.zeros(1,model.params().length());
        INDArray gradient = Nd4j.zeros(1,80730);
        List<Double> arr = new ArrayList<>();
        List<Double> acc = new ArrayList<>();

        // CREATE IPLS OBJECT
        ipls = new IPLS(Path,"init_model_1",Bootstrapers,isBootstraper,model.params().length());

        // START INITIALIZATION PHASE
        System.out.println("Creating IPLS instance");
        //IF I AM BOOTSTRAPER THEN DO NOT CONTINUE
        if(isBootstraper){
            ipls.init();
            while (true){

            }
        }
        System.out.println("Initializing IPLS daemon");
        Light_IPLS_Daemon ipls_daemon = new Light_IPLS_Daemon(ipls);
        ipls_daemon.start();

        DataSet myData = new DataSet(TotalInput,TotalLabels);
        List<DataSet> Dlist = myData.asList();

        FileInputStream bis = new FileInputStream("MNIST_Partitioned_Dataset/"+topic + "TrainDataset");
        ObjectInputStream in = new ObjectInputStream(bis);
        DataSetIterator mni = (DataSetIterator)in.readObject();
        in.close();
        bis.close();

        FileOutputStream fos = new FileOutputStream("MNIST_Partitioned_Dataset/"+topic+"data");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(mni);
        oos.close();
        fos.close();
        System.out.println(model.params().length());
        int x = new Integer(topic);
        System.out.println(model.params());
        // TRAIN THE MODEL
        for(i = 0; i < 50; i++){
            /* ===================================== */
            // GET THE GLOBAL PARTITIONS FROM THE IPLS SYSTEM

            arr = ipls_daemon.Get_Partitions();

            /* ===================================== */

            for(int j = 0; j < model.params().length(); j++){
                Dumm.put(0,j,arr.get(j));
            }
            double parameters_norm = 0;
            for(int j = 0; j < model.params().length(); j++){
                parameters_norm += arr.get(j)*(j%50 + 1);
            }
            System.out.println("==========================");
            System.out.println("NORM : " + parameters_norm);
            System.out.println("==========================");
            model.setParams(Dumm);

            /*
            System.out.println("Evaluate model....");

            Evaluation eval = model.evaluate(mnistTest);
            System.out.println(eval.stats());
            System.out.println("****************Example finished********************");
            */
            // This method is going to train the model in only one processor so that you can run
            // many nodes when you are using only one pc and you want to experiment with IPLS.
            System.out.println("START TRAINING");
            //remote_fit();

            // In this function you get the W[i] - W[i-1] (difference of the locally updated model and the global model received from ipls.GetPartitions();)
            gradient = GetDiff(model.params(),Dumm);
            gradient = gradient.mul(1);
            //HERE GO UPDATE METHOD
            if(PeerData.isSynchronous){
                System.out.println("ITERATION : " + PeerData.middleware_iteration);
            }
            else{
                System.out.println("ITERATION : "+ i);
            }
            // UPDATE THE MODEL USING IPLS
            ipls_daemon.UpdateModel(Doubles.asList(gradient.getRow(0).toDoubleVector()));


            System.gc();
            System.runFinalization();


        }
        arr = ipls.GetPartitions();
        for(int j = 0; j < model.params().length(); j++){
            Dumm.put(0,j,arr.get(j));
        }
        model.setParams(Dumm);
        System.out.println("Evaluate model....");

        Evaluation eval = model.evaluate(mnistTest);
        System.out.println(eval.stats());
        System.out.println("****************Example finished********************");

        File f = new File("DataRecv"+topic);
        f.createNewFile();

        fos = new FileOutputStream("DataRecv"+topic);
        oos = new ObjectOutputStream(fos);
        oos.writeObject(PeerData.RecvList);
        oos.close();
        fos.close();

        f = new File("ChartData" + topic);
        f.createNewFile();

        fos = new FileOutputStream("ChartData" + topic);
        oos = new ObjectOutputStream(fos);
        oos.writeObject(acc);
        oos.close();
        fos.close();



    }

}
