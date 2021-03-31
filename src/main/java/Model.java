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

        Option my_id_number = new Option("topic", "id_number",true,"The id number of the peer");
        my_id_number.setRequired(false);
        options.addOption(my_id_number);

        Option Bootstraper = new Option("bID", "Bootstraper_ID",true,"The bootstrapers hash id");
        my_id_number.setRequired(true);
        options.addOption(Bootstraper);


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


            FileInputStream fis = new FileInputStream(topic + "TrainDataset");
            ObjectInput fin = new ObjectInputStream(fis);
            mnistTrain = (DataSetIterator) fin.readObject();
            System.out.println(mnistTrain);
            System.out.println("OKKKK");
            
            fis = new FileInputStream("MnistTest");
            fin = new ObjectInputStream(fis);
            mnistTest = (DataSetIterator) fin.readObject();
           
            
            
        }
        catch (Exception e){
        	System.out.print(e);
        	
            System.out.println("Could not find iterator ");
            System.exit(-1);
        }

        //MNiST : 1000
        //lfw : 1480
        //log.info("Build model....");
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
                        .nOut(100)
                        .build())
                .layer(new DenseLayer.Builder() //create the second input layer
                        .nIn(100)
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


        Sub SUB = new Sub(topic+"reply",Path,queue,true);
        SUB.start();



        INDArray TotalInput = Nd4j.zeros(7495,784);
        INDArray TotalLabels = Nd4j.zeros(7495,10);
        INDArray batchIn = Nd4j.zeros(100,784);
        INDArray batchOut = Nd4j.zeros(100,10);

        INDArray Input;
        INDArray Output;
        INDArray Input2 = null;
        INDArray Output2 = null;
        int counter = 0;
        DataSet Data;

        INDArray Dumm = Nd4j.zeros(1,80730);
        INDArray gradient2 = Nd4j.zeros(1,80730);
        INDArray gradient = Nd4j.zeros(1,80730);
        Random rand = new Random();
        List<Double> arr = new ArrayList<>();
        List<Double> acc = new ArrayList<>();

       ipls = new IPLS();
        
        ipls.init(Path,Bootstrapers,isBootstraper);
        if(isBootstraper){
            while (true){

            }
        }

        DataSet myData = new DataSet(TotalInput,TotalLabels);
        List<DataSet> Dlist = myData.asList();
        DataSetIterator mni = new ListDataSetIterator(Dlist,100);

        FileOutputStream fos = new FileOutputStream(topic+"data");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(mni);
        oos.close();
        fos.close();
        System.out.println(model.params().length());
        int x = new Integer(topic);
        System.out.println(model.params());
        for(i = 0; i < 50; i++){
            arr = ipls.GetPartitions();
            for(int j = 0; j < model.params().length(); j++){
               Dumm.put(0,j,arr.get(j));
            }

            model.setParams(Dumm);
           // System.out.println(model.params());
            //System.out.println(Dumm);

            if(x == 0) {
                System.out.println("Evaluate model....");

                Evaluation eval = model.evaluate(mnistTest);
                System.out.println(eval.stats());
                //System.out.println(eval.accuracy());
                //acc.add(eval.accuracy());
                System.out.println("****************Example finished********************");
            }
            //remote_fit();
            Thread.sleep(3000);
            gradient = GetDiff(model.params(),Dumm);
            gradient = gradient.mul(1);
            //HERE GO UPDATE METHOD
            System.out.println("ITERATION : " + PeerData.middleware_iteration);
            ipls.UpdateGradient(Doubles.asList(gradient.getRow(0).toDoubleVector()));
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
