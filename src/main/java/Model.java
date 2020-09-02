import io.ipfs.multihash.Multihash;
import jdk.nashorn.internal.ir.Labels;
import org.deeplearning4j.datasets.iterator.INDArrayDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
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
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.shade.guava.primitives.Doubles;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;





public class Model {
    //private static Logger log = LoggerFactory.getLogger(MNISTDoubleLayer.class);
    public static Multihash _Upload_File(List<Double> Weights, MyIPFSClass ipfsClass, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  ipfsClass.add_file(filename);
    }

    public static Triplet<Integer,String,String> Get_ACK(ByteBuffer rbuff, byte[] bytes_array){
        int Partition,i;
        short Hashsize,OriginPeerSize;
        String Hash,Origin_Peer;
        Hashsize = rbuff.getShort();
        OriginPeerSize = rbuff.getShort();
        Partition = rbuff.getInt();

        byte[] Id_array = new byte[bytes_array.length -  Integer.BYTES - 3*Short.BYTES - OriginPeerSize];
        byte[] Origin_array = new byte[bytes_array.length-  Integer.BYTES - 3*Short.BYTES - Hashsize];
        System.out.println(Hashsize);
        System.out.println(OriginPeerSize);

        System.out.println(bytes_array.length -  Integer.BYTES - 3*Short.BYTES - OriginPeerSize);

        for (i =  Integer.BYTES + 3*Short.BYTES; i < Integer.BYTES + 3*Short.BYTES + Hashsize; i++) {
            Id_array[i -  Integer.BYTES - 3*Short.BYTES] = bytes_array[i];
        }
        for (i = Integer.BYTES + 3*Short.BYTES + Hashsize; i < bytes_array.length; i++){
            Origin_array[i - Integer.BYTES - 3*Short.BYTES - Hashsize] = bytes_array[i];
        }

        Hash = new String(Id_array);
        System.out.println("HASH : " + Hash + " , " + Hash.length());
        Origin_Peer = new String(Origin_array);
        return new Triplet<>(Partition,Hash,Origin_Peer);
    }

    public static void main(String[] args) throws Exception {
        String Path = args[0];
        final int numRows = 28;
        final int numColumns = 28;
        int outputNum = 10; // number of output classes
        int batchSize = 60; // batch size for each epoch
        int rngSeed = 123; // random number seed for reproducibility
        int numEpochs = 15; // number of epochs to perform
        double rate = 0.0015; // learning rate
        int i;
        DataSetIterator mnistTrain = null,mnistTest = null;
        DataSet dataset = null;
        try {
            //Get the DataSetIterators:
            mnistTrain = new MnistDataSetIterator(batchSize, true, rngSeed);
            // System.out.println(mnistTrain.next(1).getLabels().toStringFull());
            //System.out.println(mnistTrain.next(1).getFeatures().toStringFull());
            mnistTest = new MnistDataSetIterator(batchSize, false, rngSeed);
        }
        catch (Exception e){

        }





        //log.info("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(rngSeed) //include a random seed for reproducibility
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .updater(new Sgd(0.1))
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

        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(1));  //print the score with every iteration

        /*
        // INITIALIZE ETH FILE
        List<Double> L = new ArrayList<>();
        FileOutputStream fos = new FileOutputStream("ETHModel");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        System.out.println(model.params().length());
        oos.writeObject(Doubles.asList(model.params().getRow(0).toDoubleVector()));
        oos.close();
        fos.close();
         */
        //System.out.println(mnistTrain.next(10).);
        /*
        // CHECKING SERIALIZATION
        IPFS ipfs = new IPFS(Path);
        MyIPFSClass ipfsClass = new MyIPFSClass(Path);
        System.out.println((short) ipfs.id().get("ID").toString().length());
        String decodedString = ipfsClass.Marshall_Packet(ipfs.id().get("ID").toString(),ipfs.id().get("ID").toString(),2,(short)3);
        byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
        int OriginPeerSize,PeerSize;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        String Renting_Peer = null,Origin_Peer= null;
        //Get Pid
        rbuff.getShort();
        System.out.println(Get_ACK(rbuff,bytes_array));


         */


        /*

        /// SIMPLE DEBUGING PROGRAM

        First_Application IPLS = new First_Application();
        IPLS.init(Path);

        List<Double> Param = new ArrayList<>();
        List<Double> Gradients = new ArrayList<>();

        for(i = 0; i < 100; i ++){
            Gradients.add(-1.0);
        }

        for(i = 0; i < 15; i++){
            Param = IPLS.GetPartitions();
            System.out.println(Param);
            Thread.sleep(2000);
            IPLS.UpdateGradient(Gradients);
            Thread.sleep(4000);
        }

         */



        INDArray TotalInput = Nd4j.zeros(10000,784);
        INDArray TotalLabels = Nd4j.zeros(10000,10);
        INDArray Input = Nd4j.zeros(3333,784);
        INDArray Output = Nd4j.zeros(3333,10);

        int counter = 0;
        DataSet Data;


        for(i = 0; i < 1000 && mnistTest.hasNext(); i++){
            Data = mnistTest.next();
            for(int j = 0; j < Data.getFeatures().rows(); j++){
                TotalInput.putRow(counter,Data.getFeatures().getRow(j));
                TotalLabels.putRow(counter,Data.getLabels().getRow(j));
                counter++;
            }
        }
        System.out.println(TotalLabels.rows());
        if(Path.equals("/ip4/127.0.0.1/tcp/5001")) {
            for(i = 0; i < 3333; i++){
                Input.putRow(i,TotalInput.getRow(i));
                Output.putRow(i,TotalLabels.getRow(i));
            }
        }
        else if(Path.equals("/ip4/127.0.0.1/tcp/5002")){
            for(i = 3333; i < 6666; i++){
                Input.putRow(i-3333,TotalInput.getRow(i));
                Output.putRow(i-3333,TotalLabels.getRow(i));
            }
        }
        else{
            for(i = 6666; i < 9999; i++){
                Input.putRow(i-6666,TotalInput.getRow(i));
                Output.putRow(i-6666,TotalLabels.getRow(i));
            }
        }
        //log.info("Train model....");
        Pair<Gradient, INDArray> temp_gradient;
        INDArray gradient;
        INDArray Model = Nd4j.zeros(1,model.params().length());

        //System.out.println(mnistTrain);
        //System.out.println(mnistTest.next().getFeatures().length());

        IPLS ipls = new IPLS();
        ipls.init(Path);
        List<Double> arr = new ArrayList<>();

        INDArray Dumm = Nd4j.zeros(1,443610);



        System.out.println(model.params());
        for(i = 0; i < 250; i++){
            arr = ipls.GetPartitions();
            for(int j = 0; j < model.params().length(); j++){
               Dumm.put(0,j,arr.get(j));
            }
            model.setParams(Dumm);
            //System.out.println(arr.size());
            //HERE MUST GO GET PARTITIONS
            //model.setParams(UPDATED_WEIGHTS);
            model.fit(Input,Output);
            System.out.println(model.params());
            //Thread.sleep(5000);
            gradient = model.getGradientsViewArray();
            //HERE GO UPDATE METHOD
            System.out.println("Iter : " + i);
            ipls.UpdateGradient(Doubles.asList(gradient.getRow(0).toDoubleVector()));
            //Thread.sleep(2000);

        }


        //log.info("Evaluate model....");
        Evaluation eval = model.evaluate(mnistTest);
        System.out.println(eval.stats());
        //log.info(eval.stats());
        //log.info("****************Example finished********************");





    }

}
