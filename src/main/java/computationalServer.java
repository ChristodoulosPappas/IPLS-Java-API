import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.json.JSONObject;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class computationalServer {
    public  static MultiLayerNetwork model;

    public static void fit(String topic,IPFS ipfs) throws Exception {
        DataSetIterator mni;
        FileInputStream bis = new FileInputStream(topic);
        ObjectInput in = new ObjectInputStream(bis);
        INDArray params = (INDArray) in.readObject();
        System.out.println(params.getRow(0).length());
        model.setParams(params);
        System.out.println(model.params());
        bis.close();
        in.close();
        bis = new FileInputStream(topic + "data");
        in = new ObjectInputStream(bis);
        mni = (DataSetIterator)in.readObject();
        in.close();
        bis.close();
        model.fit(mni,1);
        System.out.println(model.params());
        System.out.println("========");
        FileOutputStream fos = new FileOutputStream(topic);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(model.params());
        oos.flush();
        oos.close();
        fos.close();
        ipfs.pubsub.pub(topic+"reply","ok");
    }


    public static void main(String args[]) throws Exception {
        String path = args[0];
        IPFS ipfs = new IPFS(path);
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
        String task,encoded,decodedString;
        byte[] decodedBytes;
        final int numRows = 28;
        final int numColumns = 28;
        int outputNum = 10; // number of output classes
        int batchSize = 60; // batch size for each epoch
        int rngSeed = 123; // random number seed for reproducibility
        int numEpochs = 15; // number of epochs to perform
        double rate = 0.0015; // learning rate
        int i;
        //log.info("Build model....");
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

        model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(1));  //print the score with every iteration
        Map<String,Integer> Pmap = new HashMap<>();
        
        Sub SUB = new Sub("server",path,queue,true);
        SUB.start();
        int num = 0;
        while(true){
            task =  queue.take();
            JSONObject obj = new JSONObject(task);

            encoded = (String) obj.get("data");
            decodedBytes = Base64.getUrlDecoder().decode(encoded);
            decodedString = new String(decodedBytes);
            if(Pmap.containsKey(decodedString)){
                num = Pmap.get(decodedString);
                Pmap.put(decodedString,num++);
            }
            else{
                Pmap.put(decodedString,1);
            }

            System.out.println(decodedString);
            fit(decodedString,ipfs);
            System.out.println(Pmap);
        }
    }

}
