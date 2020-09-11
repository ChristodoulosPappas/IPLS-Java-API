import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.shade.guava.primitives.Doubles;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class MnistTest {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
            String Path = args[0];
            List<String> Bootstrapers = new ArrayList<>();
            boolean isBootstraper;
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


            Class c = Class.forName("Model");
            System.out.println(c.getClass().getCanonicalName());
            for(i = 1; i < args.length-1; i++){
                Bootstrapers.add(args[i]);
            }

            if(args[args.length-1].equals("true")){
                System.out.println("Starting Bootstraper ...");
                isBootstraper = true;
            }
            else{
                isBootstraper = false;
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
            // INITIALIZE ETH FILE
            List<Double> L = new ArrayList<>();
            FileOutputStream fos = new FileOutputStream("ETHModel");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            System.out.println(model.params().length());
            oos.writeObject(Doubles.asList(model.params().getRow(0).toDoubleVector()));
            oos.close();
            fos.close();
    }

}
