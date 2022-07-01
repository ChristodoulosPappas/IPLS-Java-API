# Java IPLS Middleware



### IPFS installation

To run several IPLS nodes and experiment with the IPLS middleware on your computer, you must install 
the IPFS. Instructions on downloading IPFS are given in 
https://docs.ipfs.io/install/ipfs-desktop/#ubuntu. Next, you must set up as many IPFS daemons as many 
IPLS participants and IPFS storage nodes you want to have in our system. For example, if you want to
run an experiment with 10 IPLS participants (including the IPLS bootstrapper/Directory service) 
and 5 IPFS storage nodes, then you need to set up 15 IPFS daemons. You can set up several IPFS nodes 
locally by following the guidelines of https://stackoverflow.com/questions/40180171/how-to-run-several-ipfs-nodes-on-a-single-machine. 
Then, you must set up a private IPFS network and add all nodes just created in that private network. 
Follow the guidelines of https://medium.com/@s_van_laar/deploy-a-private-ipfs-network-on-ubuntu-in-5-steps-5aad95f7261b. 
When IPFS nodes setup is finished, then start running them by typing "ipfs daemon --enable-pubsub-experiment"
in your terminal. 

The easiest way to run the IPLS middleware is by using your IDE, but you can also convert the middleware
into a .jar program. Moreover, you should download the IPLS python API from 
https://github.com/ChristodoulosPappas/IPLS-python-API.

## Starting up IPLS middleware
To conduct various experiments with IPLS, first, you have to start the IPLS middleware. That
is done by running the Middleware.java, which takes seven parameters. Those parameters are: 


* **The port number (-p)**, the IPLS middleware listens and receives tasks from the application.
* **The number of partitions (-pa)** the model will get segmented. Note that all IPLS participants must segment the model in the same number of partitions.
* **The minimum number of partitions (-mp)** a peer should be responsible for. If a peer is not responsible for any partition (e.g, trainers) then -mp = 0.
* **The number of IPLS participants (-n)** must enter the project before it begins. For example, if -n=3, the Directory service (or IPLS bootstrapper) waits until 3 IPLS participants enter the project.
* **Indicator of indirect communication (-i)**. If -i=0, then all IPLS participants must communicate directly. Else if -i=1, all IPLS participants must communicate indirectly using the decentralized storage network.
* **The training time (-training)**. This is the time IPLS trainers have in order to finish their training. For example, if -training=10, then IPLS trainers have 10 seconds to train the model and upload their gradients.
* **Indicator if merge and download is used (-aggr)**. If -aggr=0, then aggregators do not use merge and download. Until now, merge and download is an experimental feature; thus, it is highly recommended not to be used.

For example,  **-p 12000 -pa 3 -mp 0 -n 3 -i 1 -training 10 -aggr 0** is valid example of parameters 
assignment.  The example indicates that an IPLS middleware listens to the port 12000, segments the 
model in 3 partitions, is responsible for no partition, communicates indirectly as any other IPLS 
participant, has to train its model in 10 seconds, and merge and download is not used. To start an 
IPFS node, someone simply needs to start the **Decentralized_Storage.java** class, with program parameter
the IPFS address API of the IPFS daemon that is going to become the IPFS storage node 
(e.g., **/ip4/127.0.0.1/tcp/5006**).

## IPLS Java API

Create an IPFS instance with:
```Java
IPLS ipls = new IPLS("/ip4/127.0.0.1/tcp/5001",String Path_of_model_file,List<String> Bootstrapers,boolean is_bootstraper,int model_size);
```
The IPLS constructor takes as input the IPFS API address of the IPFS daemon that the IPLS middleware is going 
to communicate with, the path of the file that the initial model is stored, the list of the IPLS 
bootstrappers, a flag indicating if the IPLS middleware is going to be bootstrapper or not, and finally
the model size.


To initialize the IPLS daemon and get model partition responsibilities use:
```Java
ipls.init();
```


Then to Update model Gradients after an iteration or a set of iterations use:
```Java
ipls.UpdateModel(List<Double> Gradients);
```

To get a new the updated model from the distributed shared memory use:
```Java
 List<Double> Parameters = ipls.GetPartitions();
```

## IPLS Python API
Using the IPLS python API, someone can create their own IPLS application. The API consists simply of 
the following two methods:

```Python
init(api_ipfs_address,model_file,bootstrappers,model_size,model,is_bootstrapper)
```
The init method is used to initialize the IPLS middleware and ask it to join the IPLS project. This 
method is given as input the IPFS address API of the IPFS daemon, which the IPLS middleware will 
communicate, the model\_file where the initial model parameters are stored, and the list of the IPLS 
bootstrappers (commonly, there is only one bootstrapper). In addition, it also takes as input the 
size of the model, the model (commonly the compiled Keras or TensorFlow model), and a flag whether 
the IPLS middleware will act as a bootstrapper or not. The init method, in reality does the exact same 
think as the java init.


```Python
fit(model,X,Y,batch_size,iter)
```
This method takes as input the Keras model, the local data of the node X, the corresponding labels Y, 
the batch size, and the number of iterations the IPLS participant will run. This method should be 
seen as the fit() method Keras or TensorFlow have. The difference is that inside IPLS fit, the 
actual IPLS API is used to train the model in a Federated learning fashion.


Note that the API is a class, so a constructor is needed. The object's constructor takes only one input:
the port the IPLS middleware is listening to. An example of how to write an IPLS application is given
in ipls_example.py (https://github.com/ChristodoulosPappas/IPLS-python-API/blob/main/ipls_example.py).


## Running IPLS locally

To run IPLS locally, first of all, someone has to start all the IPFS daemons, then start the
IPLS middlewares and the IPFS storage nodes. Each middleware and IPFS storage node must be assigned
to only one IPFS daemon. This assignment is done by using the IPFS API addresses. Then first run
the IPLS application for the bootstrapper and afterward run all the other IPLS applications. For 
example, if someone wants to run the ipls\_example.py, with 3 IPLS participants, one bootstrapper,
and 2 IPFS storage nodes, he should start 6 IPFS daemons. Then start 4 IPLS middlewares with the 
following parameters:

* -p 12000 -pa 3 -mp 0 -n 3 -i 1 -training 10 -aggr 0 (Bootstrappers Middleware)
* -p 12001 -pa 3 -mp 1 -n 3 -i 1 -training 10 -aggr 0
* -p 12002 -pa 3 -mp 1 -n 3 -i 1 -training 10 -aggr 0
* -p 12003 -pa 3 -mp 1 -n 3 -i 1 -training 10 -aggr 0


Then start 2 IPFS storage nodes (e.g., start 2 different Decentralized\_Storage processes with parameters ip4/127.0.0.1/tcp/5005 and ip4/127.0.0.1/tcp/5006). Finally run the IPLS application by executing:

* **python3 ipls_example.py 0 4 /ip4/127.0.0.1/tcp/5001 1 My_IPFS_ID**. This app is going to communicate with
the IPLS middleware that listens to the port 12000
* **python3 ipls_example.py 1 4 /ip4/127.0.0.1/tcp/5002 0 Bootstrapper_ID**. This app is going to communicate with
  the IPLS middleware that listens to the port 12001
* **python3 ipls_example.py 2 4 /ip4/127.0.0.1/tcp/5003 0 Bootstrapper_ID**. This app is going to communicate with
  the IPLS middleware that listens to the port 12002
* **python3 ipls_example.py 3 4 /ip4/127.0.0.1/tcp/5004 0 Bootstrapper_ID**. This app is going to communicate with
  the IPLS middleware that listens to the port 12003

Where Bootstrapper_ID is the IPFS id of the bootstrapper (e.g, 12D3KooWCyJZJphf9
z1Dbd2sJ\\KYc11PVV2RBVA9HQjNz26oMANgR).


## Useful Notations
* IPLS (Inter-Planetary Learning System) is a framework for decentralized Federated Learning.
For more information you can read the whitepaper given this link : https://arxiv.org/pdf/2101.01901.pdf .

* If you want to see how to use the java API check the Model.java located at the /src/main/java. 

* For a quick explanation of the protocol check the given pdf file.
## License
[MIT](LICENSE)
