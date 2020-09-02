# Java IPLS Client



> A Java client for the IPFS http api
### IPFS installation

#### Command line

Download ipfs from https://dist.ipfs.io/#go-ipfs and run with `ipfs daemon --enable-pubsub-experiment`

## Usage

Create an IPFS instance with:
```Java
IPLS ipls = new IPLS();
```

To initialize and get model partition responsibilities select IPFS Path (in the example is "/ip4/127.0.0.1/tcp/5001") and use:
```Java
ipls.init("/ip4/127.0.0.1/tcp/5001");
```


Then to Update model Gradients after an iteration or a set of iterations use:
```Java
ipls.UpdateGradient(List<Double> Gradients);
```

To get a new the updated model from the distributed shared memory use:
```Java
 List<Double> Parameters = ipls.GetPartitions();
```

## License

[Παπαγιώργη!](LICENSE)
