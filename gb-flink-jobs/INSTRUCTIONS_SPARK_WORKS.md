To run SparkWorks use case with TornadoVM Flink.


### 1. Build TornadoVM and Flink-TornadoVM 


```bash
$ git clone git@github.com:beehive-lab/flink-tornado-internal.git
$ cd flink-tornado-internal
$ # Checkout the flinkTornadoIntegration branch
$ git checkout flinkTornadoIntegration
$ #setup tornado variables and then build Flink-Tornado
$ mvn -T 1.5C install -Drat.skip=true -DskipTests 
```

###### Setup Apache-Flink and TornadoVM

Apache Flink integration for TornadoVM includes a set of utility commands for quick setups.
First export `TORNADO_ROOT` variable to your TornadoVM installation directory. For example:

```bash
$ export TORNADO_ROOT=/home/user/tools/tornado/
```

Then run the `flink-management` tool located in the `scripts` directory.

```bash
$ ./scripts/flink-management deploy-tornado
```

This scripts deploys your TornadoVM installation into the Flink distributed environment.


###### Completing the configuration

1. Export the following variables


```bash
   export TORNADO_BASE=<path/to/>flink-tornado-internal/build-target/lib/tornado/
   export PATH="${PATH}:${TORNADO_BASE}/bin/"
   export TORNADO_SDK=${TORNADO_BASE}
```

2. Execute `tornado --printFlags` and copy all flags and add the following two:
  
  ```bash
  -Dtornado.ignore.nullchecks=True` and `-Dtornado=True
  ``` 
            

3. Then add the following variables in the `flink-conf.yaml` located in `<path/to/>flink-tornado-internal/build-target/conf/flink-conf.yaml`:

```bash
    ## Set the Java Home
    env.java.home: <path/to/java/>

    ## Set the JVM arguments
    env.java.opts: 
```

4. Start the Flink-cluster: 

```bash
$ ./scripts/flink-management start
```


### 2. Compile and Run SparkWorks use case


```bash
$ git clone git@github.com:E2Data/e2data-gb-peel.git
$ git checkout flink-1.11.1
$ cd gb-flink-jobs
$ mvn clean package
```

To run:

```bash
$ ~/flink-tornado-internal/build-target/bin/flink run -c net.sparkworks.batch.SparkWorksAllReduce target/gb-flink-jobs-1.0-SNAPSHOT.jar --filename /home/juan/manchester/e2data/e2data-gb-peel/gb-bundle/src/main/resources/datasets/dataset_2000_1_5 --output /tmp/bar2
```
