# My exercices for Rock the JVM's Spark Essentials with Scala course

Initial commit copied from [spark-essentials](https://github.com/rockthejvm/spark-essentials)

[Rock the JVM's Spark Essentials with Scala](https://rockthejvm.com/course/spark-essentials)

## How to install

- install Docker
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- in a terminal window, navigate to the folder where you downloaded this repo and run `docker-compose up` to build and start the PostgreSQL container - we will interact with it from Spark
- in another terminal window, navigate to `spark-cluster/` and build the Docker-based Spark cluster with
```
chmod +x build-images.sh
./build-images.sh
```
- when prompted to start the Spark cluster, go to the `spark-cluster` folder and run `docker-compose up --scale spark-worker=3` to spin up the Spark containers
