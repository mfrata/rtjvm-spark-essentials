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



# My Notes (mfrata)

## First principles

* Spark -> Unified computing engine for distribuited data processing
* The most popular one
* Created at 2009 Matei Zaharia et al
* Spark Architecture:

| layer | components |
|---------------- | ------------------------------- |
| applications    | Streaming, ML, GraphX, Others   |
| high-level APIs | DataFrames, DataSets, Spark SQL |
| low level APIs  | RDDs, Distribuited collections  |

## DataFrames

Distribuited collections of Rows that follows a schema (runtime)

* immutable
* Created via transformations
  * narrow -> operation done in the scope of one partition (map, row by row operation)
  * wide -> !! more than one partition (sort, sum, joins)
* shuffle -> move data among nodes
* lazy evaluation (waits until a action is called to start the computation)
* logical plan (graph +narrow/wide transforms) -> physical plan -> execution on the nodes
