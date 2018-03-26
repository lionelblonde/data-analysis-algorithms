# Data Analysis Algorithms

## Repository content

This repository aggregates Scala implementations of Machine Learning algorithms, leveraging the 
[`spark.ml`](http://spark.apache.org/docs/latest/ml-guide.html) and
[`spark.mllib`](http://spark.apache.org/docs/latest/mllib-guide.html#data-types-algorithms-and-utilities) libraries.
The latter, most recent among the two, provides a high-level API built on top of
[*DataFrames*](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes), while the
former relies on the legacy API built on top of 
[*Resilient Distributed Datasets (RDDS)*](http://spark.apache.org/docs/latest/rdd-programming-guide.html).

Those implementations are meant to be encapsulated into a fat jar to act as an imported library in
[Zeppelin](https://zeppelin.apache.org/) notebook workflows.
The provided `build.sbt` file aims to enable the creation of this fat
jar file.

The I/O sections (pulling from data sources, pushing to data destinations) of the algorithms has
been factorized out of the body of each algorithms. Wrapping functions for [Graphana](https://grafana.com/) 
and HDFS are also provided.

## Fat jar creation

* Go to the root level of the cloned Scala project;
* Execute the command `sbt reload`, which as the name states reloads the build and recompiles it;
* Execute the command `sbt assembly`, which creates a `.jar` file at
  `$PROJECT_HOME/target/scala-2.11/`, with the name specified in `build.sbt`.

The `.jar` then simply needs to be copied at the desired location on the machine hosting the
Zeppelin instance.

## Covered algorithms

Batch algorithms are implemented with
[`spark.ml`](http://spark.apache.org/docs/latest/ml-guide.html).
Streaming algorithms are implemented with 
[`spark.mllib`](http://spark.apache.org/docs/latest/mllib-guide.html#data-types-algorithms-and-utilities).

**Batch algorithms**: decision tree classifier, batch k-means.  
**Streaming algorithms**: anomaly detection via running average, streaming k-means.

*Pending additions*: decision tree regressor, PCA.
