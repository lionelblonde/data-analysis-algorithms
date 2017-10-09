import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineStage, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler

case class BatchKmeansConfig(
  k: Int = 10,
  ks: Range = 0 to 0,
  seed: Long = 1L,
  tol: Double = 1E-4,
  maxIter: Int = 500,
  features: Array[String]
)

class BatchKmeans() {

  def fit(trainingData: DataFrame, config: BatchKmeansConfig): (PipelineModel, VectorAssembler, KMeansModel, Double) = {

    // Pull features out of the config
    val features = config.features

    // Create a buffer that will contain the different stages of our pipeline
    val stages = new scala.collection.mutable.ArrayBuffer[PipelineStage]()

    // Create the proper input structure to suit the spark.ml API
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    // Transform the df for the error computation
    val assembledDf = assembler.transform(trainingData)

    stages += assembler

    // Create the K-means model
    val kmeans = new KMeans()
      .setK(config.k)
      .setMaxIter(config.maxIter)
      .setSeed(config.seed)
      .setTol(config.tol)
      .setFeaturesCol("features")
    // kmeans is unsupervised, there is no setLabelCol like for LinearRegressionModel

    stages += kmeans

    // Chain all the stages in a Pipeline
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Train the full pipeline
    val pipelineModel = pipeline.fit(trainingData)

    // Retrieve the trained kmeans model (trained when training the pipeline at previous line)
    val kmeansStageIndex = stages.indexOf(kmeans)
    val kmeansModel = pipelineModel.stages(kmeansStageIndex).asInstanceOf[KMeansModel]

    // Compute the error on the training data (Careful: need assembled data)
    // Clustering evaluation metric: Within Set Sum of Squared Errors
    val trainingError = kmeansModel.computeCost(assembledDf) / (assembledDf.count().toDouble)

    (pipelineModel, assembler, kmeansModel, trainingError)
  }

  def multiFit(trainingData: DataFrame, config: BatchKmeansConfig):
      (Array[(Int, PipelineModel)], Array[(Int, VectorAssembler)], Array[(Int, KMeansModel)], Array[(Int, Double)]) = {

    // Pull features out of the config
    val features = config.features

    val pipelineModels = new scala.collection.mutable.ArrayBuffer[(Int, PipelineModel)]()
    val assemblers = new scala.collection.mutable.ArrayBuffer[(Int, VectorAssembler)]()
    val kmeansModels = new scala.collection.mutable.ArrayBuffer[(Int, KMeansModel)]()
    val trainingErrors = new scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    config.ks.foreach { e =>
      println(s"Started processing with k=${e}.")
      val outputs = fit(trainingData, BatchKmeansConfig(k=e, features=features))
      pipelineModels += ((e, outputs._1))
      assemblers += ((e, outputs._2))
      kmeansModels += ((e, outputs._3))
      trainingErrors += ((e, outputs._4))
      println(s"Done processing with k=${e}.")
    }

    // Seq is convertible in DataFrame via .toDF(), while Array is not (but a .toSeq.toDF() would!)
    (pipelineModels.toArray, assemblers.toArray, kmeansModels.toArray, trainingErrors.toArray)
  }

}
