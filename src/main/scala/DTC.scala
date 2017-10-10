import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineStage, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler, VectorSlicer, StandardScaler}

case class DTCConfig(
  // Hyperparameters
  maxBins: Int = 64,
  maxDepth: Int = 15,
  minInstancesPerNode: Int = 1,
  seed: Long = 1L,
  // Features (numeric, symbolic) and label
  numFeatures: Array[String],
  symFeatures: Array[String],
  label: String,
  // Data split
  trainTestSplit: Double = 0.7
)

class DTC() {
  def fit(df: DataFrame, config: DTCConfig):
        (PipelineModel, DecisionTreeClassificationModel, MulticlassClassificationEvaluator, Double, DataFrame)  = {

     // The API enables the use of symbolic values (as opposed to numeric ones) for the decision tree classifier algorithm.
     // This functionality is ensured by the two classes: VectorIndexer, which indexes the features, and StringIndexer, which
     // indexes the labels. As the name states, the features have to already be in Vector form, in a single collumn called "features".
     // This is not the case however with our freshly created dataframe.
     // We can therefore not use a VectorIndexer to deal with symbolic values in the features.
     // One solution would be to use a VectorAssember to assemble the features into a vector, then use the VectorIndexer on it.
     // This is however not possible since VectorAssembler only works with numeric feature...
     // We will therefore proceed the other way around: index the features individually, then assemble them.

     // We could cast all the columns as Stringtype, then use VectorIndexer to automatically identify categorical features, and index them.
     // This identification relies on the number of different values a feature takes in the data (maximum set in .setMaxCategories).
     // A feature will be considered categorical if the number of values is lower than this threshold (each value is then a category).
     // This is quite an unreliable metric to assess the nature of the features, hence the alternative proposed here.


     // Split the DataFrame into training and test sets
     val Array(trainingData, testData) = df.randomSplit(Array(config.trainTestSplit, 1 - config.trainTestSplit))

     // Add the "_index" suffix to all symbolic feature names and merge both previous lists
     val myIndexedFeatures = config.symFeatures.map(feature => s"${feature}_index") ++ config.numFeatures

     // Create a buffer that will contain the different stages of our pipeline
     val stages = new scala.collection.mutable.ArrayBuffer[PipelineStage]()

     // Create a StringIndexer for each of the symbolic features we selected earlier
     val indexers: Array[org.apache.spark.ml.PipelineStage] = config.symFeatures.map { feature =>
       new StringIndexer()
         .setInputCol(feature)
         .setOutputCol(s"${feature}_index")
         .setHandleInvalid("skip")
     }

     // Add the indexer stages to the PipelineStage buffer
     indexers.foreach(indexer => stages += indexer)

     // Assemble the newly indexed symbolic features with the numeric features in a single feature vector (Vector type)
     // To use a VectorAssembler, it is necessary that the input features are all of type DoubleType,
     // Hence the necessary preliminary indexers to index (associate with a Double) the symbolic features.
     val assembler = new VectorAssembler()
       .setInputCols(myIndexedFeatures)
       .setOutputCol("rawFeatures")

     stages += assembler

     // Slice the features (dimensionality reduction, feature extractor)
     // VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the original features.
     // Although we specified the columns to assemble to the assembler, the training and testing data will still contain every feature.
     // So even though only the ones specified will be instrumental in the learning process, they will still be there when transforming
     // (with .transform()) a DataFrame that has all the columns.
     // The slicer leaves those columns and makes sure the symbolic columns are all replaced by their indexed counterparts
     val slicer = new VectorSlicer()
       .setInputCol("rawFeatures")
       .setOutputCol("slicedFeatures")
       .setNames(myIndexedFeatures)

     stages += slicer

     // Scale the features
     val scaler = new StandardScaler()
       .setInputCol("slicedFeatures")
       .setOutputCol("features")
       .setWithStd(true)
       .setWithMean(true)

     stages += scaler

     // Index labels, adding metadata to the label column
     val labelIndexer = new StringIndexer()
       .setInputCol(config.label)
       .setOutputCol("indexedLabel")
       .fit(df) // Fit on whole dataset to include all labels in index

     stages += labelIndexer

     // Create the Decision Tree model
     val dtc = new DecisionTreeClassifier()
       .setLabelCol("indexedLabel")
       .setFeaturesCol("features")
       .setMaxBins(config.maxBins)
       .setMaxDepth(config.maxDepth)
       .setMinInstancesPerNode(config.minInstancesPerNode)
       .setSeed(config.seed)

     stages += dtc

     // Convert indexed labels back to original labels
     val labelConverter = new IndexToString()
       .setInputCol("prediction")
       .setOutputCol("predictedLabel")
       .setLabels(labelIndexer.labels) // this line would have failed if we had not fit the labelIndexer

     stages += labelConverter

     // Chain indexers, tree and assembler in a Pipeline
     val pipeline = new Pipeline().setStages(stages.toArray)

     // Train the full pipeline
     val pipelineModel = pipeline.fit(trainingData)

     // Retrieve the trained DTC model (trained when training the pipeline at previous line)
     val dtcStageIndex = stages.indexOf(dtc)
     val dtcModel = pipelineModel.stages(dtcStageIndex).asInstanceOf[DecisionTreeClassificationModel]

     // Make predictions on the training data
     val predictions = pipelineModel.transform(trainingData)

     // Build a multiclass classification evaluator
     val evaluator = new MulticlassClassificationEvaluator()
       .setLabelCol("indexedLabel")
       .setPredictionCol("prediction")
       .setMetricName("accuracy")

     // Compute training error
     val accuracy = evaluator.evaluate(predictions)
     val trainingError = 1.0 - accuracy

     (pipelineModel, dtcModel, evaluator, trainingError, testData)
   }
}
