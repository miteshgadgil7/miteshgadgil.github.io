# coding: utf-8
# Name: Mitesh Gadgil
# Email: mgadgil@ucsd.edu
# PID: A53095373
from pyspark import SparkContext
sc = SparkContext()
# Your program here

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


CoverTypes={1.0: 'Spruce/Fir',
            2.0: 'Lodgepole Pine',
            3.0: 'Ponderosa Pine',
            4.0: 'Cottonwood/Willow',
            5.0: 'Aspen',
            6.0: 'Douglas-fir',
            7.0: 'Krummholz' }

# Define the feature names
cols_txt="""
Elevation, Aspect, Slope, Horizontal_Distance_To_Hydrology,
Vertical_Distance_To_Hydrology, Horizontal_Distance_To_Roadways,
Hillshade_9am, Hillshade_Noon, Hillshade_3pm,
Horizontal_Distance_To_Fire_Points, Wilderness_Area (4 binarycolumns), 
Soil_Type (40 binary columns), Cover_Type
"""

# Break up features that are made out of several binary features.
from string import split,strip
cols=[strip(a) for a in split(cols_txt,',')]
colDict={}
for a in cols:
	colDict[a]=[a]
colDict['Soil_Type (40 binary columns)'] = ['ST_'+str(i) for i in range(40)]
colDict['Wilderness_Area (4 binarycolumns)'] = ['WA_'+str(i) for i in range(4)]
Columns=[]
for item in cols:
    Columns=Columns+colDict[item]

# Read the file into an RDD
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/covtype/covtype.data'
inputRDD=sc.textFile(path)
inputRDD.first()

# Transform the text RDD into an RDD of LabeledPoints
Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')])\
				.map(lambda x: LabeledPoint(x[-1],Vectors.dense(x[:-1])))

# ### Making the problem binary
# 
# The implementation of BoostedGradientTrees in MLLib supports only binary problems. the `CovTYpe` problem has
# 7 classes. To make the problem binary we choose the `Lodgepole Pine` (label = 2.0). We therefor transform the dataset to a new dataset where the label is `1.0` is the class is `Lodgepole Pine` and is `0.0` otherwise.

Label=2.0
Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')])\
				.map(lambda V:LabeledPoint(1.0 if V[-1]== Label else 0.0, V[:-1])).cache()

# ### Reducing data size
# In order to see the effects of overfitting more clearly, we reduce the size of the data by a factor of 10

(trainingData,testData)=Data.randomSplit([0.7,0.3],seed = 255)
trainingData.cache()
testData.cache()

# ### Gradient Boosted Trees
# 
# * Following [this example](http://spark.apache.org/docs/latest/mllib-ensembles.html#gradient-boosted-trees-gbts) from the mllib documentation
# 
# * [pyspark.mllib.tree.GradientBoostedTrees documentation](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.tree.GradientBoostedTrees)
# 
# #### Main classes and methods
# 
# * `GradientBoostedTrees` is the class that implements the learning trainClassifier,
#    * It's main method is `trainClassifier(trainingData)` which takes as input a training set and generates an instance of `GradientBoostedTreesModel`
#    * The main parameter from train Classifier are:
#       * **data** – Training dataset: RDD of LabeledPoint. Labels should take values {0, 1}.
#       * categoricalFeaturesInfo – Map storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
#       * **loss** – Loss function used for minimization during gradient boosting. Supported: {“logLoss” (default), “leastSquaresError”, “leastAbsoluteError”}.
#       * **numIterations** – Number of iterations of boosting. (default: 100)
#       * **learningRate** – Learning rate for shrinking the contribution of each estimator. The learning rate should be between in the interval (0, 1]. (default: 0.1)
#       * **maxDepth** – Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 3)
#       * **maxBins** – maximum number of bins used for splitting features (default: 32) DecisionTree requires maxBins >= max categories
#       
#       
# * `GradientBoostedTreesModel` represents the output of the boosting process: a linear combination of classification trees. The methods supported by this class are:
#    * `save(sc, path)` : save the tree to a given filename, sc is the Spark Context.
#    * `load(sc,path)` : The counterpart to save - load classifier from file.
#    * `predict(X)` : predict on a single datapoint (the `.features` field of a `LabeledPont`) or an RDD of datapoints.
#    * `toDebugString()` : print the classifier in a human readable format.

errors={}
catInfo = {}
for i in range(10,54):
    catInfo[i] = 2
depth  = 13  
model=GradientBoostedTrees.trainClassifier(trainingData,categoricalFeaturesInfo=catInfo,maxDepth=depth,numIterations=13,learningRate = 0.15)
#print model.toDebugString()
errors[depth]={}
dataSets={'train':trainingData,'test':testData}
for name in dataSets.keys():  
	data=dataSets[name]
	Predicted=model.predict(data.map(lambda x: x.features))
	LabelsAndPredictions=data.map(lambda x: x.label).zip(Predicted) 
	Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
	errors[depth][name]=Err
print depth,errors[depth]




# coding: utf-8
# Name: Mitesh Gadgil
# Email: mgadgil@ucsd.edu
# PID: A53095373
from pyspark import SparkContext
sc = SparkContext()
# Your program here

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

# Read the file into an RDD
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/HIGGS/HIGGS.csv'
inputRDD=sc.textFile(path)

# Transform the text RDD into an RDD of LabeledPoints
Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')])\
				.map(lambda x: LabeledPoint(x[0],Vectors.dense(x[1:])))


# ### Reducing data size
# In order to see the effects of overfitting more clearly, we reduce the size of the data by a factor of 10

Data1=Data.sample(False,0.1).cache()
(trainingData,testData)=Data1.randomSplit([0.7,0.3],seed=255)
trainingData.cache()
testData.cache()

# ### Gradient Boosted Trees
# 
# * Following [this example](http://spark.apache.org/docs/latest/mllib-ensembles.html#gradient-boosted-trees-gbts) from the mllib documentation
# 
# * [pyspark.mllib.tree.GradientBoostedTrees documentation](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.tree.GradientBoostedTrees)
# 
# #### Main classes and methods
# 
# * `GradientBoostedTrees` is the class that implements the learning trainClassifier,
#    * It's main method is `trainClassifier(trainingData)` which takes as input a training set and generates an instance of `GradientBoostedTreesModel`
#    * The main parameter from train Classifier are:
#       * **data** – Training dataset: RDD of LabeledPoint. Labels should take values {0, 1}.
#       * categoricalFeaturesInfo – Map storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
#       * **loss** – Loss function used for minimization during gradient boosting. Supported: {“logLoss” (default), “leastSquaresError”, “leastAbsoluteError”}.
#       * **numIterations** – Number of iterations of boosting. (default: 100)
#       * **learningRate** – Learning rate for shrinking the contribution of each estimator. The learning rate should be between in the interval (0, 1]. (default: 0.1)
#       * **maxDepth** – Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 3)
#       * **maxBins** – maximum number of bins used for splitting features (default: 32) DecisionTree requires maxBins >= max categories

errors={}
catInfo = {}
depth  = 11
model=GradientBoostedTrees.trainClassifier(trainingData,categoricalFeaturesInfo=catInfo,maxDepth=depth,numIterations=16,learningRate = 0.14)
#print model.toDebugString()
errors[depth]={}
dataSets={'train':trainingData,'test':testData}
for name in dataSets.keys():  # Calculate errors on train and test sets
	data=dataSets[name]
	Predicted=model.predict(data.map(lambda x: x.features))
	LabelsAndPredictions=data.map(lambda x: x.label).zip(Predicted) 
	Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
	errors[depth][name]=Err
print depth,errors[depth]

