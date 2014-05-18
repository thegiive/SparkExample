from pyspark.mllib.recommendation import ALS
from numpy import array
import sys
from pyspark import SparkContext

# Get the Movie Title 
dict = {} 
for l in open( "movies.txt" , "r" ):
	arr = l.split(",")
	dict[arr[0]] = arr[1] 


# Load and parse the data
sc = SparkContext(sys.argv[1], "PythonWordCount")
data = sc.textFile("ratings1.txt")
ratings = data.map(lambda line: array([float(x) for x in line.split(',')]))

# Build the recommendation model using Alternating Least Squares
model = ALS.train(ratings, 10, 30)

# Evaluate the model on training data
arr = []
for i in range(1,3952):
	  arr.append((6040,i))

#Predict the user rating 
testdata = sc.parallelize(arr)
predictions = model.predictAll(testdata)
#print predictions.map( lambda p :  (p[2] , p[1])).sortByKey(False).take(10)

#Get the user 6040 's and get the top 10 movie 
mypredict = predictions.filter( lambda p : p[0] == 6040 ).map( lambda p :  (p[2] , p[1])).sortByKey(False).take(10)

print "The system recommendate the top 10 movies for user 6040"
c = 1 
for predict in mypredict: 
	print str(c)+". "+dict[str(predict[1])]
	c += 1

