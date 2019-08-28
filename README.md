# InvertedIndex
This is an attempt to create an inverted index which indexes the documents based on words used.
Things to do: Add another feature (like number of occurrences)

Note: For the spark file, term frequency can be preferred over the nerm of occurrences of the word.
For that, the code is as follows:

oneAttempt = sc.wholeTextFiles(path).map(lambda x : cle(x)).map(lambda x: x.split("[]"))\
.map(lambda x: (x[0].split(), x[1], len(x[0]))).flatMap(lambda x: (segregrate(x[0], x[1], x[2])))\
.map(lambda x: ((x[0],x[1], x[2]), 1)).reduceByKey(lambda x,y: x + y)\
.map(lambda x: (x[0][0], x[0][1], round(x[1] / x[0][2], 5)))\
.sortBy(lambda a: a[2], ascending = False).map(lambda x: (x[0], (x[1]+ "(" + str(x[2]) + ")")))\
.reduceByKey(lambda x,y: x +" "+ y)
