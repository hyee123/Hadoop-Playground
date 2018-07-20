from pyspark import SparkConf, SparkContext

# u.item format -------->  [movieID(int) , MovieName(String), Date , imbdLink]
# u.data format ------->    [UserID(int), MovieID(int) , Rating , timestamp] 

#objective: using u.data find the most popular(most rated) three star movies
#using just RDD


#create a dictionary for u.item so we can map MovieID to their actual names
def nameDict():
	movieToNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			stuff = line.split("|") #data is | delimeted
			movieToNames[int(stuff[0])] = stuff[1]
	return movieToNames

#makes ever line to [movieID ,  (rating , 1) ]
def mapper(line):
	l = line.split()
	return (int(l[1]), (float(l[2]) , 1 ))
	


if __name__ == "__main__":

	#Create a Spark Context in order to use to create an RDD
	conf = SparkConf().setAppName("MostPopular3StarMovies")
	sc = SparkContext(conf = conf)

	#set up movie dictionary from the u.items
	movieDict = nameDict()

	#create RDD from text file (u.data)
	l = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

	#(mapper) parse l so that we ---> [moveID, (rating, 1)]
	l2 = l.map(mapper)

	#reduce down via the key such that [movieID , (summedRating , summed Count)
	l3 = l2.reduceByKey(lambda a , b: (a[0] + b[0], a[1] + b[1]))
	
	#average out the stuff -- > [movieID, (avgRating, numberOfRating)
	l4 = l3.mapValues(lambda q: ((q[0] / q[1]), q[1]))

	#filters out and keeps all movies that have three stars only
	l5 = l4.filter (lambda a: a[1][0] > 3 and a[1][0] < 4)

	#sort movies by their average ratings
	l6 = l5.sortBy(lambda a : -a[1][1])

	#take a small sample of only 25 movies
	result = l6.take(25)

	#print
	for results in result:
		print (movieDict[results[0]], results[1])

