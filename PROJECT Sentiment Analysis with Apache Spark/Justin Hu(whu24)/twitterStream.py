import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
	
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    p_count, n_count = [], [] 
    for i in counts:
        p_count.append(i[1][1])
        n_count.append(i[0][1])
    
    fig = plt.figure()
    plt.plot(p_count, 'bo-', label="Positive")
    plt.plot(n_count, 'go-', label="Negative")
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.legend(loc='upper left')
    fig.savefig("plot.png")

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    words = open(filename).read().strip().split('\n')
    return words	

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def npcount(tweet):
    if tweet in pwords:
        return ("positive", 1)
    elif tweet in nwords:
        return ("negative", 1)
    else :
        return ("positive", 0)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    tweets = tweets.flatMap(lambda line: line.split(" "))
    def npcount(tweet):
        if tweet in pwords:
            return ("positive", 1)
        elif tweet in nwords:
            return ("negative", 1)
        else :
            return ("positive", 0)
    tweets = tweets.map(lambda x: ("positive", 1) if x in pwords else ("positive", 0)).union(tweets.map(lambda x: ("negative", 1) if x in nwords else ("negative", 0)))
    tweets = tweets.reduceByKey(lambda a,b:a+b)
    
    runningCounts = tweets.updateStateByKey(updateFunction)
    runningCounts.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    tweets.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))    

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
