from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
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
    positives, negatives = [], []
    t = 0
    max_val = 0
    for count in counts:
      if count:
        t+=1
        positives.append(count[0][1])
        negatives.append(count[1][1])
        max_val = max([max_val, count[0][1], count[1][1]])
    t_range = range(0,t)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_xlabel('Time Step')
    ax.set_ylabel('Word Count')
    ax.set_xlim(-1, t)
    ax.set_ylim(0, max_val+50)
    ax.plot(t_range, positives, 'bo', linestyle='solid', label = 'positive')
    ax.plot(t_range, negatives, 'go', linestyle='solid', label = 'negative')
    plt.legend(loc='upper left')
    plt.savefig("plot.png")
    print("Figure saved in plot.png")
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    with open(filename) as f:
      content = f.readlines()
    return [one.strip() for one in content]


def stream(ssc, pwords, nwords, duration):
    def scores(tweet):
        words = tweet.split()
        pos, neg = 0, 0
        for word in words:
          if word in pwords:
            pos += 1
          elif word in nwords:
            neg += 1
        return pos, neg

    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    senti_counts = tweets.map(scores).\
      reduce(lambda a,b: (a[0]+b[0], a[1]+b[1])).\
      flatMap(lambda x: [("positive", x[0]),("negative", x[1])])
    senti_counts.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    senti_counts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
