#Algescrubber
Avi Bryant

Algescrubber is a streaming aggregation tool. It can be used as a filter in a unix pipeline, and with Hadoop or similar systems, to incrementally and efficiently summarize large volumes of data using a fixed amount of memory. Some of the aggregations it supports include:
* counts of unique values
* exponential moving averages
* top k most frequent values
* statistical moments
* percentiles

It was inspired in part by [Hadoop streaming's Aggregate package](http://hadoop.apache.org/docs/r1.1.2/streaming.html#Hadoop+Aggregate+Package), but uses the probabalistic aggregation algorithms from Twitter's [Algebird](http://github.com/twitter/algebird). It's not to be confused with an [algae scrubber](http://i611.photobucket.com/albums/tt191/FloydRTurbo/2011%20Aquarium%20Pics/Miscellaneous/ATS%20Designs/AS_teboLED-2.jpg), which is an entirely different kind of filter.

###To build:

The first time, you need to install a development build from my fork of algebird:

````sh
git clone git://github.com/avibryant/algebird.git
cd algebird
sbt publish-local
cd ..
git clone git://github.com/avibryant/algescrubber.git
cd algescrubber
./install-algebird-snapshot.rb
````

From then on:

````sh
mvn package
````

###To run:
````sh
bin/scrub < /path/to/data.tsv
````

###Input format:

The scrub command takes tab-delimited key-value input and combines all of the values for each key. Here's a very simple sample input:

````
sum:x	1
min:y	3
min:y	4
sum:x	2
min:y	3
````

And here are the keys and values of the output:

````
sum:x	2
min:y	3
````

scrub has taken the two values for the key "sum:x", 1 and 2, and produced their sum, 3; it has also taken the three values for the key "min:y", 3, 4 and 3, and produced their minimum, 3.

The prefix of each key, before the colon, determines how its values will be combined; in this case, the values for "sum:x" are summed, and the values for "min:y" are combined by taking the minimum. As in this example, you can freely mix different types of aggregation in the same input stream.

Note that the prefix is treated not just as an annotation, but as an integral part of the key. It's often useful to aggregate the same set of values in multiple ways; since, for example, "min:x" and "max:x" are different keys, there's no problem including both and aggregating them separately.

Many of the aggregations can be parameterized by including an integer in the prefix. For example, the percentile aggregator might appear as the prefix "pct95" (to compute the 95th percentile) or the prefix "pct50" to compute the median. A full list of the supported aggregations, and their parameterizations, is below.

###Output format

The output is, like the input format, a tab-separated key-value stream. The output  is designed to be easy to read by humans, while at the same time allowing multiple outputs to be combined and fed back into scrub for further aggregation. As a simple example of how these are in conflict, consider an aggregation producing the average of all of the values for a key. The human-readable output is just a single number, the average. To properly combine multiple averages, however, you have to know the count of how many values originally went into each one, so that you can weight them properly. Algescrubber solves this by producing two lines for each key, one with a possibly opaque, machine-readable value that is suitable for further aggregation, and the other as a "comment", prefixed with the # character, that includes a human-readable version of the value. Often, it's convenient to filter scrub's output through "grep ^#" to see only the human-readable versions.

For simple cases like sum, the human-readable and machine-readable formats are identical, so the output looks like this:

````sh
# sum:x	3
sum:x	3
````

Algescrubber will ignore the commented lines on input. It will also distinguish properly between new single values, and previous aggregated output, for the same key, and will happily combine these with each other. This means, for example, that you can take the aggregated output of yesterday's logs and cat it with the raw input for today's logs, and get the combined output of both.

###Aggregations

<table>
<tr>
<th>Prefix</th>
<th>Description</th>
<th>Parameter</th>
<th>Default</th>
<th>Sample input</th>
<th>Sample output</th>
</tr>
<tr>
<th>sum</th>
<td>Numeric sum</td>
<td>n/a</td>
<td>n/a</td>
<td><pre>sum:x 1
sum:x 2
</pre>
</td>
<td><pre>
sum:x 3
</pre>
</td>
</tr>
</tr>
</table>
