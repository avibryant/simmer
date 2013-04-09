#Algescrubber
Avi Bryant

Algescrubber is a streaming aggregation tool. It can be used as a filter in a unix pipeline, and with Hadoop or similar systems, to incrementally and efficiently summarize large volumes of data using a fixed amount of memory. Some of the aggregations it supports include:
* counts of unique values
* exponentially decaying values
* top k most frequent values
* percentiles
* min-hash signatures

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

The output is, like the input format, a tab-separated key-value stream. The output is designed to be easy to read by humans, while at the same time allowing multiple outputs to be combined and fed back into scrub for further aggregation. As a simple example of how these are in conflict, consider an aggregation producing the average of all of the values for a key. The human-readable output is just a single number, the average. To properly combine multiple averages, however, you have to know the count of how many values originally went into each one, so that you can weight them properly. Algescrubber solves this by producing two values for each key, one with a possibly opaque, machine-readable value that is suitable for further aggregation, and another that includes a human-readable version of the value. Often, it's convenient to filter scrub's output through "cut -f 1,3" to see only the human-readable versions.

For simple cases like sum, the human-readable and machine-readable formats are identical, so the output looks like this:

````sh
sum:x	3	3
````

For other aggregations, it might look more like this:
````sh
dcy:x	%%%AQBjb20udHdpdHRlci5hbGdlYmlyZC5EZWNheWVkVmFsdeUBQMVkIdW357VAWQAAAAAAAA==	8.752114744797748
````

Algescrubber will ignore the human readable values if it's given its own output to consume, because it only looks at the first two columns of input. It will also distinguish properly between new single values, and previous aggregated output, for the same key, and will happily combine these with each other. This means, for example, that you can take the aggregated output of yesterday's logs and cat it with the raw input for today's logs, and get the combined output of both.

###Flushing:

The scrub command takes two optional integer arguments. The first argument is capacity: how many keys it should hold in memory at once. Whenever a new keys is added that will exceed this capacity, the current aggregate value for the least recently used key is flushed. In general these will be infrequent keys that may never recur again, but if they do, you may see multiple outputs for the same key; these need to be aggregated in turn (perhaps by feeding the output back through scrub) to get the complete result.

The second argument controls the maximum number of values to aggregate for any one key before flushing. If this is set to 0, there is no maximum and frequently seen keys will only be output when there is no more input. However, if you have an infinite stream of input, you will want to set this to some non-zero value to get intermediate results out. Again, this means there may be multiple values for a single key that need to be combined after the fact.

The defaults are equivalent to:

````
bin/scrub 5000 0
````


###Numeric Aggregations

The human-readable output of these is always a single number for each key.

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
<td>Sum</td>
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

<tr>
<th>min</th>
<td>Minimum</td>
<td>n/a</td>
<td>n/a</td>
<td><pre>min:x 1
min:x 2
</pre>
</td>
<td><pre>
min:x 1
</pre>
</td>
</tr>

<tr>
<th>min</th>
<td>Maximum</td>
<td>n/a</td>
<td>n/a</td>
<td><pre>max:x 1
max:x 2
</pre>
</td>
<td><pre>
max:x 2
</pre>
</td>
</tr>

<tr>
<th>uv</th>
<td>Unique values<br>(estimated using the HyperLogLog algorithm)</td>
<td>number of hash bits - memory use is 2^n</td>
<td>uv12</td>
<td><pre>hll:x a
hll:x b
hll:x a
</pre>
</td>
<td><pre>
hll:x 2
</pre>
</td>
</tr>

<tr>
<th>pct</th>
<td>Percentile<br></td>
<td>which percentile to output</td>
<td>pct50 (ie median)</td>
<td>For now, data should be quantized to a reasonable number of integer bins.
<pre>pct50:x 2
pct50:x 4
pct50:x 100
</pre>
</td>
<td><pre>
pct50:x 4
</pre>
</td>
</tr>

<tr>
<th>dcy</th>
<td>Exponentially decayed sum<br></td>
<td>half-life of a value, in seconds</td>
<td>dcy86400 (ie, half-life of one day)</td>
<td>Data should be in the format timestamp:value.
<pre>dcy:x   1365187171:100
dcy:x   1365100771:100
dcy:x   1365014371:100
</pre>
</td>
<td>Human-readable output will be the decayed value as of the end of the current day.
<pre>
dcy:y 122.3
</pre>
</td>
</tr>
</table>

###Other Aggregations

These are more specialized than, or build in some way on, the numeric aggregations.

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
<th>top</th>
<td>Top K<br>(by any numeric aggregation)</td>
<td>how many top values to retain <br> also requires a secondary prefix (see example)</td>
<td>top10</td>
<td>For example, this will find the top 3 items by the sum of their values, assuming an item:value format<pre>
top3:sum:x	a:1
top3:sum:x	b:2
top3:sum:x	a:2
top3:sum:x	c:1
top3:sum:x	d:3
</pre>

However, top10:uv:x, or top5:pct95:x, and so on, would also be valid keys.
</td>
<td><pre>
top3:sum:x	a:3,d:3,b:2
</pre>
</td>
</tr>

<tr>
<th>bot</th>
<td>Bottom K<br>(by any numeric aggregation)</td>
<td>works just like top</td>
<td>bot10</td>
<td><pre>
bot3:sum:x	a:1
bot3:sum:x	b:2
bot3:sum:x	a:2
bot3:sum:x	c:1
bot3:sum:x	d:3
</pre>
</td>
<td><pre>
bot3:sum:x	c:1,b:2,d:3
</pre>
</td>
</tr>

<tr>
<th>mh</th>
<td>Min-Hash Signature<br>(Used for estimating set similarity)</td>
<td>number of hashes to use</td>
<td>mh64</td>
<td>Each value should be a single element of the set represented by the key.<pre>mh:x    a
mh:x    b
mh:x    c
</pre>
</td>
<td>Hex representation of n 16-bit hashes.  If two sets have k matching hash values, their jaccard similarity = k/n.<pre>
mh:x 0FCC:2E1F:0DD7:0049:3BF3:10D4:6460:75D4:392B:07AF:2064:27F0:6931:6717:3A0A:16D9:122E:51C6:8632:64BD:0CAE:0D15:8357:39A5:2008:4ED7:5733:44F8:1F70:02F7:23D5:59AE:0ECB:8EE0:4E1C:0249:9804:610B:0DBD:0316
</pre>
</td>
</tr>

<tr>
<th>fh</th>
<td>Feature Hashing<br>(projects any number of features into a fixed-size vector)</td>
<td>number of hash bits to use (output vector size will be 2^n)</td>
<td>fh10</td>
<td>Values can either be just a token, for categorical features, or token:number for continuous features.
<pre>fh4:x    hello
fh4:x    world
fh4:x    temp:32
</pre>
</td>
<td><pre>
fh4 0.0,0.0,-1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,-32.0,0.0
</pre>
</td>
</tr>

</table>

###Aggregations TODO

* Exponential moving average (this is just decaying sum / decaying count, but worth having)
* Statistical moments - mean, variance, skew, kurtosis. As a single aggregation or as 4?
* F2 (measure of key frequency skew... too specialized?)
* Remove the quantization requirement for percentiles (?)

###Other TODO

* Flushing to key/value store (redis, mongo, mysql?)
