This works much like [Hadoop streaming's Aggregate package](http://hadoop.apache.org/docs/r1.1.2/streaming.html#Hadoop+Aggregate+Package), except that it provides access to [Algebird](http://github.com/twitter/algebird)'s suite of aggregators. It can also be used directly from the unix shell, as a filter in a pipeline.

To build:
````
mvn package
````

To run:
````
bin/scrub < /path/to/data.tsv
````

The input format expects each line to contain one key and one value, separated by a tab. The key should be prefixed by an aggregation spec that algescrubber recognizes. This will determine how all of the values that share that key will be combined. For example, the spec "l" will simply treat the values as long integers and sum them. The spec "hll" will use the HyperLogLog algorithm to produce an estimate of the number of unique values for that key. See below for a list of all of the specs that are currently defined.

The output format will include two lines for each unique key in the input, also as tab-separate key/value pairs. The first line will have the key verbatim, and will include a machine-readable serialization of the aggregation (for example, for HyperLogLog this will be a serialized version of the bitfield being used for the estimate). The second line will prefix the key with a colon, and will have a human-readable representation of the aggregation (for example, for HyperLogLog this will be a decimal integer representing the estimate of unique values). If you only care about the human-readable values, just grep for "^:" to pull them out.

The reason for including the machine readable values is so that multiple outputs can be combined (eg with cat) and piped back into algescrubber. The human readable lines will be ignored. You can freely mix pre-aggregated output and new inputs for the same keys.

Although the default implementation reads from stdin and writes to stdout, it would be straightforward to add a new driver class that, for example, sinks the output into a Redis or Mongo instance that is maintaining overall aggregations for these keys.

#Aggregator specs

* *l* sums of long values
* *lmin* min long value
* *lmax* max long value
* *hll* unique values (note: append an integer to change the number of bits used, eg hll4 vs. hll12)
* *mh* minhash signatures (note: append an integer to change the number of hashes used, eg mh100 vs mh300)
* *top* top K items; expects values to be in the format score:item (note: top10 by default, try top5, top20, etc)
* *hist* produce a histogram of the value frequencies. Expects values to be quantized into a reasonable range (eg thousands not millions) of integer values. Output is of the form value:count,value:count,value:count.
* *pct* percentile; makes the same quantization assumptions as hist; produces the median by default, but use pct95, pct5 etc for other percentiles. Eventually it would be nice to remove the quantization restriction here.
* *hash* feature hashing. Input should either be a word (of a bag of words feature) or a dimension:value where value is a double. (append an integer for number of hash bits; hash10 is default, try hash4 or hash12 etc).
* *dcy* exponential decay; input needs to be in the format timestamp:value. Append an integer to set the halflife, default is one day (dcy86400). Output will be the value as of the end of the current day.
* *hh* heavy hitters; keeps the top K values by frequency (note: hh10 is default, try hh3, hh20 etc).
