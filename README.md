# TwitterLinks

[![Build Status](https://travis-ci.org/tanmaykm/TwitterLinks.jl.svg?branch=master)](https://travis-ci.org/tanmaykm/TwitterLinks.jl)

## Datasets
- [full extract](http://twitter.mpi-sws.org/data-icwsm2010.html). 41 million vertices. 24GB 
- [smaller sample](http://socialcomputing.asu.edu/datasets/Twitter). 11 million vertices. 1.3GB

## Method 1

````
- cd to hadoop configuration
- for node in `cat slaves | grep -v master`; do echo $node; julia -e 'Pkg.clone("https://github.com/tanmaykm/TwitterLinks.jl.git")'; done
- julia --machinefile slaves
- using TwitterLinks
- S = TwitterLinks.as_sparse("hdfs://root@" * string(getipaddr()) * ":9000/twitter_small.csv", :csv)
- S = normalize_cols(S)
- infl = find_influencers(S)
- count_connections(S, infl)
````
