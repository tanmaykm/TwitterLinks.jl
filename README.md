# TwitterLinks

Pagerank of twitter link graph. As an example only for learning to use Elly.jl, Blocks.jl and associated packages.

## Datasets
- [full extract](http://twitter.mpi-sws.org/data-icwsm2010.html). 41 million vertices. 24GB 
- [smaller sample](http://socialcomputing.asu.edu/datasets/Twitter). 11 million vertices. 1.3GB

## Method 1

````
- load data and install packages
    - hdfs dfs -copyFromLocal /datastore/twitter_small/Twitter-dataset/data/edges.csv /twitter_small.csv
    - cd to hadoop configuration
    - for node in `cat slaves | grep -v master`; do echo $node; julia -e 'Pkg.clone("https://github.com/tanmaykm/TwitterLinks.jl.git")'; done
- julia --machinefile slaves
- using TwitterLinks
- S = TwitterLinks.as_sparse("hdfs://root@" * string(getipaddr()) * ":9000/twitter_small.csv", :csv, 11316811)
- normalize_cols(S)
- infl = find_influencers(S)
- count_connections(S, infl)
````
