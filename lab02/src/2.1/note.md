Input: sets of N points, separated by '\n'
each data point coordinates are separated by ' '
float type
- input k from terminal

Mapping: <key=centroids, value=datapoints>
Reduce: for <key, value> of same keys: calculate avg coordinates.
    - If new keys = old keys:
        end
    - else:
        next MapReduce chain.
Output:
1. centroids
2. datapoints and indexes

Problems need to be solved:
- How to input from terminal.
    This makes sense...
- How to make a MapReduce job chain.
    So after researching, it is confirmed that every MapReduce job requires input and output directory to get things done, so we gonna assume that /input /temp1 /temp2 /..... /output

    where each temp directory has k files of centroid coord and data points.



Input [x1, y1, x2, y2, ....]
- First iter:
    + Pre: Get the k clusters
    + Map: Read from original input -> Calculate the dis -> Map into <key=cluster_index, value=datapoints>
    + Reduce: Calculate clusters -> Update clusters, save the k clusters
- Next iters:
    + Map: Read from original input -> Calculate the dis -> Map into <key=cluster_index, value=datapoints>
    + Reduce: Calculate clusters -> Update clusters

Step 0: class Point
Step 1: initialize the centroids, save it into a temp.
Step 2: Mapping
    - Input centroids and datapoints
    - Get point assign <key=index, value=point>
    - Write into context?