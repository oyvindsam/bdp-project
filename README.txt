K-means is one of the most popular algorithms used in scientific and industrial applications [1]. With use of Hadoop MapReduce one can compute clusters more efficiently, and several optimizations can be used to further improve the computing speed. In this paper I will present an implementation, optimizations, scaling issues, and test results for a K-means algorithm on a Hadoop MapReduce system.

Read the rest of the report [here](./report.pdf)


Compile:
mvn clean verify

Run:
hadoop jar kmeans.jar data_file numReducers numCentroids defaultCentroids
Eg:
hadoop jar kmeans.jar path/to/file/yt_data 1 4 d


Description:
numReducers: int, due to bug in provided code this must be 1 for the code to work correctly. This was OKed by the Instructor.
numCentroids: initial amount of centroids
defaultCentroids: if this is set to 'd', a hard-coded set of centroids is generated. Supports up to 90 centroids. If this is any other value
	numCentroids amounts of _random_ centroids are created, in a range either hard-coded or user specified by additional parameters.
	A method to find a max and min range is provided in Etl class.









- Acknowledgement:
    The sample code is based on Thomas's implementation of K-means on hadoop.
    It is modified as one learning material for RMIT's big data processing course.
    The original implementation can be found there: https://github.com/thomasjungblut/mapreduce-kmeans




