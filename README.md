# Scala-Spark Movie Recommender System
Implementation of different recommender system techniques in Scala-Spark applied to MovieLens movie data

The goal of a movie recommender system is to recommend the best films not yet seen to users.

Implemented recommendation techniques:
-Random
-Item-based Collaborative Filtering
-Content-based Filtering
-Hybrid Filtering (based on Item-based and Content-Based methods)

The different techniques have been applied on MovieLens datasets (https://grouplens.org/datasets/movielens/), in particular the latest small dataset (100k ratings) and old 1M dataset.

Recommender system have been executed in local and on AWS, in particular using S3 and EMR.
