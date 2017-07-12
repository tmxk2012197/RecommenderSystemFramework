# Movie Recommender System

## Overview

Recommended movies to users based on the data from Netflix using 4 MapReduce Jobs in Linux environment.

## Data

Data comes from the training dataset of Netflix Prize Challenge.

## Building Steps

*	Built the user-rating matrix showing each user’s rating for each movie. 
*	Built the co-occurrence matrix showing relationship among movies based on item-based collaborative filtering. 
*	Implemented multiplication of the two matrices to obtain the recommendation result.


## How to run

* $hadoop com.sun.tools.javac.Main *.java
* $jar cf recommender.jar *.class
* $hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrixGenerator /Normalize /Multiplication /Sum /RecommanderList
```
* args0: original dataset
* args1: output directory for DividerByUser job
* args2: output directory for coOccurrenceMatrixGenerator job
* args3: output directory for Normalize job
* args4: output directory for Multiplication job
* args5: output directory for Sum job
* args6: output directory for RecommenderListGenerator job
