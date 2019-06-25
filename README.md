# FDE Project 2
This is the implementation of the second project of the course Foundations of Data Engineering offered by TU München. It's about analyzing taxi rides in New York with Apache Spark. The implementation is in ReturnTrips.scala file.

## Dataset
Dataset covers one month and consists of time and location for each trip's start and end.

## Used Frameworks, Filesystem and Language
- Apache Spark
- Hadoop Filesystem
- Scala


## Task
The task is to match trips and return trips. For a given trip a, another trip b is considered as a return trip iff:
1. b's pickup time is within 8 hours after a's dropoff time
2. b's pickup location is within r meters of a's dropoff location
3. b's dropoff location is within r meters of a's pickup location
where r is a distance in meters between 50 and 200.

The dataset to be returned contains for each trip a all return return trips b.

## Time limit
The implementation of the query has to be computed in less than 10 minutes.

## Evaluation
The machine used for evaluation will have 64GB RAM and a 6 core Intel i7-3930K processor.
