SparkVoter: Simulate the vote use case using Spark Streaming features
==========================

Here, we use "VoterGenerater" to generate votes, which comes from txt file - votes-o-40000.txt.
Using "Voter" to consume votes, validate and count votes for each contestant.

In order to save the historical votes, we use redis to save each vote.


Prepare with Redis
-----------


1. install redis, see http://redis.io/topics/quickstart
2. launch redis server with command ``./redis-server``


Usage with VoterGenerater
-----------


1. ``cd VoterGenerater``
2. ``java TCPServer votes-o-40.txt 1000 0``, here 1000 is the sending interval, 0 is flag (if flag is 1, 1000 means inputrate is 1000 tuple per second)


Usage with voter_1.0
-----------


Now the voter_1.0 is a Eclipse maven project.
We can run it under Eclipse, or we should export to a runnable jar file.

1.  File -> Export, and select Java -> Runnable JAR File
2. ``java -jar voter.jar 1000 5000 1000``, here first 1000 is the batch duration, 5000 and 1000 are window size and slide size.

Notes: after each experiment, we should use "flushall" to remove all items in redis db for next experiment with redis client command tool "./redis-cli" 
