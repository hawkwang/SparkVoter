package edu.mit.sstore.voter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.receiver.Receiver;

import com.google.common.base.Optional;

import scala.Option;
import scala.Tuple2;

public class Voter  extends Receiver<String>
{

	// ============= Receiver code that receives data over a socket
	// ==============

	String host = null;
	int port = -1;

	public Voter(String host_, int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				receive();
			}
		}.start();
	}
	
	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		Socket socket = null;
		String userInput = null;

		try {
			// connect to the server
			socket = new Socket(host, port);

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			// Until stopped or connection broken continue reading
			while (!isStopped() && (userInput = reader.readLine()) != null) {
				//System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			reader.close();
			socket.close();

			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}
	
	public static PhoneCall getPhoneCall(String value)
	{
		String[] values = value.split(" ");
		
		long voteId = Long.valueOf(values[0]);
		long phoneNumber = Long.valueOf(values[1]);
		int contestantNumber = Integer.valueOf(values[2]);
		int timestamp = Integer.valueOf(values[3]);
		
		PhoneCall call =  new PhoneCall(voteId, contestantNumber, phoneNumber, timestamp);
		
		return call;
	}

	public static void main(String[] args) {
//		System.out.println("hawk test 1 ...");
//		String voteFile = "votes-o-40000.txt";
		String master = System.getenv("MASTER");
		if (master == null) {
			master = "local[2]";
		}
		
		SparkConf conf = new SparkConf()
				.setAppName("Voter Application")
				.setMaster(master);
				//.set("spark.cleaner.ttl", "100");
				//.set("spark.streaming.unpersist", "true");
		
		//JavaSparkContext sc = new JavaSparkContext(conf);

		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(Integer.valueOf(args[0])));
		
		jssc.checkpoint(".");
		
//	    JavaDStream<String> votes = jssc.textFileStream(voteFile);
		JavaReceiverInputDStream<String> votes = jssc.receiverStream(
			      new Voter("localhost", 6789) );
		
		List<Tuple2<Long,Integer>> data = Arrays.asList();
		
		// [Question] ??? how to create empty JavaPairRDD for updating 
		//final JavaPairRDD<Long, Integer> phoneCallHistoryData = new JavaPairRDD(null, null, null);
		
		
//		System.out.println("hawk test 2 ...");
	    
		//votes.cache();
		
	    JavaDStream<PhoneCall> phoneCalls = votes.map(new Function<String, PhoneCall>() {
		      public PhoneCall call(String s) {
//		    	  System.out.println("original votes - " + s);
		    	  return getPhoneCall(s);
		      }
		    });
	    
	    //System.out.println("original votes number");
	    //phoneCalls.count().print();

	    JavaPairDStream<Integer, Integer> contestants = phoneCalls.mapToPair(
	      new PairFunction<PhoneCall, Integer, Integer>() {
	        public Tuple2<Integer, Integer> call(PhoneCall x) {
	        	return new Tuple2<Integer, Integer>(x.contestantNumber, 1);
	        }
	      });
	    
	    Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
	    		  new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
	    		    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
	    		      // add the new values with the previous running count to get the new count
	    		      Integer sum = 0;
	    		      for(Integer i : values)
	    		      {
	    		    	  sum += i;
	    		      }
	    		      Integer newSum = sum + state.or(0);
	    		      return Optional.of(newSum);
	    		    }
	    		  };
	    		  
	    JavaPairDStream<Integer, Integer> runningContestantCounts = contestants.updateStateByKey(updateFunction);
	    runningContestantCounts.print();

	    JavaPairDStream<Long, Integer> calls = phoneCalls.mapToPair(
	  	      new PairFunction<PhoneCall, Long, Integer>() {
	  	        public Tuple2<Long, Integer> call(PhoneCall x) {
	  	        	return new Tuple2<Long, Integer>(x.phoneNumber, 1);
	  	        }
	  	      });

	    JavaPairDStream<Long, Integer> callNumberCounts = calls.updateStateByKey(updateFunction);
	    callNumberCounts.print();
	    
	    PairFunction<Tuple2<Long, Integer>, Integer,Long> swapFunction = new PairFunction<Tuple2<Long, Integer>, Integer, Long>() 
	    {
			public Tuple2<Integer, Long> call(Tuple2<Long, Integer> in) {
				return in.swap();
			}
		};
	    
		JavaPairDStream<Integer, Long> swappedCallNumberCounts = callNumberCounts.mapToPair(swapFunction);
		swappedCallNumberCounts.print();

		JavaPairDStream<Integer, Long> sortedCallNumberCounts = swappedCallNumberCounts.transformToPair(
				new Function<JavaPairRDD<Integer, Long>, JavaPairRDD<Integer, Long>>() 
				{

					public JavaPairRDD<Integer, Long> call(JavaPairRDD<Integer, Long> in) throws Exception 
					{
						return in.sortByKey(false);
					}

				});
		
		sortedCallNumberCounts.foreach(new Function<JavaPairRDD<Integer, Long>, Void>() {
					public Void call(JavaPairRDD<Integer, Long> rdd) {
						String out = "\nTop 10 :\n";
						for (Tuple2<Integer, Long> t : rdd.take(10)) {
							out = out + t.toString() + "\n";
						}
						System.out.println(out);
						return null;
					}
				});
	    
	    JavaDStream<PhoneCall> validatedPhoneCalls = phoneCalls.filter(new Function<PhoneCall, Boolean>() {
		      public Boolean call(PhoneCall call) { 
		    	  if(call.contestantNumber>6)
		    		  return false;
		    	  else
		    	  {
//		    		  // determine if the call number has been used more than threshold
//		    		  // step 1 - get the current times
//		    		  List<Integer> numbers = phoneCallHistoryData.lookup(call.phoneNumber);
//		    		  
//		    		  // step 2 - if not exist, return true
//		    		  if(numbers==null)
//		    			  return true;
//		    		  else
//		    		  // else if less then threshold, return true
//		    		  {
//		    			  Integer number = numbers.get(0);
//		    			  if(number>2)
//		    				  return false;
//		    			  else
//		    			  {
//		    				  //update the phoneCallHistoryData, but how ??? [Question]
//		    				  return true;
//		    			  }
//		    		  }
		    		  
		    		  return true;

		    	  }
		      }
		    });
	    
	    //System.out.println("valid votes number");
	    //validatedPhoneCalls.count().print();
	    
	    // [Question] How to make aggregation from streaming begin till current batch, 
	    // not just make aggregation based on current batch or window

//	    JavaDStream<String> phonenums = votes.filter(new Function<String, Boolean>() {
//	    		      public Boolean call(String s) { 
//	    		    	  
//	    		    	  return s.contains("a"); 
//	    		    	  }
//	    		    });
//	    		    
//	    		    
//	    
//	    JavaPairDStream<String, Integer> phonenums = votes.mapToPair(
//		  	      new PairFunction<String, String, Integer>() {
//		  	        public Tuple2<String, Integer> call(String x) {
//		  	        	String[] fields = x.split(" ");
//		  	        	System.out.println("phone number : " + fields[1]);
//		  	        	return new Tuple2<String, Integer>(fields[1], 1);
//		  	        }
//		  	      });
//
//	    JavaPairDStream<String, Integer> phoneNumCounts = phonenums.reduceByKey(
//	  	      new Function2<Integer, Integer, Integer>() {
//	  	        public Integer call(Integer i1, Integer i2) throws Exception {
//	  	        	
//	  	          return i1 + i2;
//	  	        }
//	  	      });
//
//	    phoneNumCounts.cache();
//	    
//	    JavaPairDStream<String, Integer> newvotes = votes.mapToPair(
//	  	      new PairFunction<String, String, Integer>() {
//	  	        public Tuple2<String, Integer> call(String x) {
//	  	        	String[] fields = x.split(" ");
//	  	        	
//	  	        	
//	  	        	System.out.println("voteId : " + fields[0]);
//	  	        	return new Tuple2<String, Integer>(fields[2], 1);
//	  	        }
//	  	      });
//	    
//	    JavaPairDStream<String, Integer> voteCounts = newvotes.reduceByKey(
//	      new Function2<Integer, Integer, Integer>() {
//	        public Integer call(Integer i1, Integer i2) throws Exception {
//	        	if()
//	          return i1 + i2;
//	        }
//	      });
//	    
//	    voteCounts.print();
	    
	    jssc.start();              // Start the computation
	    jssc.awaitTermination();   // Wait for the computation to terminate
	    
	}

}
