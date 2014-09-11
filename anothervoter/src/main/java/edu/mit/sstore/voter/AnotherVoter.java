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

import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.google.common.base.Optional;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Option;
import scala.Tuple2;

public class AnotherVoter extends Receiver<String> {

	// ============= Receiver code that receives data over a socket
	// ==============

	String host = null;
	int port = -1;
	
	public static final int MAX_VOTES = 1;//1000; 
	public static final int NUM_CONTESTANTS = 6; 

	public AnotherVoter(String host_, int port_) {
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
				// System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			reader.close();
			socket.close();

			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			//restart("Could not connect", ce);
			stop("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}

	public static PhoneCall getPhoneCall(String value) {
		String[] values = value.split(" ");

		long voteId = Long.valueOf(values[0]);
		long phoneNumber = Long.valueOf(values[1]);
		int contestantNumber = Integer.valueOf(values[2]);
		int timestamp = Integer.valueOf(values[3]);

		PhoneCall call = new PhoneCall(voteId, contestantNumber, phoneNumber,
				timestamp);

		return call;
	}

	static public Integer count = 0;

	public static void main(String[] args) {

		String master = System.getenv("MASTER");
		if (master == null) {
			master = "local[2]";
		}

		SparkConf conf = new SparkConf().setAppName("Voter Application")
				.setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	    
	    final Long batch_duration = Long.valueOf(args[0]);
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration( batch_duration ) );

		jssc.checkpoint(".");

		JavaReceiverInputDStream<String> votes = jssc.receiverStream(new AnotherVoter(
				"localhost", 6789));

		// get start timestamp
//		votes.foreachRDD(new Function<JavaRDD<String>, Void>() {
//
//			public Void call(JavaRDD<String> rdd) throws Exception {
//				Long count = rdd.count();
//				System.out.println( "count : " + count );
//				
////				XMemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("localhost:11211"));
////			    XMemcachedClient client= (XMemcachedClient) builder.build();
////			    client.setPrimitiveAsString(true);
//			    
//			    Long currentTimeStamp = System.currentTimeMillis();
//			    System.out.println("Start time: " + currentTimeStamp);
//			    
////			    client.add(currentTimeStamp.toString(), 0, throughput);
//			     
//
//				return null;
//			}
//
//		});	
		
		// transform text line stream to PhoneCall stream
		JavaDStream<PhoneCall> phoneCalls = votes
				.map(new Function<String, PhoneCall>() {
					public PhoneCall call(String s) {
						return getPhoneCall(s);
					}
				});

		JavaDStream<Long> counts = votes.count();
		counts.print();

		
		// filtering based on phone number 
		JavaDStream<PhoneCall> validateNumberPhoneCalls = phoneCalls
				.filter(new Function<PhoneCall, Boolean>() {
					public Boolean call(PhoneCall call) {
						
						String key = String.valueOf(call.phoneNumber);
						
						JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
						Jedis jedis = pool.getResource();
						boolean flag = true;
						try {
						  jedis.select(0); // use db - 0
						  String value = jedis.get(key);
						  if(value==null)
						  {
							  flag = true;
							  jedis.set(key, "1");
						  }
						  else
						  {
							  Integer count = Integer.valueOf(value);
							  if (count >= AnotherVoter.MAX_VOTES )
								  flag = false;
							  else
							  {
								  count++;
								  value = String.valueOf(count);
								  jedis.set(key, value);
								  flag = true;
							  }
						  }
							  
						  
						} finally {
						  if (null != jedis) {
						    jedis.close();
						  }
						}
						/// ... when closing your application:
						pool.destroy();

						return flag;

					}
				});
		
		
		// filtering based on contestant number
		JavaDStream<PhoneCall> validateCalls = validateNumberPhoneCalls
				.filter(new Function<PhoneCall, Boolean>() {
					public Boolean call(PhoneCall call) {
						if (call.contestantNumber > AnotherVoter.NUM_CONTESTANTS)
							return false;
						else {

							return true;
						}
					}
				});	
		
		// create updateFunction which is used to update the total call count for each phone number 
		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			public Optional<Integer> call(List<Integer> values,
					Optional<Integer> state) {
				// add the new values with the previous running count to get the
				// new count
				Integer sum = 0;
				for (Integer i : values) {
					sum += i;
				}
				Integer newSum = sum + state.or(0);
				return Optional.of(newSum);
			}
		};

		// 
		//validateCalls.print();
		
		//save all validate votes with redis
		validateCalls.foreachRDD(new Function<JavaRDD<PhoneCall>,Void>()
				{

					public Void call(JavaRDD<PhoneCall> rdd) throws Exception {
						
						rdd.foreach(new VoidFunction<PhoneCall>(){

							public void call(PhoneCall call) throws Exception {
								//System.out.println(call.toString());
								String key = String.valueOf(call.voteId);
								String value = call.getContent();
								
								// save <key,value> using redis
								JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
								Jedis jedis = pool.getResource();
								jedis.select(1); // use db - 1
								try {
								  jedis.set(key, value);
								} finally {
								  if (null != jedis) {
								    jedis.close();
								  }
								}
								/// ... when closing your application:
								pool.destroy();
							}
						}
						);
						
						return null;
					}
			
				}
				);	
		
		// validate calls
		JavaPairDStream<Integer, Integer> contestantVotes = validateCalls
				.mapToPair(new PairFunction<PhoneCall, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(PhoneCall x) {
						return new Tuple2<Integer, Integer>(x.contestantNumber, 1);
					}
				});		
		
		
		// use window to get generate leaderboard
		Integer size = Integer.valueOf(args[1]);
		Integer slide = Integer.valueOf(args[2]);
		
		JavaDStream<PhoneCall> windowCalls = validateCalls.window(new Duration(size), new Duration(slide));
		//windowCalls.print();
		
		// generate window contestant count
		JavaPairDStream<Integer, Integer> windowContestantNums = windowCalls
				.mapToPair(new PairFunction<PhoneCall, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(PhoneCall x) {
						return new Tuple2<Integer, Integer>(x.contestantNumber, 1);
					}
				});
		JavaPairDStream<Integer, Integer> windContestantCounts = windowContestantNums
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2)
							throws Exception {

						return i1 + i2;
					}
				});
		windContestantCounts.print();
		
		// generate the accumulated count for contestants
		JavaPairDStream<Integer, Integer> totalContestantCounts = contestantVotes.updateStateByKey(updateFunction);
		
		// used for sorting
		PairFunction<Tuple2<Integer, Integer>, Integer, Integer> swapFunction = new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> in) {
				return in.swap();
			}
		};

		JavaPairDStream<Integer, Integer> swappedTotalContestantCounts = totalContestantCounts
				.mapToPair(swapFunction);

		JavaPairDStream<Integer, Integer> sortedTotalContestantCounts = swappedTotalContestantCounts
				.transformToPair(new Function<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Integer>>() {

					public JavaPairRDD<Integer, Integer> call(
							JavaPairRDD<Integer, Integer> in) throws Exception {
						return in.sortByKey(false);
					}

				});

		sortedTotalContestantCounts.print();
		
		// make some statistics
		phoneCalls.foreachRDD(new Function<JavaRDD<PhoneCall>, Void>() {

			public Void call(JavaRDD<PhoneCall> rdd) throws Exception {
				Long count = rdd.count();
				//System.out.println( "count : " + count );
				Double throughput = (count.doubleValue()*1000 / batch_duration.doubleValue());
				System.out.println("Current rate = " + throughput
						+ " records / second");
				
				XMemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("localhost:11211"));
			    XMemcachedClient client= (XMemcachedClient) builder.build();
			    client.setPrimitiveAsString(true);
			    
			    Long currentTimeStamp = System.currentTimeMillis();
			    //System.out.println("End time: " + currentTimeStamp);
			    client.add(currentTimeStamp.toString(), 0, throughput);
			     

				return null;
			}

		});	

	
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

}
