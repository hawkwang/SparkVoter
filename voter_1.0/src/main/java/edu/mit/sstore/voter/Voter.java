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
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.google.common.base.Optional;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Option;
import scala.Tuple2;

public class Voter extends Receiver<String> {

	// ============= Receiver code that receives data over a socket
	// ==============

	String host = null;
	int port = -1;
	
	public static final int MAX_VOTES = 2;//1000; 
	public static final int NUM_CONTESTANTS = 6; 

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
		
	    Logger.getLogger("org").setLevel(Level.WARN);
	    Logger.getLogger("akka").setLevel(Level.WARN);
	    
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(Integer.valueOf(args[0])));

		jssc.checkpoint(".");

		JavaReceiverInputDStream<String> votes = jssc.receiverStream(new Voter(
				"localhost", 6789));

		// transform text line stream to PhoneCall stream
		JavaDStream<PhoneCall> phoneCalls = votes
				.map(new Function<String, PhoneCall>() {
					public PhoneCall call(String s) {
						return getPhoneCall(s);
					}
				});

		JavaDStream<Long> counts = votes.count();
		counts.print();
	
		
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
		JavaPairDStream<Long, Integer> calls = phoneCalls
				.mapToPair(new PairFunction<PhoneCall, Long, Integer>() {
					public Tuple2<Long, Integer> call(PhoneCall x) {
						return new Tuple2<Long, Integer>(x.phoneNumber, 1);
					}
				});

		// generate the accumulated count for phone numbers
		final JavaPairDStream<Long, Integer> callNumberCounts = calls
				.updateStateByKey(updateFunction);
		//callNumberCounts.print();
		
		JavaPairDStream<Long, PhoneCall> pairVotes = phoneCalls.mapToPair(new PairFunction<PhoneCall, Long, PhoneCall>(){
			public Tuple2<Long, PhoneCall> call(PhoneCall call) throws Exception {
				return new Tuple2<Long, PhoneCall>(call.voteId, call);
			}
		});

		// generate the validate phone numbers, which is still allowed to send vote
		JavaPairDStream<Long, Integer> allowedCalls = callNumberCounts.filter(new Function<Tuple2<Long, Integer>,Boolean>(){

			public Boolean call(Tuple2<Long, Integer> v1) throws Exception {
				if (v1._2() > Voter.MAX_VOTES)
					return false;
				
				return true;
			}
		});
		
		//allowedCalls.print();
		
		// get validate contestant phone calls
		JavaDStream<PhoneCall> validContestantPhoneCalls = phoneCalls
				.filter(new Function<PhoneCall, Boolean>() {
					public Boolean call(PhoneCall call) {
						if ( call.contestantNumber > Voter.NUM_CONTESTANTS )
							return false;
						return true;
					}
				});

		JavaPairDStream<Long, PhoneCall> anotherTemporyPhoneCalls = validContestantPhoneCalls
				.mapToPair(new PairFunction<PhoneCall, Long, PhoneCall>() {
					public Tuple2<Long, PhoneCall> call(PhoneCall x) {
						return new Tuple2<Long, PhoneCall>(x.phoneNumber, x);
					}
				});


		// get validate phone call records
		JavaPairDStream<Long, Tuple2<PhoneCall, Integer>> validatePhoneCalls = anotherTemporyPhoneCalls
				.join(allowedCalls);
		
		//validatePhoneCalls.print();
		

		JavaDStream<PhoneCall> validateCalls = validatePhoneCalls
				.transform(new Function<JavaPairRDD<Long, Tuple2<PhoneCall, Integer>>, JavaRDD<PhoneCall>>() {
					public JavaRDD<PhoneCall> call(
							JavaPairRDD<Long, Tuple2<PhoneCall, Integer>> v1)
							throws Exception {
						JavaRDD<PhoneCall> item = v1
								.map(new Function<Tuple2<Long, Tuple2<PhoneCall, Integer>>, PhoneCall>() {
									public PhoneCall call(
											Tuple2<Long, Tuple2<PhoneCall, Integer>> validItem)
											throws Exception {
										return validItem._2()._1();
									}

								});
						return item;
					}
				});
		
		//validateCalls.print();
		
		//save all votes with redis
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
		
		
		JavaPairDStream<Integer, Integer> contestantNums = windowCalls
				.mapToPair(new PairFunction<PhoneCall, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(PhoneCall x) {
						return new Tuple2<Integer, Integer>(x.contestantNumber, 1);
					}
				});

		JavaPairDStream<Integer, Integer> contestantCounts = contestantNums
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2)
							throws Exception {

						return i1 + i2;
					}
				});
		
		//contestantCounts.print();
		
		// generate the accumulated count for contestants
		JavaPairDStream<Integer, Integer> totalWindowContestantCounts = contestantVotes.updateStateByKey(updateFunction);
		totalWindowContestantCounts.print();

		
		
		// used for sorting
		PairFunction<Tuple2<Integer, Integer>, Integer, Integer> swapFunction = new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> in) {
				return in.swap();
			}
		};

		JavaPairDStream<Integer, Integer> swappedTotalWindowContestantCounts = totalWindowContestantCounts
				.mapToPair(swapFunction);
		//swappedTotalWindowContestantCounts.print();

		JavaPairDStream<Integer, Integer> sortedTotalWindowContestantCounts = swappedTotalWindowContestantCounts
				.transformToPair(new Function<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Integer>>() {

					public JavaPairRDD<Integer, Integer> call(
							JavaPairRDD<Integer, Integer> in) throws Exception {
						return in.sortByKey(false);
					}

				});

		sortedTotalWindowContestantCounts.print();
	
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

}
