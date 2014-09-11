package edu.mit.sstore.voter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.KeyIterator;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;

public class Calculator implements Runnable {
	
	Calculator()
	{
	}

	public void run() {

		// connect to memcached
		try {
			XMemcachedClientBuilder builder = new XMemcachedClientBuilder(
					AddrUtil.getAddresses("localhost:11211"));
			XMemcachedClient client;
			client = (XMemcachedClient) builder.build();
			client.setPrimitiveAsString(true);
			client.flushAll(); 

			rateControlledRunLoop(client);
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private boolean func(XMemcachedClient client) {
		boolean stop = false;

		List<String> keys = new ArrayList<String>();
		KeyIterator it;
		try {
			it = client.getKeyIterator(AddrUtil
					.getOneAddress("localhost:11211"));
			while (it.hasNext()) {
				String key = it.next();
				keys.add(key);
			}
			// Collections.sort(keys);

			// System.out.println("Sorted List: " + keys);
			Map<String, Object> map = client.get(keys);
			Map sortedMap = new TreeMap();
			if (map != null)
				sortedMap.putAll(map);

			List<Entry> entryList = new ArrayList<Entry>(sortedMap.entrySet());
			//System.out.println("Sorted map: " + entryList);
			int size = entryList.size();
			if (size > 3) {
				Double value = 0.0;
				value += Double.parseDouble((String) entryList.get(size - 1).getValue());
				value += Double.parseDouble((String) entryList.get(size - 2).getValue());
				value += Double.parseDouble((String) entryList.get(size - 3).getValue());
				//System.out.println("value: " + value);
				if (value.compareTo(new Double("0.0")) == 0) {
					stop = true;
					
					// make statistics here
					makeStatistics(entryList);
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return stop;
	}
	
	private void makeStatistics(List<Entry> entryList) {
		// make statistic here
		
		System.out.println("Make statistics ... ");
		System.out.println("Sorted map: " + entryList);
		
		int count = 0;
		Double total = 0.0;
		Double max = Double.MIN_VALUE;
		Double min = Double.MAX_VALUE;
		Double average = 0.0;
		Double stddev = 0.0;
		ArrayList<Double> values = new ArrayList<Double>();
		
		int size = entryList.size();
		for(int i=0; i<(size-4); i++)
		{
			Double value = Double.parseDouble((String) entryList.get(i).getValue());
			if(value.compareTo(new Double("0.0")) != 0)
			{
				values.add(value);
				total += value;
				count++;
				if (value > max)
					max = value;
				if (value < min)
					min = value;
			}
		}
		
		average = total / count;
		Double[] valueArr = new Double[values.size()];
		valueArr = values.toArray(valueArr);
		stddev = Math.sqrt(var(valueArr));
		
        String strOutput = "effective batch number:" + String.format("%2d", count);
        strOutput += " max: " + max;
        strOutput += " min: " + min;
        strOutput += " average: " + average;
        strOutput += " stddev: " + stddev;
        strOutput += " ";
		
		System.out.println( strOutput );
		
	}
	
    private double var(Double[] valueArr) {
        if (valueArr.length == 0) return Double.NaN;
        double avg = mean(valueArr);
        double sum = 0.0;
        for (int i = 0; i < valueArr.length; i++) {
            sum += (valueArr[i] - avg) * (valueArr[i] - avg);
        }
        return sum / (valueArr.length - 1);
    }
    
    private double mean(Double[] valueArr) {
        if (valueArr.length == 0) return Double.NaN;
        double sum = sum(valueArr);
        return sum / valueArr.length;
    }
    
    private double sum(Double[] a) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i];
        }
        return sum;
    }

	private void rateControlledRunLoop(XMemcachedClient client) {
		
        int transactionRate= 1000;
        
        double txnsPerMillisecond = transactionRate / 1000.0;
        
        long lastRequestTime = System.currentTimeMillis();
        
        boolean hadErrors = false;

        boolean beStop = false;
        
        while (true) {
            final long now = System.currentTimeMillis();
            final long delta = now - lastRequestTime;
            if (delta > 0) {
            	// check if the 
            	//System.out.println("Checking ... ");
        		
            	beStop = func(client);
                
                if( beStop == true )
                {
                    break;
                }
            }
            else {
                try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }

            lastRequestTime = now;
            
        } // WHILE
        
        // make statistic here
        
		
		
	}

}
