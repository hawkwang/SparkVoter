package edu.mit.sstore.voter;

import java.io.*;
import java.net.*;
import java.util.*;

import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

class TCPServer
{

   public static void main(String argv[]) throws Exception
   {
        if (argv.length < 3) {
          System.err.println("Usage: TCPServer <inputfile> <inputrate> ");
          System.exit(1);
        }

         TCPServer svr = new TCPServer(argv[0], Integer.parseInt(argv[1]), Integer.parseInt(argv[2]));

         ServerSocket welcomeSocket = new ServerSocket(6789);

		System.out.println("Ready to get connection and send votes ... ");
		
		Calculator calculator = new Calculator();
		new Thread(calculator).start();


         while(true)
         {
            Socket connectionSocket = welcomeSocket.accept();
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            svr.send(outToClient);
            //svr.sendOneVoteEachDuration(outToClient);
            //svr.rateControlledRunLoop(outToClient);

            //String voteinfo;
            //for(long i=0; i< 400000l; i++)
            //{
            //	voteinfo = "" + i +" " + i +" " + i + " " + i + '\n';
            //	outToClient.writeBytes(voteinfo);
            //    Thread.sleep(100);
            //}
         }
    }


    // attributes
    private ArrayList<String> m_votes;
    
    private int m_position = 0;
    private int m_flag = 0;
    private int sendrate = 1000;
    private boolean stop = false;


    // methods
    public TCPServer(String inputfile, int sendrate, int flag)
    {
	m_position = 0;
        m_flag = flag;

        String  line = null;
        m_votes = new ArrayList<String>();
        try{
           // open input stream test.txt for reading purpose.
           InputStream fis=new FileInputStream(inputfile);
           BufferedReader br = new BufferedReader( new InputStreamReader(fis) );
           while ((line = br.readLine()) != null) {
              //System.out.println(line);
              m_votes.add(line);
           }         
        }catch(Exception e){
           e.printStackTrace();
        }        

        this.sendrate = sendrate;
    }

    public void send(DataOutputStream outToClient) throws Exception
    {
        if(m_flag==0)
            sendOneVoteEachDuration(outToClient);
        else
            rateControlledRunLoop(outToClient);
    }


    public void sendOneVoteEachDuration(DataOutputStream outToClient) throws Exception
    {
        int duration = this.sendrate;
        //this.reset();

        while(true)
        {
            if(this.hasMoreVotes()==true)
            {
                String vote = this.nextVoteString();
                long current = System.currentTimeMillis();
                System.out.println("Sending - " + vote + " @ " + current );
                //System.out.println("Sending - " + vote);
                String tuple = vote + "\n";
                outToClient.writeBytes(tuple);
                Thread.sleep(duration);
            }
            else
                break;
        }
    }

    public void rateControlledRunLoop(DataOutputStream outToClient) throws Exception
    {

        this.reset();

        int transactionRate= this.sendrate;
        
        double txnsPerMillisecond = transactionRate / 1000.0;
        //System.out.println("txn per millisecond - " + txnsPerMillisecond);
        
        long lastRequestTime = System.currentTimeMillis();
        
        boolean hadErrors = false;

        String tuple = null;
        
        long counter = 0;
        boolean beStop = false;
        
        while (true) {
            final long now = System.currentTimeMillis();
            final long delta = now - lastRequestTime;
            if (delta > 0) {
                final int transactionsToCreate = (int) (delta * txnsPerMillisecond);
                //System.out.println("transactionsToCreate - " + transactionsToCreate);

                if (transactionsToCreate < 1) {
                    Thread.sleep(10);
                    continue;
                }

                try {
                    for (int i = 0; i < transactionsToCreate; i++) 
                    {
                        if(stop==true)
                        {
                            beStop = true;
                            break;
                        }
                        
                        if(this.hasMoreVotes()==true)
                        {
                            tuple = this.nextVoteString() + "\n";
                            //tuple = "" + i +" " + i +" " + i + " " + i + '\n';
                            long current = System.currentTimeMillis();
                            outToClient.writeBytes(tuple); 
                            counter++;
                            if(counter % 1000 == 0)
                            	System.out.println("Sending - " + counter + " @ " + current );
                        }
                        else
                            beStop = true;
                    } 
                } catch (final Exception e) {
                    if (hadErrors) return;
                    hadErrors = true;
                    Thread.sleep(5000);
                } finally {
                }
                
                if( beStop == true )
                {
                    break;
                }
            }
            else {
                Thread.sleep(25);
            }

            lastRequestTime = now;
            
        } // WHILE

    }


    public String nextVoteString()
    {
    	String call = "";
        if(hasMoreVotes()==false)
            return "";
        
        //System.out.println("get call at position: " + m_position);
        call = m_votes.get(m_position);
        m_position++;
        
        return call;

    }

    public boolean hasMoreVotes()
    {
        int size = this.m_votes.size();
        //System.out.println("size and pos: " + size +"-"+m_position);
        if(m_position >= size)
            return false;
        else
            return true;
    }

    public boolean isEmpty()
    {
        return m_votes.isEmpty();
    }
    
    public int size()
    {
        return this.m_votes.size();
    }

    public void reset()
    {
        m_position = 0;
    }

}


