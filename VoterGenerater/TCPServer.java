import java.io.*;
import java.net.*;
import java.util.*;

class TCPServer
{

   public static void main(String argv[]) throws Exception
   {
        if (argv.length < 2) {
          System.err.println("Usage: TCPServer <inputfile> <inputrate> ");
          System.exit(1);
        }

         TCPServer svr = new TCPServer(argv[0], Integer.parseInt(argv[1]));

         ServerSocket welcomeSocket = new ServerSocket(6789);

         while(true)
         {
            Socket connectionSocket = welcomeSocket.accept();
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            svr.rateControlledRunLoop(outToClient);

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
    private int sendrate = 1000;
    private boolean stop = false;


    // methods
    public TCPServer(String inputfile, int sendrate)
    {
	m_position = 0;

        String  line = null;
        m_votes = new ArrayList<String>();
        try{
           // open input stream test.txt for reading purpose.
           InputStream fis=new FileInputStream(inputfile);
           BufferedReader br = new BufferedReader( new InputStreamReader(fis) );
           while ((line = br.readLine()) != null) {
              System.out.println(line);
              m_votes.add(line);
           }         
        }catch(Exception e){
           e.printStackTrace();
        }        

        this.sendrate = sendrate;
    }

    public void rateControlledRunLoop(DataOutputStream outToClient) throws Exception
    {

        this.reset();

        int transactionRate= this.sendrate;
        
        double txnsPerMillisecond = transactionRate / 1000.0;
        System.out.println("txn per millisecond - " + txnsPerMillisecond);
        
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
                System.out.println("transactionsToCreate - " + transactionsToCreate);

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
                            outToClient.writeBytes(tuple); 
                            counter++;
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


