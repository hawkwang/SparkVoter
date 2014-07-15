import java.io.*;
import java.net.*;

class TCPServer
{

   public static void main(String argv[]) throws Exception
   {
        if (argv.length < 1) {
          System.err.println("Usage: TCPServer <inputrate> ");
          System.exit(1);
        }

         TCPServer svr = new TCPServer(Integer.parseInt(argv[0]));

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

    private int sendrate;
    private boolean stop = false;

    public TCPServer(int sendrate)
    {
        this.sendrate = sendrate;
    }

    public void rateControlledRunLoop(DataOutputStream outToClient) throws Exception
    {
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
                        
                        tuple = "" + i +" " + i +" " + i + " " + i + '\n';
                        outToClient.writeBytes(tuple); 
                        counter++;
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

}


