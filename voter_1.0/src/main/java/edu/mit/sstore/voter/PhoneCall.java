package edu.mit.sstore.voter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class PhoneCall implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1409337187611088655L;
    
    public long voteId;
    public int contestantNumber;
    public long phoneNumber;
    public int timestamp;
    
    protected PhoneCall(long voteId, int contestantNumber, long phoneNumber, int timestamp) {
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.timestamp = timestamp;
    }
    
    private void writeObject(ObjectOutputStream s) throws IOException {
        s.writeLong(this.voteId);
        s.writeInt(this.contestantNumber);
        s.writeLong(this.phoneNumber);
        s.writeInt(this.timestamp);
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        this.voteId = s.readLong();
        this.contestantNumber = s.readInt();
        this.phoneNumber = s.readLong();
        this.timestamp = s.readInt();
    }
    
    public void debug() {
        System.out.println("call : " + this.voteId + "-" + this.phoneNumber + "-" + this.contestantNumber + "-" + this.timestamp);
    }
    
    public String getString()
    {
        return "" + this.voteId + " " + this.phoneNumber + " " + this.contestantNumber + " " + this.timestamp +"\n";
    }
    
    public String toString()
    {
        return "" + this.voteId + " " + this.phoneNumber + " " + this.contestantNumber + " " + this.timestamp +"\n";
    }

    public String getContent()
    {
        return this.voteId + " " + this.phoneNumber + " " + this.contestantNumber + " " + this.timestamp;
    }

}
