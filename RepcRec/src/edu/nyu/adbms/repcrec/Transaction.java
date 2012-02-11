package edu.nyu.adbms.repcrec;

import java.sql.Timestamp;
import java.util.Date;

import javax.print.attribute.standard.DateTimeAtCreation;

public class Transaction {
  
  //private String name;
  private Integer id;
  private boolean isReadOnly;
  private Instruction currentInstruction;
  private long creationTime;
  private Timestamp timestamp;
  
  

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public Transaction() {
    
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }


  public long getCreationTime() {
    return creationTime;
  }


  public void setCreationTime(long t) {
    this.creationTime = t;
  }


  public Transaction(Integer transactionId, boolean isReadOnly) {
    id = transactionId;
    this.isReadOnly = isReadOnly;
  }


  public Integer getId() {
    return id;
  }


  public boolean isReadOnly() {
    return isReadOnly;
  }


  public Instruction getCurrentInstruction() {
    return currentInstruction;
  }

  public void setCurrentInstruction(Instruction currentInstruction) {
    this.currentInstruction = currentInstruction;
  }
  
  
}
