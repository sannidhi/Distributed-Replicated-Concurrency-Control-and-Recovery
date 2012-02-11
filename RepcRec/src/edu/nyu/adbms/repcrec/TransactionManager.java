package edu.nyu.adbms.repcrec;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 
 * @author krutikashah
 *
 */

public class TransactionManager {

  private SiteManager siteManager;
  Transaction transaction;
  private Map<Integer,Transaction> transactions;
  private List<Transaction> blockedQueue;
  private ArrayList<Instruction> allInstructions;
  
  public TransactionManager(SiteManager siteManager
      ,ArrayList<Instruction> allInstructions) {
    this.siteManager = siteManager;
    blockedQueue = new ArrayList<Transaction>();
    transactions = new HashMap<Integer,Transaction>();
    this.allInstructions = allInstructions;
    
  }
   

  /**
   * executeTransaction selects a sequentially chosen available site to execute the
   * instruction just read from the input file
   * 
   * @param instruction is the current instruction to be executed
   * @throws InterruptedException 
   * 
   */
  public int executeTransaction(Instruction instruction)  {
      if(instruction.getOperationType().equals("begin")) {
        System.out.println("T" +instruction.getTransactionId()+ " begins");
      transaction = new Transaction(instruction.getTransactionId(), false);
//      siteManager.assignSites(transaction);
      Date d = new Date();
      long t = d.getTime();
  
      transaction.setCreationTime(System.nanoTime()); 
       transactions.put(instruction.getTransactionId(), transaction);
    }
    else if(instruction.getOperationType().equals("beginRO")) {
      transaction = new Transaction(instruction.getTransactionId(), true);
      System.out.println("T" +instruction.getTransactionId()+ " begins");
  //    siteManager.assignSites(transaction);
      Date d = new Date();
      long t = d.getTime();
      transaction.setCreationTime(t);
      transactions.put(instruction.getTransactionId(), transaction);
    }
    else if(instruction.getOperationType().equals("R")) {
      transaction = transactions.get(instruction.getTransactionId());
      transaction.setCurrentInstruction(instruction);
      siteManager.assignSites(transaction);
      int result = siteManager.executeInstruction(transaction); 
      if(result == 2 && !blockedQueue.contains(transaction)) {
        blockedQueue.add(transaction);
      } 
      else if(result == -1) {
        //abort
        Iterator<Instruction> iter  = allInstructions.iterator();
        while(iter.hasNext()) {
          Instruction i = iter.next();
          if(i.getTransactionId().equals(transaction.getId()) ){
            iter.remove();
          }
        }
        transactions.remove(transaction.getId());
        blockedQueue.remove(transaction);
        return 3;
      }
      return result;
    }
    else if(instruction.getOperationType().equals("W")) {
      transaction = transactions.get(instruction.getTransactionId());
      transaction.setCurrentInstruction(instruction);
      siteManager.assignSites(transaction);
      int result = siteManager.executeInstruction(transaction);
      if(result == 2 && !blockedQueue.contains(transaction)) {
        blockedQueue.add(transaction);
      } 
      else if(result == -1) {
        //abort
        System.out.println("Transaction " +transaction.getId()+ " aborted because no lock " +
        		"on variable " +instruction.getVariable() + " could be obtained" );
       Iterator<Instruction> iter  = allInstructions.iterator();
        while(iter.hasNext()) {
          Instruction i = iter.next();
          if(i.getTransactionId().equals(transaction.getId()) ){
            iter.remove();
          }
        }
        transactions.remove(transaction.getId());
        blockedQueue.remove(transaction);
        return 3;
      }
      return result;
    }
    else if(instruction.getOperationType().equals("end")) {
      transaction = transactions.get(instruction.getTransactionId());
      transaction.setCurrentInstruction(instruction);
      siteManager.assignSites(transaction);
      int r = siteManager.executeInstruction(transaction);
        System.out.println("T" +transaction.getId()+ "committed successfully");
      return r;

    }
    
    else if(instruction.getOperationType().equals("dump")) {
      Transaction transaction = new Transaction();
      transaction.setCurrentInstruction(instruction);
      transactions.put(instruction.getTransactionId(), transaction);

      //all v at all sites
      if(instruction.getDumpId() != 0) {
        return(siteManager.executeInstruction(transaction));
      }
      else if(instruction.getVariable() != null) {
        return (siteManager.dump(instruction.getVariable()));
      }
      else {
        return (siteManager.dump());
      }

    }
    
    else if(instruction.getOperationType().equals("fail")) {
      Transaction transaction = new Transaction();
      transaction.setCurrentInstruction(instruction);
      transactions.put(instruction.getTransactionId(), transaction);

//      transaction.setCurrentInstruction(instruction);
      Set<Transaction> result = siteManager.fail(transaction);
      for(Transaction t : result) {
        transactions.remove(t.getId());
        blockedQueue.remove(t);
        System.out.println("Transaction " +t.getId()+ " aborted since " +
            "site " + instruction.getFailId() + " failed");
      
      Iterator<Instruction> iter  = allInstructions.iterator();
      while(iter.hasNext()) {
        Instruction i = iter.next();
        if(i.getTransactionId().equals(t.getId()) ){
          iter.remove();
        }
        }
      }
   }
    
    else if(instruction.getOperationType().equals("recover")) {
      siteManager.recover(instruction.getRecoverId());
    }
    return 0;
  }

  public List<Transaction> getBlockedQueue() {
    return blockedQueue;
  }

}
