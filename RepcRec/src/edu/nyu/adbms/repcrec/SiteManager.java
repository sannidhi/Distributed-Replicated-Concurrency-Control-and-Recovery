package edu.nyu.adbms.repcrec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Sannidhi Jalukar
 *
 */
public class SiteManager {  
  
	private Map<Integer,Site> sitesInt;
	private Site site;
	private DataLockManager evenSiteDataLockManager;
	private DataLockManager oddSiteDataLockManager;
		
	//many tranaction belong to one site but one T belogn to one S
	private Map<Transaction,Integer> siteTrasactionRel;
	
	
	public Map<Transaction, Integer> getSiteTrasactionRel() {
    return siteTrasactionRel;
  }

 
	public SiteManager() {
		this.siteTrasactionRel = new HashMap<Transaction , Integer>();
		this.sitesInt = new HashMap<Integer, Site>();
		
		for(Integer i = 1; i <=10 ; i++) {
		  init(i);
		}		
	}
	
	
	private void init(Integer siteId) {
	  
	  oddSiteDataLockManager = new DataLockManager();
    evenSiteDataLockManager = new DataLockManager();
    Variable v,volV;
    //odd : even
    if(siteId %2 != 0) {      
    for(Integer i = 1; i <= 20; i++) {
      if(i % 2 == 0) {
      String  name = "x" + i;
        v = new Variable(name, 10 * i);
        volV = new Variable (name);
        v.isObsolete = false;
       oddSiteDataLockManager.stableStorage.put(name, v);
       oddSiteDataLockManager.volatileMemory.put(name, volV);


      }
    }
    site = new Site(siteId,oddSiteDataLockManager);

    }
    //even : even + 2odd
    else {    
    for(Integer i = 1; i <= 20; i++) {
      if(i % 2 == 0 || siteId == 1 + i % 10) {
        
     String name = "x" + i;
      v = new Variable(name, 10 * i);
      volV = new Variable (name);
      if(siteId == 1 + i % 10) {
        v.isReplicated = false;
        volV.isReplicated = false;
      }
      else {
        v.isReplicated = true;
        volV.isReplicated = true;
      }
        
      v.isObsolete = false;
      evenSiteDataLockManager.stableStorage.put(name, v);
      evenSiteDataLockManager.volatileMemory.put(name, volV);
      }
    }
    site = new Site(siteId,evenSiteDataLockManager);

    }	 
        sitesInt.put(siteId, site);    
	}
	
	
	public void assignSites(Transaction T) {
	  Site selectedSite;
	  if (T.getCurrentInstruction().getOperationType().equalsIgnoreCase("end")){
	    for(Integer i = 1; i<=sitesInt.size(); i++) {
	      selectedSite = sitesInt.get(i);
	      if(selectedSite.isAvailable) {
	        T.getCurrentInstruction().setSiteId(selectedSite.id);
	        siteTrasactionRel.put(T, i);
	        break;
	      }
	    }
	  }
	  else {
	  for(Integer i = 1; i<=sitesInt.size(); i++) {
	    selectedSite = sitesInt.get(i);
	    if(selectedSite.isAvailable && selectedSite.dataLockManager.stableStorage.containsKey(T.getCurrentInstruction().getVariable())) {
	      T.getCurrentInstruction().setSiteId(selectedSite.id);
	      siteTrasactionRel.put(T, i);
	      break;
	    }
	  }	  
	  }
	}
	
	/**
	 * The site manager manages all the 10 sites  and is responsible for
	 * communicating between the TM and all the sites
	 * @param transaction currently executing transaction
	 * @return
	 */
	public int executeInstruction(Transaction transaction) {
	 
	  Instruction curInst = transaction.getCurrentInstruction();
	  Integer siteId = siteTrasactionRel.get(transaction);
	  Site executingSite = sitesInt.get(siteId);
	  if(curInst.getOperationType().equals("R")) {
	     int result = (executingSite.R(transaction, transaction.isReadOnly()));
	     //broadcast new shared owner on all sites
	     if (result == 1) {
	       for(Integer i =1 ;i<=sitesInt.size();i++) {
	          Site site = sitesInt.get(i);
	          if(site.dataLockManager.volatileMemory.containsKey(curInst.getVariable()) && site.isAvailable) {
	          Variable v = site.dataLockManager.volatileMemory.get(curInst.getVariable());
	          v.sharedOwners.add(transaction);
	     }
	    
	  }
	     }
	     //look for a site whose isobsolete is false
	     else if (result == 4) {
	       for(Integer i =1 ;i<=sitesInt.size();i++) {
           Site site = sitesInt.get(i);
           if(site.isAvailable) {
           Variable vi = site.dataLockManager.stableStorage.get(curInst.getVariable());
           if(!vi.isObsolete) {
             result = site.R(transaction, transaction.isReadOnly());
             if (result==1)
               break;
           }
           }
	       }
	       if (result == 1) {
	         for(Integer i =1 ;i<=sitesInt.size();i++) {
	            Site site = sitesInt.get(i);
	            if(site.dataLockManager.volatileMemory.containsKey(curInst.getVariable()) && site.isAvailable) {
	            Variable v = site.dataLockManager.volatileMemory.get(curInst.getVariable());
	            v.sharedOwners.add(transaction);
	       }
	    }
	       }
	       
	     }
	     
	     else if(result == -1){
	     //  System.out.println("site aborting" );
	       for(Integer i =1 ;i<=sitesInt.size();i++) {
           Site site = sitesInt.get(i);
           if(site.isAvailable) {
             for (Variable var : site.dataLockManager.volatileMemory.values()) {
               if(var.owner != null) {
                if (var.owner.equals(transaction)) {
                  var.owner=null;
                  var.hasExclusiveLock = false;
                  var.sharedOwners.remove(transaction);
                }
           }
             }
         
       }
	       
	     }
	     }	     return result;
	  }

	  else if(curInst.getOperationType().equals("W")) {
	    int result = executingSite.W(transaction);
	    if (result == 1) {
	      String var = transaction.getCurrentInstruction().getVariable();
	      for(Integer i =1 ;i<=sitesInt.size();i++) {
	        Site site = sitesInt.get(i);
	        if(site.dataLockManager.volatileMemory.containsKey(var) && site.isAvailable) {
	        Variable v = site.dataLockManager.volatileMemory.get(var);
	        v.value = transaction.getCurrentInstruction().getValue();
	        v.owner = transaction;
	        v.hasExclusiveLock = true;
	      }
	      }
	    }
	    //release locks on all variables on all sites whic have owner/shared lock as T 
	    else if(result == -1) {
	       for(Integer i =1 ;i<=sitesInt.size();i++) {
	          Site site = sitesInt.get(i);
	          if(site.isAvailable) {
	            for (Variable var : site.dataLockManager.volatileMemory.values()) {
	              if(var.owner != null) {
	               if (var.owner.equals(transaction)) {
	                 var.owner=null;
	                 var.hasExclusiveLock = false;
	                 var.sharedOwners.remove(transaction);
	               }
	          }
	            }
	        
	      }
	    }
	  }
	     return (result);      

	  }
	  
	  else if(curInst.getOperationType().equals("end")) {
	    System.out.println("T" +curInst.getTransactionId()+ " ends");
	    for(Integer i = 1; i<=sitesInt.size(); i++) {
	      Site site = sitesInt.get(i);
	      if(site.isAvailable) {
	      site.end(transaction);
	      }
	    }
	    return 55;
	  }
	  
	  else if(curInst.getOperationType().equals("dump")) {
	    Integer tempsiteid = transaction.getCurrentInstruction().getDumpId();
	    if(this.sitesInt.get(tempsiteid).isAvailable) {
	      Site executeDump = sitesInt.get(tempsiteid);
	       return (executeDump.dump(transaction));
	    }
	    else
	      return 0; //failure
	  }
	  

	  return 100;
	}
	
	public Set<Transaction> fail(Transaction transaction) {
	  Integer failID = transaction.getCurrentInstruction().getFailId();
	  System.out.println("Site " +failID + " fails");
    Site failSite = sitesInt.get(failID);
    failSite.isAvailable = false;
    Set<Transaction> keys = new HashSet<Transaction>();

    for (Variable var : failSite.dataLockManager.volatileMemory.values()) {
      if(var.owner!=null)
        keys.add(var.owner);
      var.hasExclusiveLock = false;
      var.owner = null;
     // if(var.sharedOwners.contains(transaction)){
      Iterator<Transaction> iter = var.sharedOwners.iterator();
      while(iter.hasNext()) {
        Transaction t = iter.next();
        keys.add(t);
        System.out.println("SL on "+t.getId() +" removed");
        iter.remove();
      }

    }
    
    
 
    for(Transaction T : keys) {
      for(Integer s = 1;s<=sitesInt.size();s++) {
        Site site = sitesInt.get(s);
        if(site.isAvailable) {
        DataLockManager dlm = site.dataLockManager;
        for (Variable var : dlm.volatileMemory.values()) {
          var.hasExclusiveLock = false;
          if(var.owner!= null && var.owner.equals(T)) {
            var.owner = null;
          }
          var.sharedOwners.remove(T);
        }
        }
      }}
    return keys;
   }
    
   

	public void recover(Integer id) {
	   System.out.println("Site " +id + " recovers");
	  sitesInt.get(id).isAvailable = true;
    for (Variable var : sitesInt.get(id).dataLockManager.stableStorage.values()) {
      var.isObsolete = true;
    }

	}
	
	
	public int dump() {
    for (int i=1;i<=sitesInt.size();i++){
      Site site = sitesInt.get(i);
      if (site.isAvailable){
        for(int k = 1;k<=20;k++)
      {
          String name = "x"+k;
          if(site.dataLockManager.stableStorage.containsKey(name)) {
          Variable v = site.dataLockManager.stableStorage.get(name);
        System.out.println("Site: " +i+ " variable "+v.name+" = "+v.value);
      }
      }
      }
      else{
        System.out.println("Site: "+i+" is down");
      }
    }
    return 1;
  }
	
	public int dump(String xj){
    for (int i=1;i<=sitesInt.size();i++){
      Site site = sitesInt.get(i);
      if (site.dataLockManager.stableStorage.containsKey(xj)){
        if (site.isAvailable){
          System.out.println("Site: " +i+ " value= " +site.dataLockManager.stableStorage.get(xj).value);
        }
        else{ 
          System.out.println("Site: "+i+ " is down");
        }
      }
    }
    return 1;
  }

}
