package edu.nyu.adbms.repcrec;

import java.util.Map;
import java.util.TreeMap;
/**
 * 
 * @author Sannidhi Jalukar
 *
 */
public class DataLockManager {
   Map<String, Variable> volatileMemory;
   Map<String, Variable> stableStorage;
  
  public DataLockManager() {
    volatileMemory = new TreeMap<String, Variable>();
    stableStorage = new TreeMap<String, Variable>();
  }
  
  /**
   * Grants Read locks
   * @param T
   * @param siteVar
   * @param sVar
   * @return
   */
  public int checkGrantSharedLock(Transaction T, Variable siteVar, Variable sVar) {
    
    //can read
    if(!siteVar.hasExclusiveLock && !sVar.isObsolete) {
      return 1;
    }
    //for a just recovered site
    else if(!siteVar.hasExclusiveLock && sVar.isObsolete) {
      //can read only if the var is not replicated 
      if(!siteVar.isReplicated) {
        return 1;
      }
      return 4; //variable is not obsolete and does not have an exclusive lock    
    }   
    else {
      long results = siteVar.owner.getCreationTime() - T.getCreationTime();
      //blocked
      if (results > 0) {
        return 2;
      }
      
      //abort
      else if(results < 0) {
        for (Variable var : this.volatileMemory.values()) {
          if(var.owner != null){
            if(var.owner.equals(T) || var.sharedOwners.contains(T)) {
                  var.owner = null;
                  var.sharedOwners.remove(T);
                }
              }
        return -1;
      }
      }
    }
    //default
    return 10;
  }

  /**
   * Grants exclusive write locks if possible
   * @param T
   * @param siteVar
   * @return
   */
  public int checkGrantWriteLock(Transaction T, Variable siteVar) {
    int flag =0;
    if(!siteVar.hasExclusiveLock && siteVar.sharedOwners.isEmpty()) {
      return 1; //success write
    }
    else if (!siteVar.hasExclusiveLock) {
      //T1 reads then T1 writes on same variable
      if(siteVar.sharedOwners.size()==1){
        if(siteVar.sharedOwners.get(0).getId().equals(T.getId())) {
          return 1; //success
        }
        else{
          if (siteVar.sharedOwners.get(0).getCreationTime() > T.getCreationTime()){
            return 2;
          }
          else
            return -1;
        }
      }
    
      //check for current T having time greater than existing shared owners
      //if so then using wait die abort the T
      else if (siteVar.sharedOwners.size()!=1){
        for (Transaction SO : siteVar.sharedOwners){
          if(T.getCreationTime() > SO.getCreationTime()){
            flag =1;
          break;
          }
        }
        if(flag==1)
          //abort
          return -1;
        else
          //add to blocked Q
          return 2;
      }
  }
      else{
        //if no shared owners and only exclusive lock , confirm wait die
      long results = siteVar.owner.getCreationTime() - T.getCreationTime();
      //blocked
      if (results > 0) {
        return 2;
      }
      
      //abort
      else if(results < 0) {
        return -1;

      }
      }
    
    //default
    return 10;
    }
  
  public int end(Transaction T){
    Variable vStableStorage;
    //commit all values from volatile to stable storage and release locks
    for (Variable vVolatileMemory : this.volatileMemory.values()) {
      if(vVolatileMemory.owner != null && vVolatileMemory.owner.equals(T)) {
        vStableStorage = stableStorage.get(vVolatileMemory.name);
        vStableStorage.value = vVolatileMemory.value;
        vVolatileMemory.owner = null;
        vVolatileMemory.hasExclusiveLock = false;
        vStableStorage.isObsolete = false;
      }
      if(vVolatileMemory.sharedOwners.contains(T)) {
      vVolatileMemory.sharedOwners.remove(T);
      }
      }
    return  55;
    }
  }
