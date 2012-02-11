package edu.nyu.adbms.repcrec;

import java.util.ArrayList;
import java.util.List;


public class Variable {
	String name;
	int value;
	//int lastCommittedValue;
	//boolean isLocked;
	//boolean hasSharedLock;
	boolean hasExclusiveLock;
	boolean isObsolete;
	Transaction owner;
	boolean isReplicated;
	List<Transaction> sharedOwners; 
	
	public Variable (String name){
	  this.name=name;
	  this.hasExclusiveLock =false;
    //for write lock
    this.owner = null;
    //for shared locks
    this.sharedOwners = new ArrayList<Transaction>();
	  
	}
	
	public Variable(String name, int initValue) {
		this.name = name;
		this.value = initValue;
	//	this.lastCommittedValue = initValue;
		//this.isLocked = false;
		//this.hasSharedLock = false;
		this.hasExclusiveLock =false;
		
		//for write lock
		this.owner = null;
		
    //for shared locks
		this.sharedOwners = new ArrayList<Transaction>();
		
		
		
	}
	
	/*public boolean lockVariable(Transaction t) {
		try {
	//		this.isLocked = true;
			this.owner = t;
			return true;
		} catch(Exception e) {
			return false;
		}
	}
	
	public boolean isTransactionAuthorizedForWrite(Transaction t) {
		if (this.isLocked == true) {
			if (this.owner.hashCode() == t.hashCode())
				return true;
			else
				return false;
		} else {
			return false;
		}
	}*/
	
	
}
