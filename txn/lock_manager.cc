// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
  LockRequest lr(EXCLUSIVE, txn);

  if (lock_table_.count(key)) {
    lock_table_[key]->push_back(lr);
  } else {
    deque<LockRequest> *lock_queue = new deque<LockRequest>();
	lock_queue->push_back(lr);
    lock_table_[key] = lock_queue;
  }

  // Check if the transraction acquired the lock
  if (lock_table_[key]->size() == 1) {
    return true;
  } else {
    if (txn_waits_.count(txn)) {
      txn_waits_[txn] = 1;
    } else {
     txn_waits_[txn]++;
    }
    return false;
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
  bool Locked;

  //request key
  deque<LockRequest> *requests = lock_table_[key];

  // Remove txn from request
  deque<LockRequest>::iterator i;
  i = requests->begin(); 
  while (i != requests->end()) {
	  if (i->txn_ == txn) {
		  Locked = (requests->front().txn_ == txn);
		  requests->erase(i);
		  break;
	  }
	  i++;
  }

  // Start next txn if a lock is acquired
  if (requests->size() >= 1 && Locked) {
    Txn *start = requests->front().txn_;
    if (txn_waits_[start]-1 == 0) {
		txn_waits_[start] -= 1;
		ready_txns_->push_back(start);
	}
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 638:
  //
  // Implement this method!
  // init vector-chan
  deque<LockRequest>::iterator i;
  owners->clear();

  // fill the owners vector
  if (lock_table_[key]->size() != 0)
    owners->push_back(lock_table_[key]->begin()->txn_);

  if (owners->empty()) {
  	  return UNLOCKED;
  } else {
	  return EXCLUSIVE;
  }
}
