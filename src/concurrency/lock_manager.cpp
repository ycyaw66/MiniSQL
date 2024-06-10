#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  // 读未提交不需要加共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }
  LockPrepare(txn, rid);
  LockRequestQueue &req_queue = lock_table_[rid];
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
  // 等待其他进程释放排他锁，不会用wait，借鉴了原框架代码
  if (req_queue.is_writing_) {
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool { return txn->GetState() == TxnState::kAborted || !req_queue.is_writing_; });
  }
  // 死锁检测
  CheckAbort(txn, req_queue);
  txn->GetSharedLockSet().insert(rid);
  req_queue.sharing_cnt_++;
  auto ite = req_queue.GetLockRequestIter(txn->GetTxnId());
  ite->granted_ = LockMode::kShared;
  return true;
}

bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockPrepare(txn, rid);
  LockRequestQueue &req_queue = lock_table_[rid];
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
  // 等待其他进程释放排他锁和共享锁
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 0) {
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool { return txn->GetState() == TxnState::kAborted || !req_queue.is_writing_ && req_queue.sharing_cnt_ == 0; });
  }
  CheckAbort(txn, req_queue);
  txn->GetExclusiveLockSet().insert(rid);
  req_queue.is_writing_ = true;
  auto ite = req_queue.GetLockRequestIter(txn->GetTxnId());
  ite->granted_ = LockMode::kExclusive;
  return true;
}

bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockPrepare(txn, rid);
  LockRequestQueue &req_queue = lock_table_[rid];
  if (req_queue.is_upgrading_) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }
  auto ite = req_queue.GetLockRequestIter(txn->GetTxnId());
  // 无需升级
  if (ite->lock_mode_ == LockMode::kExclusive || ite->granted_ == LockMode::kExclusive) {
    return true;
  }
  ite->lock_mode_ = LockMode::kExclusive;
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 1) {
    req_queue.is_upgrading_ = true;
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool { return txn->GetState() == TxnState::kAborted || (!req_queue.is_writing_ && 1 == req_queue.sharing_cnt_); });
  }
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.is_upgrading_ = false;
  }
  CheckAbort(txn, req_queue);
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().insert(rid);
  req_queue.sharing_cnt_--;
  req_queue.is_upgrading_ = false;
  req_queue.is_writing_ = true;
  ite->granted_ = LockMode::kExclusive;
  return true;
}

bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue &req_queue = lock_table_[rid];
  auto ite = req_queue.GetLockRequestIter(txn->GetTxnId());
  auto lock_mode = ite->granted_;
  req_queue.EraseLockRequest(txn->GetTxnId());
  if (lock_mode == LockMode::kShared) {
    req_queue.sharing_cnt_--;
    req_queue.cv_.notify_all();
    txn->GetSharedLockSet().erase(rid);
  } else if (lock_mode == LockMode::kExclusive) {
    req_queue.is_writing_ = false;
    req_queue.cv_.notify_all();
    txn->GetExclusiveLockSet().erase(rid);
  }
  // 更新2PL的阶段
  if (txn->GetState() == TxnState::kGrowing) {
    txn->SetState(TxnState::kShrinking);
  }
  return true;
}

void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    // lock_table_.emplace(rid, LockRequestQueue());
    // 不知道为啥上面会CE，只能借鉴框架原来的代码
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  revisited_node_ = INVALID_TXN_ID;
  visited_set_.clear();
  while (!visited_path_.empty()) {
    visited_path_.pop();
  }
  std::set<txn_id_t> all_txn;
  for (auto [t1, neighbor] : waits_for_) {
    all_txn.insert(t1);
    for (auto t2 : neighbor) {
      all_txn.insert(t2);
    }
  }
  for (auto txn_id : all_txn) {
    if (dfs(txn_id)) {
      // 找最年轻的事务id
      newest_tid_in_cycle = revisited_node_;
      while (!visited_path_.empty()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }
  return false;
}

bool LockManager::dfs(txn_id_t txn_id) {
  if (visited_set_.find(txn_id) != visited_set_.end()) {
    revisited_node_ = txn_id;
    return true;
  }
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);
  for (auto neighbor : waits_for_[txn_id]) {
    if (dfs(neighbor)) {
      return true;
    }
  }
  visited_path_.pop();
  visited_set_.erase(txn_id);
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id : txn->GetSharedLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id : txn->GetExclusiveLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    // 每次唤醒时即时构建和销毁图表，而不是维护图表
    waits_for_.clear();
    for (auto &[row_id, req_queue] : lock_table_) {
      // 根据锁类型添加边
      for (auto lock_req : req_queue.req_list_) {
        for (auto nxt_lock_req : req_queue.req_list_) {
          if (lock_req.txn_id_ == nxt_lock_req.txn_id_) {
            continue;
          }
          if (lock_req.lock_mode_ == LockMode::kShared && lock_req.granted_ == LockMode::kNone && nxt_lock_req.granted_ == LockMode::kExclusive) {
            AddEdge(lock_req.txn_id_, nxt_lock_req.txn_id_);
          } else if (lock_req.lock_mode_ == LockMode::kExclusive && lock_req.granted_ == LockMode::kNone && nxt_lock_req.granted_ != LockMode::kNone) {
            AddEdge(lock_req.txn_id_, nxt_lock_req.txn_id_);
          }
        }
      }
    }
    txn_id_t newest_tid_in_cycle = INVALID_TXN_ID;
    if (HasCycle(newest_tid_in_cycle)) {
      auto *txn = txn_mgr_->GetTransaction(newest_tid_in_cycle);
      txn_mgr_->Abort(txn);
      DeleteNode(newest_tid_in_cycle);
    }
  }
  // 休眠
  std::this_thread::sleep_for(cycle_detection_interval_);
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto [t1, neighbor] : waits_for_) {
    for (auto t2 : neighbor) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}
