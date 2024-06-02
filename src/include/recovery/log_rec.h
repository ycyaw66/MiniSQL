#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

struct LogRec {
  LogRec() = default;

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  lsn_t prev_lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};
  KeyType ins_key_{};
  ValType ins_val_{};
  KeyType del_key_{};
  ValType del_val_{};
  KeyType old_key_{};
  ValType old_val_{};
  KeyType new_key_{};
  ValType new_val_{};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

// 每个事务有多个操作，每个操作对应一个日志记录
static lsn_t GetAndUpdatePrevLsn(txn_id_t txn_id, lsn_t lsn) {
  auto it = LogRec::prev_lsn_map_.find(txn_id);
  if (it == LogRec::prev_lsn_map_.end()) {
    LogRec::prev_lsn_map_[txn_id] = lsn;
    return INVALID_LSN;
  }
  lsn_t prev_lsn = it->second;
  it->second = lsn;
  return prev_lsn;
}

static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kInsert;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  log_rec->ins_key_ = ins_key;
  log_rec->ins_val_ = ins_val;
  return log_rec;
}

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kDelete;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  log_rec->del_key_ = del_key;
  log_rec->del_val_ = del_val;
  return log_rec;
}

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kUpdate;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  log_rec->old_key_ = old_key;
  log_rec->old_val_ = old_val;
  log_rec->new_key_ = new_key;
  log_rec->new_val_ = new_val;
  return log_rec;
}

static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kBegin;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  return log_rec;
}

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kCommit;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  return log_rec;
}

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kAbort;
  log_rec->lsn_ = lsn;
  log_rec->prev_lsn_ = GetAndUpdatePrevLsn(txn_id, lsn);
  log_rec->txn_id_ = txn_id;
  return log_rec;
}

#endif  // MINISQL_LOG_REC_H
