#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  lru_list_size_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {
    frame_id = nullptr;
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  auto lru_iter = lru_list_iter_.find(*frame_id);
  if (lru_iter != lru_list_iter_.end()) {
    lru_list_iter_.erase(lru_iter);
  }
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto lru_iter = lru_list_iter_.find(frame_id);
  if (lru_iter != lru_list_iter_.end()) {
    lru_list_.erase(lru_iter->second);
    lru_list_iter_.erase(lru_iter);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list_.size() >= lru_list_size_ || lru_list_iter_.find(frame_id) != lru_list_iter_.end()) {
    return;
  }
  lru_list_.push_front(frame_id);
  lru_list_iter_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() {
  return lru_list_.size();
}