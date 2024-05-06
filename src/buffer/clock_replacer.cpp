#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  clock_list_size_ = num_pages;
  clock_list_iter_.resize(num_pages, clock_list_.end());
  clock_list_ref_.resize(num_pages, false);
  clock_hand_ = clock_list_.end();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (clock_list_.empty()) {
    frame_id = nullptr;
    return false;
  }
  while (true) {
    if (clock_hand_ == clock_list_.end()) {
      clock_hand_ = clock_list_.begin();
    }
    if (!clock_list_ref_[*clock_hand_]) {
      *frame_id = *clock_hand_;
      list<frame_id_t>::iterator temp_clock_hand_ = clock_hand_;
      temp_clock_hand_++;
      clock_list_.erase(clock_hand_);
      clock_list_iter_[*frame_id] = clock_list_.end();
      clock_hand_ = temp_clock_hand_;
      break;
    }
    clock_list_ref_[*clock_hand_] = false;
    clock_hand_++;
  }
  return true;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (clock_list_iter_[frame_id] != clock_list_.end()) {
    clock_list_.erase(clock_list_iter_[frame_id]);
    clock_list_iter_[frame_id] = clock_list_.end();
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (clock_list_.size() >= clock_list_size_) {
    return;
  }
  if (clock_list_iter_[frame_id] != clock_list_.end()) {
    return;
  }
}

size_t CLOCKReplacer::Size() {
  return clock_list_.size();
}