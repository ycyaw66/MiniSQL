#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  clock_list_size_ = 0;
  clock_list_.resize(num_pages, make_pair(false, false));
  clock_hand_ = 0;
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (!clock_list_size_) {
    frame_id = nullptr;
    return false;
  }
  while (true) {
    if (clock_list_[clock_hand_].first) {
      if (clock_list_[clock_hand_].second) {
        clock_list_[clock_hand_].second = false;
      } else {
        *frame_id = clock_hand_;
        clock_list_[clock_hand_].first = false;
        clock_hand_ = (clock_hand_ + 1) % clock_list_.size();
        clock_list_size_--;
        return true;
      }
    }
    clock_hand_ = (clock_hand_ + 1) % clock_list_.size();
  }
  return false;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (clock_list_[frame_id].first) {
    clock_list_[frame_id].first = false;
    clock_list_size_--;
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (!clock_list_[frame_id].first) {
    clock_list_[frame_id].first = true;
    clock_list_size_++;
  }
  clock_list_[frame_id].second = true;
}

size_t CLOCKReplacer::Size() {
  return clock_list_size_;
}