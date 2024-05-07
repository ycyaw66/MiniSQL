#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <algorithm>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * CLOCKReplacer implements the clock replacement.
 */
class CLOCKReplacer : public Replacer {
 public:
  /**
   * Create a new CLOCKReplacer.
   * @param num_pages the maximum number of pages the CLOCKReplacer will be required to store
   */
  explicit CLOCKReplacer(size_t num_pages);

  /**
   * Destroys the CLOCKReplacer.
   */
  ~CLOCKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  size_t clock_list_size_;
  size_t clock_hand_;
  vector<pair<bool, bool>> clock_list_;
};

#endif  // MINISQL_CLOCK_REPLACER_H