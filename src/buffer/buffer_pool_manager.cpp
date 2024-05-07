#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).

  // 1.1    If P exists, pin it and return it immediately.
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  // 2.     If R is dirty, write it back to the disk.
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    FlushPage(page->GetPageId());
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->page_id_ = page_id;
  replacer_->Unpin(frame_id);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page->GetData());
  return page;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    FlushPage(page->GetPageId());
  }
  page_id_t new_page_id = AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_[new_page_id] = frame_id;
  page->page_id_ = new_page_id;
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = new_page_id;
  return page;
}


bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P). If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->ResetMemory();
  free_list_.emplace_back(frame_id);
  return false;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 1.   Search the page table for the requested page (P).
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }
  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
      page->is_dirty_ = false;
    }
    return true;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

frame_id_t BufferPoolManager::TryToFindFreePage() {
  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }
  if (!replacer_->Victim(&frame_id)) {
    return INVALID_FRAME_ID;
  }
  return frame_id;
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}