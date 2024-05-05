#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == GetMaxSupportedSize()) {
    // LOG(INFO) << "No more pages can be allocated in this BitmapPage.";
    return false;
  }
  page_allocated_++;
  while (!IsPageFree(next_free_page_)) {
    next_free_page_++;
  }
  page_offset = next_free_page_;
  bytes[page_offset / 8] |= (1 << (page_offset % 8));
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    // LOG(INFO) << "page_offset is out of range: " << page_offset;
    return false;
  }
  if (IsPageFree(page_offset)) {
    // LOG(INFO) << "page_offset is already free: " << page_offset;
    return false;
  }
  page_allocated_--;
  bytes[page_offset / 8] ^= (1 << (page_offset % 8));
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    // LOG(INFO) << "page_offset is out of range: " << page_offset;
    return false;
  }
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;