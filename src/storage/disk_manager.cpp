#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
  meta_page_ = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  for (uint32_t i = 0; i < meta_page_->GetExtentNums(); i++) {
    if (meta_page_->GetExtentUsedPage(i) < BITMAP_SIZE) {
      next_free_extent_ = i;
      break;
    }
  }
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  char buffer[PAGE_SIZE] = {0};
  ReadPhysicalPage(next_free_extent_ * (BITMAP_SIZE + 1) + 1, buffer);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
  uint32_t page_offset;
  if (bitmap_page->AllocatePage(page_offset)) {
    meta_page_->num_allocated_pages_++;
    if (meta_page_->GetExtentNums() <= next_free_extent_) {
      meta_page_->num_extents_++;
    }
    meta_page_->extent_used_page_[next_free_extent_]++;
    WritePhysicalPage(next_free_extent_ * (BITMAP_SIZE + 1) + 1, buffer);
    page_id_t page_id = next_free_extent_ * BITMAP_SIZE + page_offset;
    while (meta_page_->GetExtentUsedPage(next_free_extent_) == BITMAP_SIZE && next_free_extent_ < MAX_VALID_EXTENT_ID) {
      next_free_extent_++;
    }
    ASSERT(next_free_extent_ < MAX_VALID_EXTENT_ID, "No free extent");
    return page_id;
  }
  return INVALID_PAGE_ID;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  if (extent_id >= MAX_VALID_EXTENT_ID) {
    ASSERT(false, "Invalid extent id");
    return;
  }
  char buffer[PAGE_SIZE] = {0};
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, buffer);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
  if (bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE)) {
    meta_page_->num_allocated_pages_--;
    meta_page_->extent_used_page_[extent_id]--;
    WritePhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, buffer);
    if (extent_id < next_free_extent_) {
      next_free_extent_ = extent_id;
    }
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  if (extent_id >= MAX_VALID_EXTENT_ID) {
    LOG(ERROR) << "Invalid extent id";
    return false;
  }
  if (extent_id >= meta_page_->GetExtentNums()) {
    return true;
  }
  char buffer[PAGE_SIZE] = {0};
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, buffer);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
  return bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) { 
  return logical_page_id / BITMAP_SIZE + logical_page_id + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}