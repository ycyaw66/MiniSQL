#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void BPlusTreeLeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {}

/**
 * Helper methods to set/get next page id
 */
page_id_t BPlusTreeLeafPage::GetNextPageId() const {
  return next_page_id_;
}

void BPlusTreeLeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int BPlusTreeLeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  return 0;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *BPlusTreeLeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void BPlusTreeLeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId BPlusTreeLeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void BPlusTreeLeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *BPlusTreeLeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void BPlusTreeLeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> BPlusTreeLeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int BPlusTreeLeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void BPlusTreeLeafPage::MoveHalfTo(BPlusTreeLeafPage *recipient) {
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void BPlusTreeLeafPage::CopyNFrom(void *src, int size) {
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool BPlusTreeLeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int BPlusTreeLeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void BPlusTreeLeafPage::MoveAllTo(BPlusTreeLeafPage *recipient) {
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void BPlusTreeLeafPage::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void BPlusTreeLeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void BPlusTreeLeafPage::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void BPlusTreeLeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
}