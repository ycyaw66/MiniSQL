#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {
  table_heap_ = nullptr;
  row_ = nullptr;
  txn_ = nullptr;
}

TableIterator::TableIterator(TableHeap *table_heap, Row* row, Txn *txn)
    : table_heap_(table_heap),
      row_(row),
      txn_(txn) {}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  if (other.row_ != nullptr) {
    row_ = new Row(*other.row_);
  }
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  if (row_ != nullptr) {
    delete row_;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return itr.row_->GetRowId() == row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(itr.row_->GetRowId() == row_->GetRowId());
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  if (itr.row_ != nullptr) {
    row_ = new Row(*itr.row_);
  }
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // 寻找下一个row
  page_id_t page_id = row_->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  RowId next_rid;
  // 如果在当前页有下一个row
  if (page->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
    Row *next_row = new Row(next_rid);
    table_heap_->GetTuple(next_row, txn_);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    row_ = next_row;
    return *this;
  } 

  // 如果在当前页没有下一个row，寻找后续页的第一个row
  page_id_t next_page_id = page->GetNextPageId();
  while (next_page_id != INVALID_PAGE_ID) {
    // 找一个存在至少一个row的page
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    page->RLatch();
    RowId first_rid;
    if (page->GetFirstTupleRid(&first_rid)) {
      Row *first_row = new Row(first_rid);
      table_heap_->GetTuple(first_row, txn_);
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
      row_ = first_row;
      return *this;
    }
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
    next_page_id = page->GetNextPageId();
  }
  *this = table_heap_->End();
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++(*this);
  return temp;
}
