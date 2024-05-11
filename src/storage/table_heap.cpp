#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  bool create_newpage = false;
  page_id_t page_id = GetFirstPageId();
  while (true) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->WLatch();
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    }
    if (create_newpage) {
      // 在新页中插入仍然失败，直接返回，防止一直建新页
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
      return false;
    }
    if (page->GetNextPageId() == INVALID_PAGE_ID) {
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      new_page->WLatch();
      page->SetNextPageId(new_page_id);
      new_page->Init(new_page_id, page_id, log_manager_, txn);
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page_id, true);
      create_newpage = true;
    }
    page->WUnlatch();
    if (create_newpage) {
      // 建了新页，更新了当前页的next_page_id，需要标记dirty
      buffer_pool_manager_->UnpinPage(page_id, true);
    } else {
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
    page_id = page->GetNextPageId();
  }
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  Row old_row(rid);
  // 先获取旧的tuple
  if (!GetTuple(&old_row, txn)) {
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  // 第一种情况：新tuple可以在原page中直接更新
  if (page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  
  // 第二种情况：新tuple太大，需要删除旧tuple，另外找一个页插入新tuple
  if (InsertTuple(row, txn)) {
    // 仅mark，事务提交时才apply
    MarkDelete(rid, txn);
    return true;
  }
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Txn *txn) {
  page_id_t page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  page->RLatch();
  if (!page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Txn *txn) {
  RowId first_rid;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return End();
  }
  page->RLatch();
  if (page->GetFirstTupleRid(&first_rid)) {
    Row *first_row = new Row(first_rid);
    GetTuple(first_row, txn);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return TableIterator(this, first_row, txn);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return End();
}

TableIterator TableHeap::End() {
  return TableIterator(this, nullptr, nullptr);
}
