#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 * 3 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * 把catalog meta加载到内存里
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager, LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    // create catalog meta page
    catalog_meta_ = CatalogMeta::NewInstance();
    auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  } else {
    // load catalog meta page
    auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    for (auto iter : catalog_meta_->table_meta_pages_) {
      page_id_t page_id = iter.second;
      auto table_page = buffer_pool_manager_->FetchPage(page_id);
      char *table_data = table_page->GetData();
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_data, table_meta);
      TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager, lock_manager);
      TableInfo* table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
      tables_[table_meta->GetTableId()] = table_info;
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
    for (auto iter : catalog_meta_->index_meta_pages_) {
      page_id_t page_id = iter.second;
      auto index_page = buffer_pool_manager_->FetchPage(page_id);
      char *index_data = index_page->GetData();
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_data, index_meta);
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      // map不存在的话应该会新建一个？
      index_names_[tables_[index_meta->GetTableId()]->GetTableName()].insert({index_meta->GetIndexName(), index_meta->GetIndexId()});
      indexes_[index_meta->GetIndexId()] = index_info;
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t table_id = catalog_meta_->GetNextTableId();
  table_names_[table_name] = table_id;
  // 新建table_meta和table_heap
  page_id_t table_meta_page_id;
  Schema *tmp_schema = schema->DeepCopySchema(schema);
  auto table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  // TableMetadata类中，root_page_id_指的是TableHeap的root_page_id_，所以先构造TableHeap再构造TableMetadata
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, tmp_schema, txn, log_manager_, lock_manager_);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), tmp_schema);
  // 构造table_info
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;
  // 序列化table_meta
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);
  // 更新catalog meta
  catalog_meta_->table_meta_pages_[table_id] = table_meta_page_id;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_names_[table_name]];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.empty()) {
    return DB_TABLE_NOT_EXIST;
  }
  for (auto iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name, const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name, IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  // 删除table对应的index
  for (auto iter : index_names_[table_name]) {
    DropIndex(table_name, iter.first);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * 把catalog meta page写入磁盘
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (page == nullptr) {
    return DB_FAILED;
  }
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}