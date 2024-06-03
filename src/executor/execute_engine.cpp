#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  **/
  struct dirent *stdir;
  while ((stdir = readdir(dir)) != nullptr) {
    if (strcmp(stdir->d_name, ".") == 0 || strcmp(stdir->d_name, "..") == 0 || stdir->d_name[0] == '.') {
      continue;
    }
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * 要做的错误检查：column是否重复，primary key是否在column中，primary key是否重复
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  TableInfo *table_info = nullptr;
  // 根据语法树获取name和schema
  string table_name(ast->child_->val_);
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
    cout << "Table " + table_name + " already exists." << endl;
    return DB_FAILED;
  }
  auto column_definition_list_node = ast->child_->next_;
  std::vector<Column *> columns;
  uint32_t index = 0;
  // 存储primary key
  std::vector<std::string> primary_keys;
  for (auto column_definition_node = column_definition_list_node->child_;column_definition_node != nullptr; column_definition_node = column_definition_node->next_) {
    // 如果是column_list，说明是primary key（不一定存在）
    if (column_definition_node->type_ == kNodeColumnList) {
      for (auto identifier_node = column_definition_node->child_; identifier_node != nullptr; identifier_node = identifier_node->next_) {
        primary_keys.push_back(identifier_node->val_);
      }
      break;
    }
    // 如果是column_definition，说明是column
    string column_name(column_definition_node->child_->val_);
    string column_type(column_definition_node->child_->next_->val_);
    // 空值直接转会报错
    bool unique = false;
    if (column_definition_node->val_ != nullptr && string(column_definition_node->val_) == "unique") {
      unique = true;
    }
    if (column_type == "int") {
      columns.push_back(new Column(column_name, kTypeInt, index++, true, unique));
    } else if (column_type == "float") {
      columns.push_back(new Column(column_name, kTypeFloat, index++, true, unique));
    } else if (column_type == "char") {
      string length(column_definition_node->child_->next_->child_->val_);
      // 检查length是否是非负整数
      if (length.find_first_not_of("0123456789") != string::npos) {
        cout << "You have an error in your SQL syntax" << endl;
        return DB_FAILED;
      }
      uint32_t len = stoi(length);
      columns.push_back(new Column(column_name, kTypeChar, len, index++, true, unique));
    } else {
      cout << "You have an error in your SQL syntax" << endl;
      return DB_FAILED;
    }
  }
  // 检查有没有重复的column
  for (int i = 0; i < columns.size(); i++) {
    for (int j = i + 1; j < columns.size(); j++) {
      if (columns[i]->GetName() == columns[j]->GetName()) {
        cout << "Duplicate column name '" + columns[i]->GetName() + "'"<< endl;
        return DB_FAILED;
      }
    }
  }
  // 检查有没有重复的primary key
  for (int i = 0; i < primary_keys.size(); i++) {
    for (int j = i + 1; j < primary_keys.size(); j++) {
      if (primary_keys[i] == primary_keys[j]) {
        cout << "Duplicate column name '" + primary_keys[i] + "'"<< endl;
        return DB_FAILED;
      }
    }
  }
  // 检查是否所有的primary key都在column中
  for (auto key : primary_keys) {
    bool found = false;
    for (int i = 0; i < columns.size(); i++) {
      if (columns[i]->GetName() == key) {
        found = true;
        // 因为不支持not null语法，所以令主键为非空(nullable=false)，不是主键则为可空
        // 更新column的null属性，因为没有SetUnique函数，所以只能重新new一个
        if (columns[i]->GetType() == kTypeChar) {
          columns[i] = new Column(columns[i]->GetName(), columns[i]->GetType(), columns[i]->GetLength(), columns[i]->GetTableInd(), false, columns[i]->IsUnique());
        } else {
          columns[i] = new Column(columns[i]->GetName(), columns[i]->GetType(), columns[i]->GetTableInd(), false, columns[i]->IsUnique());
        }
        break;
      }
    }
    if (!found) {
      cout << " Key column '" + key + "' doesn't exist in table" << endl;
      return DB_FAILED;
    }
  }
  Schema *schema = new Schema(columns);
  // 建表
  dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  cout << "Table " + table_name + " is created successfully" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  if (dbs_[current_db_]->catalog_mgr_->DropTable(table_name) == DB_TABLE_NOT_EXIST) {
    cout << "Table " + table_name + " doesn't exist" << endl;
    return DB_FAILED;
  }
  cout << "Table " + table_name + " is dropped successfully" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  bool found = false;
  for (const auto &itr : tables) {
    vector<IndexInfo *> indexes;
    if (dbs_[current_db_]->catalog_mgr_->GetTableIndexes(itr->GetTableName(), indexes) != DB_INDEX_NOT_FOUND) {
      found = true;
      break;
    }
  }
  if (!found) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  uint max_table_name_width = 10; // "Table_name"
  uint max_index_name_width = 10; // "Index_name"
  for (const auto &itr : tables) {
      if (itr->GetTableName().length() > max_table_name_width) max_table_name_width = itr->GetTableName().length();
      vector<IndexInfo *> indexes;
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(itr->GetTableName(), indexes);
      for (const auto &index : indexes) {
          if (index->GetIndexName().length() > max_index_name_width) max_index_name_width = index->GetIndexName().length();
      }
  }
  cout << "+" << setfill('-') << setw(max_table_name_width + 2) << "";
  cout << "+" << setfill('-') << setw(max_index_name_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_table_name_width) << "Table_name" << " | " << std::left << setfill(' ') << setw(max_index_name_width) << "Index_name" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_table_name_width + 2) << "";
  cout << "+" << setfill('-') << setw(max_index_name_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
      vector<IndexInfo *> indexes;
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(itr->GetTableName(), indexes);
      for (const auto &index : indexes) {
          cout << "| " << std::left << setfill(' ') << setw(max_table_name_width) << itr->GetTableName() << " | " << std::left << setfill(' ') << setw(max_index_name_width) << index->GetIndexName() << " |" << endl;
      }
  }
  cout << "+" << setfill('-') << setw(max_table_name_width + 2) << "";
  cout << "+" << setfill('-') << setw(max_index_name_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * 由于drop的语法是删除对应名字的index（没有写对应的表），因此同一个数据库插入时不允许重名的index
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  auto column_list_node = ast->child_->next_->next_;
  std::vector<std::string> index_keys;
  for (auto identifier_node = column_list_node->child_; identifier_node != nullptr; identifier_node = identifier_node->next_) {
    index_keys.push_back(identifier_node->val_);
  }
  IndexInfo *index_info = nullptr;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, "bptree");
  switch (result) {
    case DB_SUCCESS:
      cout << "Index " + index_name + " is created successfully" << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table " + table_name + " doesn't exist" << endl;
      return DB_FAILED;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index " + index_name + " already exists" << endl;
      return DB_FAILED;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists" << endl;
      return DB_FAILED;
    case DB_FAILED:
      cout << "Failed to create index" << endl;
      return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  // 找到index所在table
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Index " + index_name + " doesn't exist" << endl;
    return DB_FAILED;
  }
  bool found = false;
  string table_name;
  for (const auto &itr : tables) {
    vector<IndexInfo *> indexes;
    if (dbs_[current_db_]->catalog_mgr_->GetTableIndexes(itr->GetTableName(), indexes) != DB_INDEX_NOT_FOUND) {
      for (const auto &index : indexes) {
        if (index->GetIndexName() == index_name) {
          table_name = itr->GetTableName();
          found = true;
          break;
        }
      }
    }
    if (found) {
      break;
    }
  }
  if (!found) {
    cout << "Index " + index_name + " doesn't exist" << endl;
    return DB_FAILED;
  }
  dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name);
  cout << "Index " + index_name + " is dropped successfully" << endl;
  return DB_SUCCESS;
}

/**
 * 事务相关，暂不实现
 */
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * 事务相关，暂不实现
 */
dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * 事务相关，暂不实现
 */
dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name(ast->child_->val_);
  std::ifstream file(file_name);
  if (!file.is_open()) {
    cout << "Failed to open file: " << file_name << endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  char ch;
  std::string sql_statement;
  while (file.get(ch)) {
    if (ch == ';') {
      sql_statement += ch;
      // Parse and execute SQL statement
      YY_BUFFER_STATE bp = yy_scan_string(sql_statement.c_str());
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state for SQL statement: " << sql_statement << std::endl;
        continue;
      }
      yy_switch_to_buffer(bp);
      
      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();
      
      // parse result handle
      auto result = Execute(MinisqlGetParserRootNode());
      
      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();
      
      // quit condition
      ExecuteInformation(result);
      if (result == DB_QUIT) {
        break;
      }
      sql_statement.clear();
    } else {
      sql_statement += ch;
    }
  }
  file.close();  
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  cout << "Query OK (" << fixed << setprecision(4) << duration_time / 1000 << " sec)." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
