#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
 public:
  explicit TableIterator();
  
  explicit TableIterator(TableHeap *table_heap, Row* row, Txn *txn);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  Row *row_;
  TableHeap *table_heap_;
  Txn *txn_;
};

#endif  // MINISQL_TABLE_ITERATOR_H
