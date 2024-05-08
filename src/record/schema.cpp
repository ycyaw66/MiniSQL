#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  MACH_WRITE_TO(size_t, buf, columns_.size());
  offset += sizeof(size_t);
  buf += sizeof(size_t);

  for (size_t i = 0; i < columns_.size(); i++) {
    Column *column = columns_[i];
    uint32_t ofs = column->SerializeTo(buf);
    offset += ofs;
    buf += ofs;
  }

  MACH_WRITE_TO(bool, buf, is_manage_);
  offset += sizeof(bool);
  buf += sizeof(bool);

  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  size_t columns_num = MACH_READ_FROM(size_t, buf);
  offset += sizeof(size_t);
  buf += sizeof(size_t);

  std::vector<Column *> columns;
  columns.resize(columns_num);
  for (size_t i = 0; i < columns_num; i++) {
    uint32_t ofs = Column::DeserializeFrom(buf, columns[i]);
    offset += ofs;
    buf += ofs;
  }

  bool is_manage = MACH_READ_FROM(bool, buf);
  offset += sizeof(bool);
  buf += sizeof(bool);

  schema = new Schema(columns, is_manage);
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t) + sizeof(size_t) + sizeof(bool);
  for (size_t i = 0; i < columns_.size(); i++) {
    size += columns_[i]->GetSerializedSize();
  }
  return size;
}