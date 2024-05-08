#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  MACH_WRITE_TO(size_t, buf, name_.size());
  offset += sizeof(size_t);
  buf += sizeof(size_t);
  memcpy(buf, name_.c_str(), name_.size());
  offset += name_.size();
  buf += name_.size();

  MACH_WRITE_TO(TypeId, buf, type_);
  offset += sizeof(TypeId);
  buf += sizeof(TypeId);

  MACH_WRITE_TO(uint32_t, buf, len_);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  MACH_WRITE_TO(uint32_t, buf, table_ind_);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  
  MACH_WRITE_TO(bool, buf, nullable_);
  offset += sizeof(bool);
  buf += sizeof(bool);

  MACH_WRITE_TO(bool, buf, unique_);
  offset += sizeof(bool);
  buf += sizeof(bool);

  return offset;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Invalid column magic number.");
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  size_t name_size = MACH_READ_FROM(size_t, buf);
  offset += sizeof(size_t);
  buf += sizeof(size_t);

  std::string name(buf, name_size);
  offset += name_size;
  buf += name_size;

  TypeId type = MACH_READ_FROM(TypeId, buf);
  offset += sizeof(TypeId);
  buf += sizeof(TypeId);

  uint32_t len = MACH_READ_FROM(uint32_t, buf);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  uint32_t table_ind = MACH_READ_FROM(uint32_t, buf);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  bool nullable = MACH_READ_FROM(bool, buf);
  offset += sizeof(bool);
  buf += sizeof(bool);

  bool unique = MACH_READ_FROM(bool, buf);
  offset += sizeof(bool);
  buf += sizeof(bool);

  if (type == TypeId::kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }

  return offset;
}

uint32_t Column::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t) * 3 + sizeof(size_t) + name_.size() + sizeof(TypeId) + sizeof(bool) * 2;
  return size;
}