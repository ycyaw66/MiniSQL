#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf, ROW_MAGIC_NUM);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  size_t fields_num = fields_.size();
  MACH_WRITE_TO(size_t, buf, fields_num);
  offset += sizeof(size_t);
  buf += sizeof(size_t);

  uint32_t null_bitmap = 0;
  for (size_t i = 0; i < fields_num; i++) {
    Field *field = fields_[i];
    if (field->IsNull()) {
      null_bitmap |= (1 << i);
    }
  }
  MACH_WRITE_TO(uint32_t, buf, null_bitmap);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  for (size_t i = 0; i < fields_num; i++) {
    Field *field = fields_[i];
    uint32_t ofs = field->SerializeTo(buf);
    offset += ofs;
    buf += ofs;
  }
  
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(magic_num == ROW_MAGIC_NUM, "Invalid row magic number.");
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  size_t fields_num = MACH_READ_FROM(size_t, buf);
  fields_.resize(fields_num);
  offset += sizeof(size_t);
  buf += sizeof(size_t);

  uint32_t null_bitmap = MACH_READ_FROM(uint32_t, buf);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  std::vector<Column *> columns = schema->GetColumns();
  for (size_t i = 0; i < fields_num; i++) {
    uint32_t ofs = Field::DeserializeFrom(buf, columns[i]->GetType(), &fields_[i], (null_bitmap & (1 << i)) != 0);
    offset += ofs;
    buf += ofs;
  }

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t size = sizeof(uint32_t) + sizeof(size_t) + sizeof(uint32_t);
  for (size_t i = 0; i < fields_.size(); i++) {
    size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
