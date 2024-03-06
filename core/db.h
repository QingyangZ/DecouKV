//
//  db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_H_
#define YCSB_C_DB_H_

#include <vector>
#include <string>

namespace ycsbc {

class DB {
 public:
  typedef std::pair<std::string, std::string> KVPair;
  static const int kOK = 0;
  static const int kErrorNoData = 1;
  static const int kErrorConflict = 2;
  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Init() { }
  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Close() { }
  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of field/value pairs for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  virtual int Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) = 0;
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error.
  ///
  virtual int Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) = 0;
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified vector are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A vector of field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified vector are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A vector of field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Delete(const std::string &table, const std::string &key) = 0;

  virtual ~DB() { }
};
inline void ShiftZeroToChar(char *p, uint32_t len){
  for( uint32_t i = 0; i < len; i++ ){
    if( *(uint8_t*)(p+i) == 0){
       *(uint8_t*)(p+i) = uint8_t(255);
    }
  }
}
inline void ShiftCharToZero(char *p, uint32_t len){
  for( uint32_t i = 0; i < len; i++ ){
    if( *(uint8_t*)(p+i) == 255){
       *(uint8_t*)(p+i) = uint8_t(0);
    }
  }
}
// convert value_fields -> data
inline void SerializeRow(const std::vector<DB::KVPair>& value_fields, std::string& data){
  // get the field name and value size
  uint32_t field_name_len = value_fields[0].first.size();
  uint32_t field_value_len = value_fields[0].second.size();
  uint32_t data_size = value_fields.size() * (2 * sizeof(uint32_t) + field_name_len + field_value_len);
  data.reserve(data_size);
  for (const DB::KVPair& p : value_fields) {
    uint32_t name_len = p.first.size();
    // ShiftZeroToChar(reinterpret_cast<char*>(&name_len),sizeof(uint32_t));
    data.append(reinterpret_cast<char*>(&name_len), sizeof(uint32_t));
    data.append(p.first.data(), p.first.size());
    uint32_t value_len = p.second.size();
    // ShiftZeroToChar(reinterpret_cast<char*>(&value_len),sizeof(uint32_t));
    data.append(reinterpret_cast<char*>(&value_len), sizeof(uint32_t));
    data.append(p.second.data(), p.second.size());
  }
}

// convert data -> value_fields
inline void DeserializeRow(std::vector<DB::KVPair>& value_fields, const std::string& data){
  const char* p = data.data();
  const char* end = p + data.size();
  while (p != end) {
    if (p >= end - 4)
      break;
    uint32_t len = *reinterpret_cast<const uint32_t*>(p);
    //ShiftCharToZero(reinterpret_cast<char*>(&len),sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::string field(p, uint32_t(len));
    p += len;
    len = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    //ShiftCharToZero(reinterpret_cast<char*>(&len),sizeof(uint32_t));
    std::string value(p, uint32_t(len));
    p += len;
    value_fields.push_back(std::make_pair<std::string, std::string>(std::move(field), std::move(value)));
  }
}
// convert data -> value_fields by filter
inline void DeserializeRowFilter(std::vector<DB::KVPair>& value_fields, const std::string& data,
    const std::vector<std::string>& fields){
  const char* p = data.data();
  const char* end = p + data.size();
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != end && filter_iter != fields.end()) {
    if (p >= end - 4)
      break;
    uint32_t len = *reinterpret_cast<const uint32_t*>(p);
    //ShiftCharToZero(reinterpret_cast<char*>(&len),sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::string field(p, uint32_t(len));
    p += len;
    len = *reinterpret_cast<const uint32_t*>(p);
    //ShiftCharToZero(reinterpret_cast<char*>(&len),sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::string value(p, uint32_t(len));
    p += len;
    if (*filter_iter == field){
      value_fields.push_back(std::make_pair<std::string, std::string>(std::move(field), std::move(value)));
      filter_iter++;
    }
  }
}
} // ycsbc

#endif // YCSB_C_DB_H_
