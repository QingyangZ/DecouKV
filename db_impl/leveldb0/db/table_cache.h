// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"
#include "table/filter_block.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));




    // **********************************************
    // Get the index block from a table cache into iiter.
    Status IndexBlockGet(uint64_t file_number, uint64_t file_size, 
                         Iterator** iiter, FilterBlockReader** filter);
    Status IndexBlockDelete(uint64_t file_number, uint64_t file_size);
    Status GetByIndexBlock(const ReadOptions& options, uint64_t file_number,
                           uint64_t file_size, Slice& k, Slice& value,
                           void* arg,
                           void (*handle_result)(void*, const Slice&,
                                                 const Slice&));
    Status GetIterByIndexBlock(const ReadOptions& options, uint64_t file_number,
                               uint64_t file_size, Iterator** iiter,
                               Slice& value);
    // **********************************************

    // Evict any entry for the specified file number
    void Evict(uint64_t file_number);
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
    Cache* cache_;
   private:
    
    Env* const env_;
    const std::string dbname_;
    const Options& options_;
    
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
