//
//  rocksdb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 18/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "rocksdb_db.h"

#include <iostream>
#include <vector>

#include "rocksdb/iterator.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

using std::cout;
using std::endl;
using std::cerr;

namespace ycsbc {

  RocksDB::RocksDB(const char *dbfilename) :no_found(0){
    Config_Reader config_reader = Config_Reader();
    db_config dc = config_reader.get_config("rocksdb");

    //create database if not exists
    options.create_if_missing = true;
    options.use_direct_reads = dc.enable_direct_io_;
    options.use_direct_io_for_flush_and_compaction = dc.enable_direct_io_;
    options.disable_auto_compactions= !dc.enable_compaction_;
    options.max_background_compactions = dc.thread_compaction_;
    options.max_background_jobs = dc.thread_compaction_ + 4;
    options.write_buffer_size = dc.memtable_size_;
    options.target_file_size_base = dc.sst_file_size_;
    rocksdb::BlockBasedTableOptions block_options;
    block_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(dc.bloom_bits_));
    block_options.block_cache = rocksdb::NewLRUCache(dc.block_cache_size_);

    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_options));
    options.statistics = rocksdb::CreateDBStatistics();

    rocksdb::Status s = rocksdb::DB::Open(options,dbfilename,&db_);
    if(!s.ok()){
      cerr << "init rocksdb failed!"<<endl;
      exit(0);
    }
  }


  int RocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields, std::vector<KVPair> &result){
    std::string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);
    if(!s.ok()){
      cerr << s.ToString() << endl;
      exit(0);
    }else{
      if(s.IsNotFound()){
        this->no_found++;
        return DB::kErrorNoData;
      }
    }
    return DB::kOK;
  }

  int RocksDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields, std::vector<std::vector<KVPair> > &result){
    rocksdb::Iterator* iter = db_->NewIterator(rocksdb::ReadOptions());
    iter->Seek(key);
    for( int i = 0; i < len; i++ ){
      if(!iter->Valid() || iter->value().empty() ){
        no_found++;
      }
      iter->Next();
    }
    delete iter;
    return DB::kOK;
  }

  int RocksDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    rocksdb::Status s;
    for( KVPair &p :values ){
      s = db_->Put(rocksdb::WriteOptions(),key,p.second);
    }
    return DB::kOK;
  }

  int RocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    return Insert(table,key,values);
  }

  int RocksDB::Delete(const std::string &table, const std::string &key){
    std::vector<KVPair> values;
    return Insert(table,key,values);
  }

  void RocksDB::printStats(){
    std::string stats;
    db_->GetProperty("rocksdb.stats",&stats);
    cerr << stats << endl;
    cerr << options.statistics->ToString() << endl;
  }

  RocksDB::~RocksDB(){
    printStats();
    delete db_;
  }
} //ycsbc

