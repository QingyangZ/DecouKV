//
//  leveldb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 05/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "leveldb_db.h"

#include <iostream>
#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "leveldb/status.h"

using std::cout;
using std::endl;
using std::cerr;
namespace ycsbc {

  LevelDB::LevelDB(const char *dbfilename) :no_found(0){
    Config_Reader config_reader = Config_Reader();
    db_config dc = config_reader.get_config("leveldb");

    //create database if not exists
    options.create_if_missing = true;
    options.enable_direct_io = dc.enable_direct_io_;
    options.enable_compaction = dc.enable_compaction_;
    options.thread_compaction = dc.thread_compaction_;
    options.filter_policy = leveldb::NewBloomFilterPolicy(dc.bloom_bits_);
    options.block_cache = leveldb::NewLRUCache(dc.block_cache_size_);
    options.write_buffer_size = dc.memtable_size_;
    options.max_file_size = dc.sst_file_size_;
    leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
    if(!s.ok()){
      cerr << "init leveldb failed!"<<endl;
      exit(0);
    }
  }


  int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields, std::vector<KVPair> &result){
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
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

  int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields, std::vector<std::vector<KVPair> > &result){
    leveldb::Iterator* iter = db_->NewIterator(leveldb::ReadOptions());
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

  int LevelDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    leveldb::Status s;
    for( KVPair &p :values ){
      s = db_->Put(leveldb::WriteOptions(),key,p.second);
    }
    return DB::kOK;
  }

  int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    return Insert(table,key,values);
  }

  int LevelDB::Delete(const std::string &table, const std::string &key){
    std::vector<KVPair> values;
    return Insert(table,key,values);
  }

  void LevelDB::printStats(){
    std::string stats;
    db_->GetProperty("leveldb.stats",&stats);
    cout << stats << endl;
    cout << "Missing operations count : " << no_found << endl;
  }

  LevelDB::~LevelDB(){
    cout << "print leveldb statistics: " << endl;
    printStats();
    delete db_;
  }

} //ycsbc
