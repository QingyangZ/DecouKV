#pragma once
#include <map>
#include <set>
#include <vector>
#include <iostream>

#include "db/dbformat.h"
#include "db/double_skiplist.h"
#include "db/internal_stats.h"
#include "db/version_set.h"
#include "db/column_family.h"
#include "monitoring/file_read_sample.h"
// #include "port/port.h"
// #include "util/mutexlock.h"
// #include "port/port.h"
// #include "monitoring/file_read_sample.h"

namespace ROCKSDB_NAMESPACE {

class Mutex;

class GlobalIndex {
  public:
    struct SkipListItem;
    struct KeyComparator;
    typedef DoubleSkipList<SkipListItem, KeyComparator> GITable;
    
    ~GlobalIndex();
    struct SkipListItem {
      // the maximum key in the data_block
      Slice key;
      // the index(offset) to a data_block
      IndexValue value;
      // the file number of the file that the index block is in
      // const FileMetaData *f;
      uint64_t file_number;
      // the size of the file that the index block is in
      // uint64_t file_size;
      // the node at next level of skiplist
      void* next_level_node = nullptr;
      // The filter that manages this data block
      SkipListItem(Slice key_) {
        this->key = key_;
      };
      SkipListItem(int num){};
      void SetNextNode(void* next_level_node_) {
        this->next_level_node = next_level_node_;
      };
      SkipListItem(Slice key_, IndexValue value_, uint64_t file_number_) {
        this->key = key_;
        this->value = value_;
        this->file_number = file_number;
      }
      // Check whether the internal key may be in the data block
      // @param use_file_gran_filter_: this determines how we parse the filter block
      // bool KeyMaybeInDataBlock(Slice internal_key, bool use_file_gran_filter_);
    };
    // TODO:
    // Comparator
    const Comparator *user_comparator_;
    struct KeyComparator {
      const Comparator *user_comparator_;
      explicit KeyComparator(const Comparator* c) { user_comparator_ = c; };
      int operator()(SkipListItem a, SkipListItem b) const;
    };
    
    Arena arena_;

    class EditFile {
     public:
      explicit EditFile(int level, InternalKey smallest, InternalKey largest)
          : level_(level), smallest_(smallest), largest_(largest){};
      ~EditFile(){};
      int level_;
      InternalKey smallest_;
      InternalKey largest_;
    };
    using DeletedFiles = std::map<uint64_t, EditFile>;
    DeletedFiles delete_files_;
    using NewFiles = std::map<uint64_t, EditFile>;
    NewFiles add_files_;

    void EditFiles(ReadOptions read_options);

    bool EditEmpty() {
      return delete_files_.size() == 0 && add_files_.size() == 0;
    }

    // If bloom filter is used, Whether to use filter blocks with file (sstable)
    // granularity If false, then use filter blocks with block granularity bool
    // use_file_gran_filter_ = true;

    // Search an internal key in a skip list of global index table, 
    // and the result is saved in arg_saver
    // @param internal_key: the internal key to be queried
    // @param gitable_: the skip list of global index table
    // @param next_level_: the next-level-node of the found node,
    //      and it will be updated after this method (but I don't know why)
    // @param arg_saver: the saver to save operation status,
    //      and it will be updated after this method
    // @param handle_result: the method to handle found result
    void SearchGITable(const ReadOptions& options, Slice internal_key,
                       GetContext* get_context, int level,
                       SkipListItem* item_ptr, Status* status,
                       GITable* gitable_, void** next_level_);
    // void CheckLevel0(const ReadOptions& options,
    //                  std::vector<FileMetaData*> files_[config::kNumLevels]);
    void GlobalIndexBuilder(const ReadOptions& options,
                            VersionStorageInfo* vstorage);
    void GetFromGlobalIndex(const ReadOptions& read_options, const LookupKey& k,
                            PinnableSlice* value, std::string* timestamp,
                            Status* status, MergeContext* merge_context,
                            SequenceNumber* max_covering_tombstone_seq,
                            bool* value_found, bool* key_exists,
                            SequenceNumber* seq, ReadCallback* callback,
                            bool* is_blob, bool do_merge);
    SkipListItem* InitItem(Slice key, IndexValue value, uint64_t file_number);
    void SkipListGlobalIndexBuilder(
        const ReadOptions& options,
        const InternalKeyComparator& internal_comparator,
        InternalIteratorBase<IndexValue>* iiter, const FileMetaData* f,
        GITable** gitable_, GITable::Node** next_level_node,
        GITable::Node* bound_node, bool is_first, bool is_last, int level,
        GITable::Iterator** next_level_ptr);
    bool global_index_exists_ = false;
    void AddFile(int level, const ReadOptions& options,
                 const FileMetaData* file_meta, InternalKey smallest,
                 InternalKey largest);
    void DeleteFile(int level, uint64_t file_number, const ReadOptions& options,
                    InternalKey smallest, InternalKey largest);

    std::vector<GITable*> index_files_level0;
    std::vector<GITable*> index_files_;
    void SeekPtr(Slice key, GITable::Iterator** next_level_ptr,
                GITable::Node** suit_node);
    void AddPtr(Slice key, GITable::Iterator** next_level_ptr,
                GITable::Node** suit_node);
    
    ColumnFamilyData* cfd_;
    size_t max_file_size_for_l0_meta_pin_;
  private:
    port::RWMutex rw_mutex_;
};

// class GlobalIndexIterator : public Iterator {
//  public:
//   explicit GlobalIndexIterator(GlobalIndex::GITable* table) : iter_(table) {}

//   GlobalIndexIterator(const GlobalIndexIterator&) = delete;
//   GlobalIndexIterator& operator=(const GlobalIndexIterator&) = delete;

//   ~GlobalIndexIterator() override = default;

//   bool Valid() const override { return iter_.Valid(); }

//   void Seek(const Slice& k) override { iter_.Seek(k); }
//   void SeekWithNode(const Slice& k, void** start_node_) override {
//     iter_.SeekWithNode(k, start_node_);
//   }
//   void SeekToFirst() override { iter_.SeekToFirst(); }
//   void SeekToLast() override { iter_.SeekToLast(); }
//   void Next() override { iter_.Next(); }
//   void Prev() override { iter_.Prev(); }
//   Slice key() const override { return iter_.key().key; }
//   Slice value() const override {
//     GlobalIndex::SkipListItem* ptr = &iter_.key();
//     char* p = reinterpret_cast<char*>(ptr);
//     return Slice(p, sizeof(GlobalIndex::SkipListItem));
//   }

//   Status status() const override { return Status::OK(); }

//  private:
//   GlobalIndex::GITable::Iterator iter_;
// };

}   // namespace ROCKSDB_NAMESPACE

