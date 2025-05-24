// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "db/double_skiplist.h"
#include "table/block.h"
#include "table/filter_block.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

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
      Slice value;
      // the file number of the file that the index block is in
      uint64_t file_number;
      // the size of the file that the index block is in
      uint64_t file_size;
      // the node at next level of skiplist
      void* next_level_node = nullptr;
      // The filter that manages this data block
      SkipListItem(Slice key) {
        this->key = key;
      };
      SkipListItem(int num){};
      void SetNextNode(void* next_level_node) {
        this->next_level_node = next_level_node;
      };
      SkipListItem(Slice key, Slice value, uint64_t file_number,
                   uint64_t file_size) {
        this->key = key;
        this->value = value;
        this->file_number = file_number;
        this->file_size = file_size;
      }
      // Check whether the internal key may be in the data block
      // @param use_file_gran_filter_: this determines how we parse the filter block
      bool KeyMaybeInDataBlock(Slice internal_key, bool use_file_gran_filter_);
    };
    // TODO:
    // Comparator
    struct KeyComparator {
      const InternalKeyComparator* comparator;
      explicit KeyComparator(const InternalKeyComparator* c) { comparator = c; };
      int operator()(SkipListItem a, SkipListItem b) const;
    };
    
    Arena arena_;

    // If bloom filter is used, Whether to use filter blocks with file (sstable) granularity
    // If false, then use filter blocks with block granularity
    // bool use_file_gran_filter_ = true;
    
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
                       SkipListItem* item_ptr, GITable* gitable_,
                       void** next_level_, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&));
    void CheckLevel0(const ReadOptions& options,
                     std::vector<FileMetaData*> files_[config::kNumLevels]);
    void GlobalIndexBuilder(
        const ReadOptions& options,
        std::vector<FileMetaData*> files_[config::kNumLevels]);
    bool GetFromGlobalIndex(const ReadOptions& options, Slice user_key,
                            Slice internal_key, Status* my_s, void* arg,
                            void (*handle_result)(void*, const Slice&,
                                                  const Slice&));
    SkipListItem* InitItem(Slice key, Slice value, uint64_t file_number,
                           uint64_t file_size);
    void SkipListGlobalIndexBuilder(
        const ReadOptions& options, Iterator* iiter, FilterBlockReader* filter,
        uint64_t file_number, uint64_t file_size, GITable** gitable_,
        GITable::Node** next_level_node, GITable::Node* bound_node,
        bool is_first, GITable::Iterator** next_level_ptr);
    bool global_index_exists_ = false;
    void AddFile(int level, const ReadOptions& options, uint64_t file_numver,
                 uint64_t file_size, InternalKey smallest, InternalKey largest);
    void DeleteFile(int level, uint64_t file_number, const ReadOptions& options,
                    InternalKey smallest, InternalKey largest);
    VersionSet* vset_;

    std::vector<GITable*> index_files_level0;
    std::vector<GITable*> index_files_;
    void SeekPtr(Slice key, GITable::Iterator** next_level_ptr,
                GITable::Node** suit_node);
    void AddPtr(Slice key, GITable::Iterator** next_level_ptr,
                GITable::Node** suit_node);
    friend class Version;
};

class GlobalIndexIterator : public Iterator {
 public:
  explicit GlobalIndexIterator(GlobalIndex::GITable* table) : iter_(table) {}

  GlobalIndexIterator(const GlobalIndexIterator&) = delete;
  GlobalIndexIterator& operator=(const GlobalIndexIterator&) = delete;

  ~GlobalIndexIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }

  void Seek(const Slice& k) override { iter_.Seek(k); }
  void SeekWithNode(const Slice& k, void** start_node_) override {
    iter_.SeekWithNode(k, start_node_);
  }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return iter_.key().key; }
  Slice value() const override {
    GlobalIndex::SkipListItem* ptr = &iter_.key();
    char* p = reinterpret_cast<char*>(ptr);
    return Slice(p, sizeof(GlobalIndex::SkipListItem));
  }

  Status status() const override { return Status::OK(); }

 private:
  GlobalIndex::GITable::Iterator iter_;
};

class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  void AddGlobalIndexIterators(const ReadOptions& options,
                               std::vector<Iterator*>* iters,
                               leveldb::GlobalIndex* global_index_);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats, leveldb::GlobalIndex* global_index_);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;
  bool GetFromGlobalIndexL0(Slice user_key, Slice internal_key, void* arg,
                            bool (*func)(void*, int, FileMetaData*),
                            leveldb::GlobalIndex* global_index_,
                            const ReadOptions& options, Status* my_s,
                            void* arg1,
                            void (*handle_result)(void*, const Slice&,
                                                  const Slice&));
  // ***********************************************************
  // TODO:
  // class GlobalIndex;
  // GlobalIndex* global_index_;

  // ***********************************************************

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  Iterator* GlobalIndexConcatenatingIterator(const ReadOptions& options,
                                             Iterator* global_index_iter) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;
  friend class GlobalIndex;
  friend class DB;
  // ***************************************************************
  // TODO:
  // Done.
  // **********************************************************
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];
  friend class GlobalIndex;
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;
  friend class DBImpl;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
