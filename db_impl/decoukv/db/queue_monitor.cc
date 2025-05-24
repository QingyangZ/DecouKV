#include "db/queue_monitor.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

// static Slice GetLengthPrefixedSlice(const char* data) {
//   uint32_t len;
//   const char* p = data;
//   p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
//   return Slice(p, len);
// }

KeyTable::KeyTable(const InternalKeyComparator& comparator)
  : comparator_(comparator),
//   bloom_(options_.use_datatable_bloom?  new MergeableBloom(options_) : nullptr),
	// table_(comparator_, &arena_, &(mem->table_), options_, bloom_),
  // IsLastTable(false),
  refs_(0) {
  mems_.clear();
}

// KeyTable::KeyTable(const InternalKeyComparator& comparator)
//   : comparator_(comparator),
    // bloom_(nullptr),
    // table_(comparator_),
    // IsLastTable(true),
    // refs_(0) {}

KeyTable::~KeyTable() {
  assert(refs_ == 0);
//   if (bloom_ != nullptr) {
//     delete bloom_;
//   }
}

// size_t KeyTable::ApproximateMemoryUsage() { return table_.get()..GetSize(); }

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
// static const char* EncodeKey(std::string* scratch, const Slice& target) {
//   scratch->clear();
//   PutVarint32(scratch, target.size());
//   scratch->append(target.data(), target.size());
//   return scratch->data();
// }

class DataTableIterator : public Iterator {
 public:
  explicit DataTableIterator(MemTable* mem, ReadOptions ro, Arena arena)
      : iter_(mem->NewIterator(ro, &arena)){}

  DataTableIterator(const DataTableIterator&) = delete;
  DataTableIterator& operator=(const DataTableIterator&) = delete;

  ~DataTableIterator() override = default;

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice& k) override { iter_->Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  // Slice key() const override { return GetLengthPrefixedSlice(iter_->key()); }
  // Slice value() const override {
  //   Slice key_slice = GetLengthPrefixedSlice(iter_->key());
  //   return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  // }

  Status status() const override { return Status::OK(); }

 private:
  InternalIterator* iter_;
  std::string tmp_;  // For passing to EncodeKey
};

void KeyTable::Add(MemTable* mem) { 
  mems_.emplace_front(mem);
  mem->Ref();
}

void KeyTable::Del(int n) {
  for(int i = 0; i < n; i++) {
    if (mems_.empty()) break;
    MemTable* mem = mems_.back();
    mem->Unref();
    mems_.pop_back();
  }
}

bool KeyTable::IsEmpty() { return (mems_.size() == 0); }

// Iterator* KeyTable::NewIterator() { 
//   Arena arena;
//   ScopedArenaIterator iter(mem->NewIterator(ro, &arena));
// }

const void* KeyTable::AddPtrWithNode(const ParsedInternalKey& key,
                                     InternalIterator** n_iter,
                                     InternalIterator** prev_iter) {
  for (; (*n_iter)->Valid(); (*n_iter)->Next()) {
    ParsedInternalKey ikey;
    ParseInternalKey((*n_iter)->key(), &ikey, false);
    if (comparator_.comparator.Compare(key, ikey) < 0) {
      assert((*prev_iter)->Valid() || (*prev_iter)->IsHead());
      return (*prev_iter)->GetNode();
    }
    if ((*prev_iter)->Valid()) {
      (*prev_iter)->Next();
    } else {
      (*prev_iter)->SeekToFirst();
    }
  }
  assert((*prev_iter)->Valid() || (*prev_iter)->IsHead());
  return (*prev_iter)->GetNode();
}

bool KeyTable::Get(const LookupKey& key, std::string* value,
                   std::string* timestamp, Status* s,
                   MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   const ReadOptions& read_opts,
                   ReadCallback* callback,
                   bool* is_blob_index, bool do_merge) {
  // if (bloom_ != nullptr) {
  //   Slice tmpkey = key.user_key();
  //   if(!(bloom_->KeyMayMatch(tmpkey))) {
  //     return false;
  //   }
  // }
  bool done = false;
  int i = 0;
  void* next_node = nullptr;
  for (MemTable* mem : mems_) {
    if(mem->GetByNode(key, value, timestamp, s, merge_context,
                  max_covering_tombstone_seq, read_opts, &next_node, callback)) {
      done = true;
      // std::cout << key.user_key().ToString() << std::endl;
      break;
    }
  }
  return done;
  // Slice memkey = key.memtable_key();
  // std::unique_ptr<MemTableRep>::Iterator iter(&table_);
  // iter.Seek(memkey.data());
  // if (iter.Valid()) {
  //   // entry format is:
  //   //    klength  varint32
  //   //    userkey  char[klength]
  //   //    tag      uint64
  //   //    vlength  varint32
  //   //    value    char[vlength]
  //   // Check that it belongs to same user key.  We do not check the
  //   // sequence number since the Seek() call above should have skipped
  //   // all entries with overly large sequence numbers.
  //   const char* entry = iter.key();
  //   uint32_t key_length;
  //   const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  //   if (comparator_.comparator.user_comparator()->Compare(
  //           Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
  //     // Correct user key
  //     const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
  //     switch (static_cast<ValueType>(tag & 0xff)) {
  //       case kTypeValue: {
  //         Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
  //         value->assign(v.data(), v.size());
  //         return true;
  //       }
  //       case kTypeDeletion:
  //         s = Status::NotFound(Slice());
  //         return true;
  //     }
  //   }
  // }
  // return false;
}

// Status KeyTable::Compact(KeyTable* dtable, SequenceNumber snum) {
// 	if(dtable != nullptr) {
//     if (IsLastTable) {
//       table_.LastTableCompact(&(dtable->table_), snum);
//     } else {
//       if (bloom_ != nullptr) {
//         bloom_->Merge(dtable->bloom_);
//       }
//       table_.Compact(&(dtable->table_), snum);
//     }
// 		return Status::OK();
// 	} else {
// 		return Status::Corruption("Compaction has sth wrong!");
// 	}
// }
}