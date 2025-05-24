#include "db/global_index.h"

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <stack>
#include <ctime>

namespace ROCKSDB_NAMESPACE {

static bool NewestLast(FileMetaData* a, FileMetaData* b) {
  return a->fd.GetNumber() < b->fd.GetNumber();
}

GlobalIndex::~GlobalIndex() {
  for (auto itr = index_files_level0.begin(); itr != index_files_level0.end(); ++itr) {
    delete *itr;
  }
  for (auto itr = index_files_.begin(); itr != index_files_.end(); ++itr) {
    delete *itr;
  }
}

int GlobalIndex::KeyComparator::operator()(SkipListItem a, SkipListItem b) const {
  // assert(a.key.size() <= b.key.size());
  return user_comparator_->Compare(a.key, b.key);
}

GlobalIndex::SkipListItem* GlobalIndex::InitItem(Slice key, IndexValue value,
                                                 uint64_t file_number) {
  size_t item_size = sizeof(SkipListItem);
  SkipListItem* item_ptr = nullptr;
  item_ptr = (SkipListItem*)arena_.Allocate(item_size);
  char* key_data = (char*)arena_.Allocate(key.size());
  memcpy(key_data, key.data(), key.size());
  item_ptr->key = Slice(key_data, key.size());

  item_ptr->value = value;

  item_ptr->file_number = file_number;

  // SkipListItem* item_ptr = new SkipListItem(key_data, value_data, file_number, file_size);
  return item_ptr;
}

// Build skip-list of global index by index_block
void GlobalIndex::SkipListGlobalIndexBuilder(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    InternalIteratorBase<IndexValue>* iiter, const FileMetaData* f,
    GITable** gitable_, GITable::Node** next_level_node,
    GITable::Node* bound_node, bool is_first, bool is_last, int level,
    GITable::Iterator** next_level_iter) {
  // TODO
  // Format of an entry is concatenation of:
  //  key               : Slice
  //  value             : Slice
  //  file_number       : char[file_num.size]
  //  offset
  //  TODO Bloom filter...
  iiter->SeekToFirst();
  if (!iiter->Valid()) return;

  SkipListItem* item_ptr = nullptr;
  // SkipListItem item_ptr(iiter->key(), iiter->value(), f);

  if (iiter->Valid()) {
    item_ptr = InitItem(iiter->key(), iiter->value(), f->fd.GetNumber());
    if (*next_level_iter != nullptr) {
      if (is_first) {
        (*next_level_iter)->SeekToHead();
        *next_level_node = (*next_level_iter)->node_;
        (*next_level_iter)->SeekToFirst();
      }
    } else {
      *next_level_node = nullptr;
    }
    item_ptr->SetNextNode(*next_level_node);
  }
  iiter->Next();
  while (iiter->Valid()) {
    // assert(*next_level_node == nullptr ||
    //        (*next_level_node)->Next(0)->GetHeight() > 0);
    // if (*next_level_node != nullptr && level == 1)
    //   std::cout << "insert: key:" << item_ptr->key.ToString()
    //             << " ptr: " << item_ptr->next_level_node
    //             << " key: " << (*next_level_node)->key.key.ToString()
    //             << std::endl;
    (*gitable_)->SeqInsert(*item_ptr, bound_node);
    // (*gitable_)->Insert(*item_ptr);

    if (*next_level_iter != nullptr && !(*next_level_iter)->IsEmpty()) {
      AddPtr(item_ptr->key, next_level_iter, next_level_node); 
    } else {
      *next_level_node = nullptr;
    }
    item_ptr = InitItem(iiter->key(), iiter->value(), f->fd.GetNumber());

    item_ptr->SetNextNode(*next_level_node);
    iiter->Next();
  }
  // find the max key 
  // if(!is_last) {
  // Version* version = cfd_->current();
  // DataBlockIter block_iter;
  // Status s;
  // s = version->table_cache_->GetIterByIndexBlock(
  //     options, internal_comparator, *f,
  //     version->GetMutableCFOptions().prefix_extractor.get(), 
  //     cfd_->internal_stats()->GetFileReadHist(level),
  //     version->IsFilterSkipped(level, is_last), level,
  //     max_file_size_for_l0_meta_pin_, item_ptr->value, &block_iter);
  // if (s.ok()) block_iter.SeekToLast();

  // char* key_data = (char*)arena_.Allocate(f->largest.Encode().size());
  // memcpy(key_data, block_iter.key().data(), block_iter.key().size());
  // item_ptr->key = Slice(f->largest.Encode().data());

  // if (*next_level_node != nullptr && level == 1)
  //     std::cout << "insert: key:" << item_ptr->key.ToString()
  //               << " ptr: " << (*next_level_node)->key.key.ToString()
  //               << std::endl;
  (*gitable_)->SeqInsert(*item_ptr, bound_node);
  // (*gitable_)->Insert(*item_ptr);

  if (*next_level_iter != nullptr && !(*next_level_iter)->IsEmpty()) {
    AddPtr(item_ptr->key, next_level_iter, next_level_node);
    // std::cout << "insert: key:" << item_ptr->key.ToString()
    //           << " ptr: " << (*next_level_node)->key.key.ToString()
    //           << std::endl;
  } else {
    *next_level_node = nullptr;
  }
  return;
}

void GlobalIndex::AddFile(int level, const ReadOptions& options,
                          const FileMetaData* file_meta, InternalKey smallest,
                          InternalKey largest) {
  // std::cout << "AddFile: " << level << " fnumber:" << file_meta->fd.GetNumber()
  //           << " pointer: " << file_meta << std::endl;
  // std::cout << smallest.Encode().ToString() << "~"
  //           << largest.Encode().ToString() << std::endl;
  Version* version = cfd_->current();
  const InternalKeyComparator* icmp =
      version->storage_info()->InternalComparator();
  const KeyComparator kcmp = KeyComparator(user_comparator_);
  GITable::Iterator* gitable_iter = nullptr;
  GITable::Iterator* next_level_iter = nullptr;
  GITable::Iterator* last_level_iter = nullptr;
  GITable *next_gitable, *gitable, *last_gitable;
  GITable::Node* next_level_node = nullptr;
  GITable::Node* bound_node = nullptr;
  IndexBlockIter iiter_on_stack;
  bool is_first;
  // get the index block of this sstable into iiter
  auto iiter = version->table_cache_->IndexBlockGet(
      options, *icmp, *file_meta,
      version->GetMutableCFOptions().prefix_extractor.get(),
      cfd_->internal_stats()->GetFileReadHist(level),
      false, level,
      max_file_size_for_l0_meta_pin_, &iiter_on_stack);
  iiter->SeekToFirst();

  if (level == 0) {
    // 对level0修改，直接插入到头部
    if (index_files_level0.size() == 0) {
      next_gitable = nullptr;
      next_level_iter = nullptr;
    } else {
      next_gitable = index_files_level0[0];
      next_level_iter = new GITable::Iterator(next_gitable);
      (*next_level_iter).SeekToFirst();
    }
    gitable = new GITable(kcmp, &arena_);
    bound_node = gitable->tail_;
    SkipListGlobalIndexBuilder(options, *icmp, iiter, file_meta, &gitable,
                                 &next_level_node, bound_node, true,
                                 true, level, &next_level_iter);

    index_files_level0.insert(index_files_level0.begin(), gitable);
  } else {
    if (level == 1) {
      // TODO: 对level1修改
      // DONE.
      gitable = index_files_[0];
      if (index_files_level0.size() == 0) {
        last_gitable = nullptr;
      } else
        last_gitable = index_files_level0[index_files_level0.size() - 1];
      next_gitable = index_files_[1];
    } else {
      gitable = index_files_[level - 1];
      last_gitable = index_files_[level - 2];
      if (level >= index_files_.size()) next_gitable = nullptr;
      else
        next_gitable = index_files_[level];
    }
    // TODO: 加入下一层指针并插入
    gitable_iter = new GITable::Iterator(gitable);
    next_level_iter = new GITable::Iterator(next_gitable);
    // next_level_iter->SeekToFirst();
    std::string abc = "abc";
    SkipListItem* item_ptr =
        new SkipListItem(ExtractUserKey(smallest.Encode()));
    gitable_iter->Seek(*item_ptr);
    bound_node = gitable_iter->node_;
    if (bound_node == nullptr) 
      bound_node = gitable->tail_;
    else if(file_meta->fd.GetNumber() == bound_node->key.file_number)
      return;

    if (next_gitable == nullptr || next_gitable->IsEmpty()) {
      next_level_iter = nullptr;
    } else {
      next_level_iter->SeekToFirst();
      if (gitable->IsEmpty()) {
        is_first = true;
      }
      else if (gitable_iter->Valid()) {
        gitable_iter->Prev();
        if (gitable_iter->Valid()) {
          is_first = false;
          SeekPtr(gitable_iter->key().key, &next_level_iter, &next_level_node);
        } else {
          is_first = true;
          // next_level_iter->SeekToFirst();
        }
      } else {
        gitable_iter->SeekToLast();
        SeekPtr(gitable_iter->key().key, &next_level_iter, &next_level_node);
        // next_level_iter->Seek(gitable_iter->key());
        // AddPtr(gitable_iter->key().key, &next_level_iter, &next_level_node);
      }
    }
    SkipListGlobalIndexBuilder(options, *icmp, iiter, file_meta, &gitable,
                               &next_level_node, bound_node, is_first, false,
                               level, &next_level_iter);
    // TODO: 修改同一层指针
    SkipListItem* large_item_ptr =
        new SkipListItem(ExtractUserKey(largest.Encode()));
    gitable_iter = new GITable::Iterator(gitable);
    gitable_iter->Seek(*large_item_ptr);
    if (next_level_iter != nullptr && !next_level_iter->IsEmpty()) {
      // AddPtr(item_ptr->key, &next_level_iter, &next_level_node);
      gitable_iter->Next();
      if (gitable_iter->Valid()) {
        assert(next_level_node != nullptr);
        gitable_iter->key().SetNextNode(next_level_node);
      }
    } 
    // TODO2: 修改前一层指针
    if (last_gitable == nullptr) return;
    last_level_iter = new GITable::Iterator(last_gitable);
    if (last_level_iter->IsEmpty()) return;
    
    // gitable_iter->Prev();
    if (is_first) {
      last_level_iter->SeekToFirst();
      gitable_iter->SeekToHead();
      last_level_iter->key().SetNextNode(gitable_iter->node_);
      
    } else {
      gitable_iter->Seek(*item_ptr);
      last_level_iter->Seek(gitable_iter->key());
    }

    if (!last_level_iter->Valid()) return;
    SeekPtr(last_level_iter->key().key, &gitable_iter, &next_level_node);
    while (user_comparator_->Compare((last_level_iter->key()).key, largest.Encode()) <= 0) {
      last_level_iter->Next();
      if (!last_level_iter->Valid()) return;
      last_level_iter->key().SetNextNode(next_level_node);
      assert(next_level_node != nullptr);
      AddPtr(last_level_iter->key().key, &gitable_iter, &next_level_node);
    }
    last_level_iter->Next();
    if (!last_level_iter->Valid()) return;
    last_level_iter->key().SetNextNode(next_level_node);
    assert(next_level_node != nullptr);
    delete large_item_ptr;
    delete item_ptr;
  }

  // delete iiter;
  delete gitable_iter;
  delete next_level_iter;
  delete last_level_iter;
}

void GlobalIndex::DeleteFile(int level, uint64_t file_number,
                             const ReadOptions& options,
                             InternalKey smallest, InternalKey largest) {
  // std::cout << "DeleteFile: " << level << " fnumber" << file_number
  //           << std::endl;
  // std::cout << smallest.Encode().ToString() << "~"
  //           << largest.Encode().ToString() << std::endl;
  GITable::Iterator* index_iter = nullptr;
  GITable::Iterator* next_gitable_iter = nullptr;
  GITable* next_gitable, *gitable;
  GITable::Node* next_level_node = nullptr;
  SkipListItem smallest_item = SkipListItem(ExtractUserKey(smallest.Encode()));
  SkipListItem largest_item = SkipListItem(ExtractUserKey(largest.Encode()));
  GITable::Node *prev_node, *next_node;
  SkipListItem *del, *node;
  Version* version = cfd_->current();
  bool is_last;
  if (level != 0) {
    GITable* list = index_files_[level - 1];
    bool is_head = false;
    assert(list != nullptr);
    index_iter = new GITable::Iterator(list);
    index_iter->Seek(smallest_item);
    // if (index_iter->key().file_number == file_number)
    //   std::cout << " pointer: " << index_iter->key().f << std::endl;
    // else
    //   std::cout << "delete false" << std::endl;
    if (!index_iter->Valid()) {
      return;
    }
    index_iter->Prev();
    if (!index_iter->Valid()) {
      index_iter->SeekToHead();
      is_head = true;
    } else
      is_head = false;
    prev_node = index_iter->node_;
    index_iter->Next();
    // if (index_iter->key().file_number != file_number) {
    //   std::cout << "delete false!" << std::endl;
    //   return;
    //   // for (int i = 0; i < index_files_.size(); i++) {
    //   //   list = index_files_[i];
    //   //   index_iter = new GITable::Iterator(list);
    //   //   index_iter->Seek(smallest_item);
    //   // }
    // }
    // assert(index_iter->key().file_number == file_number);
    next_node = list->Delete(smallest_item, largest_item);
    if (next_node == nullptr) {
      is_last = true;
      index_iter->SeekToLast();
      next_node = index_iter->node_;
    } else {
      is_last = false;
      // 处理同一层的下一个结点指针
      if (level < index_files_.size()) {
        next_gitable = index_files_[level];
        next_gitable_iter = new GITable::Iterator(next_gitable);
        if (!next_gitable_iter->IsEmpty()) {
          if (is_head) {
            next_gitable_iter->SeekToHead();
            next_level_node = next_gitable_iter->node_;
          } else {
            SeekPtr(prev_node->key.key, &next_gitable_iter, &next_level_node);
            // next_gitable_iter->Seek(prev_node->key);
            // if (next_gitable_iter->Valid()) {
            //   next_gitable_iter->Prev();
            //   if (!next_gitable_iter->Valid()) {
            //     next_gitable_iter->SeekToHead();
            //   }
            // }
            // else
            //   next_gitable_iter->SeekToLast();
          }
          assert(next_level_node != nullptr);
          next_node->key.SetNextNode(next_level_node);
        }
        
      }
    }
    
    // while (index_iter->Valid() &&
    //        vset_->icmp_.Compare(index_iter->key().key, largest.Encode()) <= 0) {
    //   del = &index_iter->key();
    //   assert(index_iter->key().file_number == file_number);
    //   index_iter->Next();
      
    //   list->Delete(*del);
    //   delete node;
    // }
    // assert(!list->IsEmpty()); // 这里断言不会把某个level全部删掉，但严格来说是可能出现的情况
    // TODO: 处理层间指针
    // DONE.
    if (level == 1) {
      int level0_size = index_files_level0.size();
      if (level0_size == 0) return;
      gitable = index_files_level0[level0_size - 1];
    } else {
      gitable = index_files_[level - 2];
      if (gitable == nullptr) return;
    }
    index_iter = new GITable::Iterator(gitable);

    assert(prev_node != nullptr);
    
    if (is_head) {
      index_iter->SeekToFirst();
      if (!index_iter->Valid()) return;
      index_iter->key().SetNextNode(prev_node);
    } else {
      index_iter->Seek(smallest_item);
      if (!index_iter->Valid()) return;
    }
    index_iter->Next();
    if (is_head && is_last) {
      // empty
    } else {
      while (index_iter->Valid() &&
            user_comparator_->Compare(index_iter->key().key, next_node->key.key) <=0) {
        index_iter->key().SetNextNode(prev_node);
        index_iter->Next();
      }
    }
    if (is_last) {
      while (index_iter->Valid()) {
        index_iter->key().SetNextNode(prev_node);
        index_iter->Next();
      }
    } else if (index_iter->Valid()) {
      index_iter->key().SetNextNode(prev_node);
    }
  } else {
    int i = 0;
    for (std::vector<GITable*>::iterator iiter = index_files_level0.begin();
          iiter != index_files_level0.end(); iiter++, i++) {
      gitable = index_files_level0[i];
      index_iter = new GITable::Iterator(gitable);
      index_iter->SeekToFirst();
      if (index_iter->Valid() && index_iter->key().file_number == file_number) {
        // TODO:处理层间指针
        iiter = index_files_level0.erase(iiter);
        if (i == 0) {
          // DONE
        } else {
          index_iter = new GITable::Iterator(index_files_level0[i - 1]);
          if (i == index_files_level0.size()) {
            // last SST in level 0.
            next_gitable = index_files_[0];
          } else {
            // other.
            next_gitable = index_files_level0[i];
          }
          next_gitable_iter = new GITable::Iterator(next_gitable);
          index_iter->SeekToFirst();
          next_gitable_iter->SeekToFirst();
          if (!next_gitable_iter->Valid()) return;
          next_gitable_iter->SeekToHead();
          next_level_node = next_gitable_iter->node_;
          next_gitable_iter->Next();
          (index_iter->key()).SetNextNode(next_level_node);
          while (index_iter->Valid()) {
            AddPtr(index_iter->key().key, &next_gitable_iter, &next_level_node);
            assert(next_level_node != nullptr);
            index_iter->Next();
            if (!index_iter->Valid()) break;
            (index_iter->key()).SetNextNode(next_level_node);
          }
        }
        
        break;
      }
      // std::cout << " fnumber: " << index_iter->key().file_number << std::endl;
    }
  }
  // std::cout << "DeleteFile: " << level << " fnumber" << file_number
  //           << std::endl;
  delete index_iter;
  delete next_gitable_iter;
  return;
}

void GlobalIndex::SeekPtr(Slice key, GITable::Iterator** next_level_ptr,
                         GITable::Node** suit_node) {
  // TODO: 增加层间指针
  if ((*next_level_ptr) != nullptr) {
    (*next_level_ptr)->Seek(key);
    if (!(*next_level_ptr)->Valid()) {
      (*next_level_ptr)->SeekToLast();
    } else {
      (*next_level_ptr)->Prev();
      if (!(*next_level_ptr)->Valid()) {
        (*next_level_ptr)->SeekToHead();
      }
    }
  }
  *suit_node = (*next_level_ptr)->node_;
  assert(*suit_node != nullptr);
  (*next_level_ptr)->Next();
  return;
}

void GlobalIndex::AddPtr(Slice key, GITable::Iterator** next_level_ptr,
                         GITable::Node** suit_node) {
  // TODO: 增加层间指针

  assert((*next_level_ptr) != nullptr);

  while ((*next_level_ptr)->Valid()) {
    // std::cout << key.ToString() << std::endl;
    // std::cout << (*next_level_ptr)->key().key.ToString() << std::endl;
    if (user_comparator_->Compare((*next_level_ptr)->key().key, key) > 0) {
      (*next_level_ptr)->Prev();
      if (!(*next_level_ptr)->Valid()) {
        (*next_level_ptr)->SeekToHead();
      }
      break;
    }
    (*next_level_ptr)->Next();
  }
  if (!(*next_level_ptr)->Valid() && !(*next_level_ptr)->IsHead()) {
    (*next_level_ptr)->SeekToLast();
  }
  *suit_node = (*next_level_ptr)->node_;
  assert(*suit_node != nullptr);
  (*next_level_ptr)->Next();
  return;
}

// void GlobalIndex::CheckLevel0(const ReadOptions& options,
//     std::vector<FileMetaData*> files_[config::kNumLevels]) {
//   if (files_[0].size() == index_files_level0.size()) return;
//   if (files_[0].size() == index_files_level0.size() + 1) {
//     std::vector<FileMetaData*> tmp;
//     tmp.reserve(files_[0].size());

//     for (uint32_t i = 0; i < files_[0].size(); i++) {
//       FileMetaData* f = files_[0][i];
//       tmp.emplace_back(f);
//     }
//     if (!tmp.empty()) {
//       std::sort(tmp.begin(), tmp.end(), NewestFirst);
//     }
//     AddFile(0, options, tmp[0]->number, tmp[0]->file_size, tmp[0]->smallest,
//             tmp[0]->largest);
//   }
// }

// build global index
void GlobalIndex::GlobalIndexBuilder(const ReadOptions& options,
                                     VersionStorageInfo* vstorage) {
  // Search other levels.
  user_comparator_ = vstorage->InternalComparator()->user_comparator();
  const InternalKeyComparator* icmp = vstorage->InternalComparator();
  const KeyComparator kcmp = KeyComparator(user_comparator_);
  // init a new global index table
  GITable* gitable_ = nullptr;
  // the node at next level
  GITable::Node* next_level_node = nullptr;
  // the stack to store each level of global index table
  std::stack<GITable*> S;
  GITable::Iterator* skiplist_iter_ptr = nullptr;
  Version* version = cfd_->current();
  // is_last denotes that the file is the last one on that level
  // is_first denotes that the file is the first one on that level
  bool is_first, is_last;
  // iterate from highest level to level 1
  for (int level = vstorage->num_levels() - 1; level > 0; level--) {
    gitable_ = new GITable(kcmp, &arena_);
    size_t num_files = vstorage->LevelFiles(level).size();
    if (num_files == 0) {
      skiplist_iter_ptr = nullptr;
      S.push(gitable_);
      // std::cout << "level: " << gitable_->level << " "
      //           << static_cast<const void*>(gitable_->tail_) << std::endl;
      continue;
    }
    // for each SStable
    for (uint32_t i = 0; i < num_files; i++) {
      is_first = (i == 0) ? true : false;
      is_last = (i == num_files - 1) ? true : false;
      // get the meta data of that file
      FileMetaData* f = vstorage->LevelFiles(level)[i];
      IndexBlockIter iiter_on_stack;

      // get the index block of this sstable into iiter
      auto iiter = version->table_cache_->IndexBlockGet(
          options, *icmp, *f,
          version->GetMutableCFOptions().prefix_extractor.get(),
          cfd_->internal_stats()->GetFileReadHist(level),
          version->IsFilterSkipped(level, is_last), level,
          max_file_size_for_l0_meta_pin_, &iiter_on_stack);
      // iiter->SeekToFirst();
      // insert this index block into the skiplist global index table
      SkipListGlobalIndexBuilder(options, *icmp, iiter, f, &gitable_,
                                 &next_level_node, gitable_->tail_, is_first,
                                 is_last, level, &skiplist_iter_ptr);
      // vset->table_cache_->IndexBlockDelete(f->number, f->file_size);
      // delete iiter;
    }

    skiplist_iter_ptr = new GITable::Iterator(gitable_);
    (*skiplist_iter_ptr).SeekToFirst();
    S.push(gitable_);
    // gitable_ = new GITable(kcmp, &arena_, level - 1);
    // std::cout << "level: " << gitable_->level << " "
    //           << static_cast<const void*>(gitable_->tail_) << std::endl;
  }
  while (!S.empty()) {
    index_files_.emplace_back(S.top());
    S.pop();
  }

  // Search level-0 in order from newest to oldest.

  std::vector<FileMetaData*> tmp;
  tmp.reserve(vstorage->LevelFiles(0).size());

  for (uint32_t i = 0; i < vstorage->LevelFiles(0).size(); i++) {
    FileMetaData* f = vstorage->LevelFiles(0)[i];
    tmp.emplace_back(f);
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestLast);
    // std::cout << "level: 0" << std::endl;
    for (uint32_t i = 0; i < tmp.size(); i++) {
      is_last = (i == tmp.size() - 1) ? true : false;
      
      FileMetaData* f = tmp[i];
      IndexBlockIter iiter_on_stack;

      auto iiter = version->table_cache_->IndexBlockGet(
          options, *icmp, *f,
          version->GetMutableCFOptions().prefix_extractor.get(),
          cfd_->internal_stats()->GetFileReadHist(0),
          version->IsFilterSkipped(0, is_last), 0,
          max_file_size_for_l0_meta_pin_, &iiter_on_stack);
      // iiter->SeekToFirst();

      gitable_ = new GITable(kcmp, &arena_);
      SkipListGlobalIndexBuilder(options, *icmp, iiter, f, &gitable_,
                                 &next_level_node, gitable_->tail_, true,
                                 is_last, 0, &skiplist_iter_ptr);
      S.push(gitable_);
      skiplist_iter_ptr = new GITable::Iterator(gitable_);
      (*skiplist_iter_ptr).SeekToFirst();
      // delete iiter;
      // vset->table_cache_->IndexBlockDelete(f->number, f->file_size);
    }
    while (!S.empty()) {
      index_files_level0.emplace_back(S.top());
      S.pop();
    }
  }

  delete skiplist_iter_ptr;
}
std::atomic<int> false_count(0);
// search the key in a skiplist
void GlobalIndex::SearchGITable(const ReadOptions& options, Slice internal_key,
                                GetContext* get_context, int level,
                                SkipListItem* item_ptr, Status* status,
                                GITable* gitable_, void** next_level_) {
  GITable::Iterator* index_iter = nullptr;
  SkipListItem* found_item = nullptr;
  index_iter = new GITable::Iterator(gitable_);
  if (gitable_->IsEmpty()) {
    *next_level_ == nullptr;
    return;
  }
  if (*next_level_ != nullptr) {
    // std::cout << "seek with node" << std::endl;
    index_iter->SeekWithNode(*item_ptr, next_level_);
  } else {
    // std::cout << "seek without node" << std::endl;
    index_iter->Seek(*item_ptr);
  }
  // index_iter->Seek(*item_ptr);
  if(index_iter->Valid()) {
    // Found.
    
    found_item = &index_iter->key();
    // std::cout << "key: " << found_item->key.ToString()
    //           << " my found filenum: " << found_item->f->fd.GetNumber()
    //           << " range: " << found_item->f->smallest.Encode().ToString()
    //           << "~" << found_item->f->largest.Encode().ToString() << std::endl;
    Version* version = cfd_->current();
    const InternalKeyComparator* icmp =
        version->storage_info()->InternalComparator();
    // 汇总level0的信息，确定下一层的指针位置。
    *next_level_ = found_item->next_level_node;

    // if (get_context->sample()) {
    //   sample_file_read_inc(const_cast<FileMetaData*>(found_item->f));
    // }

    bool timer_enabled =
        GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
        get_perf_context()->per_level_perf_context_enabled;
    StopWatchNano timer(version->clock_, timer_enabled /* auto_start */);
    // assert(sv != SuperVersion::kSVObsolete);
    // FileMetaData* fd;
    // int l;
    // ColumnFamilyData* cfd;
    // cfd_->current()->version_set()->GetMetadataForFile(found_item->file_number,
    //                                                    &l, &fd, &cfd);
    FileMetaData* fd = version->storage_info()->GetFileMetaDataByNumber(
        found_item->file_number);
    if (fd == nullptr) {
      if (level != 0) {
        false_count++;
        // std::cout << "level: " << level << " false_count" << false_count << std::endl;
      }
      return;
    }
    *status = version->table_cache_->GetByIndexValue(
        options, *icmp, *fd, internal_key, found_item->value, get_context,
        version->mutable_cf_options_.prefix_extractor.get(),
        cfd_->internal_stats()->GetFileReadHist(level), false, level,
        version->max_file_size_for_l0_meta_pin_);
    // found;
    
    if (timer_enabled) {
      PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                                level);
    }
    if (!status->ok()) {
      return;
    }

  } else {
    // *next_level_ = nullptr;

    // 可以调整为：
    index_iter->SeekToLast();
    found_item = &index_iter->key();
    *next_level_ = (GITable::Node*)found_item->next_level_node;
  }
  delete index_iter;
}

// get the key-value from global index table
void GlobalIndex::GetFromGlobalIndex(
    const ReadOptions& read_options, const LookupKey& k, PinnableSlice* value,
    std::string* timestamp, Status* status, MergeContext* merge_context,
    SequenceNumber* max_covering_tombstone_seq, bool* value_found,
    bool* key_exists, SequenceNumber* seq, ReadCallback* callback,
    bool* is_blob, bool do_merge) {
  // TODO:
  assert(global_index_exists_);
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();

  assert(status->ok() || status->IsMergeInProgress());

  void* next_level_ = nullptr;
  // std::cout << "key to found: " << internal_key.ToString() << std::endl;
  SkipListItem* item_ptr = new SkipListItem(ExtractUserKey(ikey));

  if (index_files_level0.size() == 0 && index_files_.size() == 0) {
    *status = Status::NotFound();
    return;
  }

  if (key_exists != nullptr) {
    // will falsify below if not found
    *key_exists = true;
  }
  PinnedIteratorsManager pinned_iters_mgr;
  uint64_t tracing_get_id = BlockCacheTraceHelper::kReservedGetId;
  VersionSet* vset_ = cfd_->current()->vset_;
  if (vset_ && vset_->block_cache_tracer_ &&
      vset_->block_cache_tracer_->is_tracing_enabled()) {
    tracing_get_id = vset_->block_cache_tracer_->NextGetId();
  }
  // Note: the old StackableDB-based BlobDB passes in
  // GetImplOptions::is_blob_index; for the integrated BlobDB implementation, we
  // need to provide it here.
  bool is_blob_index = false;
  bool* const is_blob_to_use = is_blob ? is_blob : &is_blob_index;

  GetContext get_context(
      cfd_->current()->user_comparator(), cfd_->current()->merge_operator_,
      cfd_->current()->info_log_, cfd_->current()->db_statistics_,
      status->ok() ? GetContext::kNotFound : GetContext::kMerge, user_key,
      do_merge ? value : nullptr, do_merge ? timestamp : nullptr, value_found,
      merge_context, do_merge, max_covering_tombstone_seq,
      cfd_->current()->clock_, seq,
      cfd_->current()->merge_operator_ ? &pinned_iters_mgr : nullptr, callback,
      is_blob_to_use, tracing_get_id);

  // Pin blocks that we read to hold merge operands
  if (cfd_->current()->merge_operator_) {
    pinned_iters_mgr.StartPinning();
  }
  ReadLock rl(&rw_mutex_);
  // Search level0
  for (uint32_t i = 0; i < index_files_level0.size(); i++) {
    if (*max_covering_tombstone_seq > 0) {
      // The remaining files we look at will only contain covered keys, so we
      // stop here.
      break;
    }
    // std::cout << "my level: 0, file: " << i << std::endl;
    SearchGITable(read_options, ikey, &get_context, 0, item_ptr, status,
                  index_files_level0[i], &next_level_);
    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        cfd_->current()->db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        continue;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        continue;
      case GetContext::kFound:
        RecordTick(cfd_->current()->db_statistics_, GET_HIT_L0);

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1, 0);
        delete item_ptr;
        return;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        delete item_ptr;
        return;
      case GetContext::kCorrupt:
        *status = Status::Corruption("corrupted key for ", user_key);
        delete item_ptr;
        return;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(cfd_->current()->info_log_,
                        "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        delete item_ptr;
        return;
    }
  }

  // Search other levels
  
  for (uint32_t i = 0; i < index_files_.size(); i++) {
    if (*max_covering_tombstone_seq > 0) {
      // The remaining files we look at will only contain covered keys, so we
      // stop here.
      break;
    }
    // std::cout << "my level: " << i + 1 << std::endl;
    SearchGITable(read_options, ikey, &get_context, i + 1, item_ptr, status,
                  index_files_[i], &next_level_);
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        cfd_->current()->db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        continue;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        continue;
      case GetContext::kFound:
        RecordTick(cfd_->current()->db_statistics_, GET_HIT_L0);

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1, 0);
        delete item_ptr;
        return;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        delete item_ptr;
        return;
      case GetContext::kCorrupt:
        *status = Status::Corruption("corrupted key for ", user_key);
        delete item_ptr;
        return;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(cfd_->current()->info_log_,
                        "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        delete item_ptr;
        return;
    }
  }
  // std::cout << "not found!" << std::endl;
  delete item_ptr;
  return;
}

void GlobalIndex::EditFiles(ReadOptions read_options) { 
  assert(!EditEmpty());
  global_index_exists_ = false;
  WriteLock wl(&rw_mutex_);
  for (auto del_iter = delete_files_.rbegin(); 
       del_iter != delete_files_.rend(); 
       del_iter++) {
    DeleteFile(del_iter->second.level_, del_iter->first, read_options,
               del_iter->second.smallest_, del_iter->second.largest_);
  }
  delete_files_.clear();
  for (auto add_file : add_files_) {
    FileMetaData* f =
          cfd_->current()->storage_info()->GetFileMetaDataByNumber(
              add_file.first);
    if (f == nullptr) return;
    AddFile(add_file.second.level_, read_options, f, f->smallest, f->largest);
  }
  add_files_.clear();
  global_index_exists_ = true;
  return;
}

// bool Version::GetFromGlobalIndexL0(
//     Slice user_key, Slice internal_key, void* arg,
//     bool (*func)(void*, int, FileMetaData*),
//     leveldb::GlobalIndex* global_index_, const ReadOptions& options,
//     Status* my_s, void* arg1,
//     void (*handle_result)(void*, const Slice&, const Slice&)) {
      
//   assert(global_index_->global_index_exists_);
//   Saver* saver = reinterpret_cast<Saver*>(arg1);
//   void* next_level_ = nullptr;
//   GlobalIndex::SkipListItem* item_ptr =
//       new GlobalIndex::SkipListItem(internal_key);

//   for (uint32_t i = 0; i < global_index_->index_files_level0.size(); i++) {
//     // std::cout << "my level: 0, file: " << i << std::endl;
//     global_index_->SearchGITable(options, internal_key, item_ptr,
//                                  global_index_->index_files_level0[i],
//                                  &next_level_, saver, handle_result);
//     switch (saver->state) {
//       case kFound:
//         *my_s = Status::OK();
//         return true;
//       case kDeleted:
//         *my_s = Status::NotFound(Slice());
//         return false;
//       case kCorrupt:
//         *my_s = Status::Corruption("corrupted key for ", saver->user_key);
//         return true;
//     }
//   }

//   // Search other levels.
//   const Comparator* ucmp = vset_->icmp_.user_comparator();
//   for (int level = 1; level < config::kNumLevels; level++) {
//     // std::cout << "level: " << level << std::endl;
//     size_t num_files = files_[level].size();
//     if (num_files == 0) continue;

//     // Binary search to find earliest index whose largest key >= internal_key.
//     uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
//     if (index < num_files) {
//       FileMetaData* f = files_[level][index];
//       if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
//         // All of "f" is past any data for user_key
//       } else {
//         if (!(*func)(arg, level, f)) {
//           // std::cout << "file number: " << f->number << std::endl;
//           return true;
//         }
//       }
//     }
//   }
// }

// ****************************************************

} // namespace ROCKSDB_NAMESPACE

