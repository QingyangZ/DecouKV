// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()), create_if_missing(true),
      error_if_exists(false), paranoid_checks(false), env(Env::Default()),
      info_log(NULL), write_buffer_size(64 << 20), nvm_buffer_size(2*1073741824ull),
      num_levels(1), max_open_files(1000), block_cache(NULL), block_size(4096),
      block_restart_interval(16), compression(kNoCompression),
      reuse_logs(false),
      filter_policy(NULL) {
}

}  // namespace leveldb
