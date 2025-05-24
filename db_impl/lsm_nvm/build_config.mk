SOURCES=db/builder.cc db/dbformat.cc db/db_impl.cc db/db_iter.cc db/dumpfile.cc db/filename.cc db/log_reader.cc db/log_writer.cc db/memtable.cc db/table_cache.cc db/version_edit.cc db/version_set.cc db/write_batch.cc table/block_builder.cc table/block.cc table/filter_block.cc table/format.cc table/iterator.cc table/merger.cc table/table_builder.cc table/table.cc table/two_level_iterator.cc util/arena.cc util/bloom.cc util/BloomFilter.cc util/cache.cc util/coding.cc util/comparator.cc util/cpumap.cc util/crc32c.cc util/debug.cc util/env.cc util/env_posix.cc util/filter_policy.cc util/hash.cc util/histogram.cc util/logging.cc util/MurmurHash3.cc util/options.cc util/status.cc util/thpool.cc  port/port_posix.cc
MEMENV_SOURCES=helpers/memenv/memenv.cc
CC=cc
CXX=g++
PLATFORM=OS_LINUX
PLATFORM_LDFLAGS=-pthread
PLATFORM_LIBS=-lnuma -lsnappy -ltcmalloc
PLATFORM_CCFLAGS= -fno-builtin-memcmp -fpermissive -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DMULTILEVEL_IMMUTABLE -DUSE_OFFSETS -DENABLE_RECOVERY -D_ENABLE_PREDICTION -DSNAPPY
PLATFORM_CXXFLAGS=-std=c++0x -fno-builtin-memcmp -fpermissive -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DMULTILEVEL_IMMUTABLE -DUSE_OFFSETS -DENABLE_RECOVERY -D_ENABLE_PREDICTION -DSNAPPY
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_EXT=so
PLATFORM_SHARED_LDFLAGS=-shared -Wl,-soname -Wl,
PLATFORM_SHARED_VERSIONED=true
