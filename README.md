# Terark-DS
- Terark-DS: A High-Performance and Storage-Efficient Key-Value Separation Storage Engine on Disaggregated Storage
- Terark-DS was forked from [TerarkDB dev.1.4](https://github.com/bytedance/terarkdb), and TerarkDB was forked from RocksDB v5.18.3.
# Important
- **Core Support**: Terark-DS currently primarily supports ByteStore (a disaggregated storage system independently developed internally by ByteDance) as its underlying disaggregated storage backend.
- **Local File System Compatibility**: It is also compatible with local file systems for operation. However, when running on a local file system, only the optimizations related to garbage collection will take effect.
- **Requirement for Full Optimizations**: For access to the complete set of optimization technologies, it is necessary to build on a distributed storage system. This includes setting up the corresponding ByteStore storage cluster, or using other distributed storage systems provided that the relevant interfaces (to adapt to Terark-DS) are implemented for the latter.

# Build and Test
## Prepare
- Clone source code
```
git clone https://github.com/SZ-NPE/terark-ds.git
```
- Prepare submodules
```
git submodule update --init --recursive
```
## Build 
```
mkdir build
cd build

# Build with ByteStoreEnv and MockIO
cmake .. 
make -j32
```
- You will see the binary file `db_bench` in the `build` directory.

## Test
### 1. Test with Local File System
- Run `db_bench` with local directory `/mnt/nsd0/dbbench`
- **NOTE: When running based on the local POSIX interface, the differentiated redundancy strategy and adaptive WAL writing cannot take effect, as the necessary underlying distributed storage system is missing.** 
```
# filluniquerandom 50GB (Mixed large and small key-value pairs)
b="/mnt/nsd0/"; sudo ./db_bench --benchmarks="filluniquerandom,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none  --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=8388608 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri="" --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --use_existing_db=false --use_terark_table=false  --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > filluniquerandom.txt


# overwrite 150GB (Mixed large and small key-value pairs)
b="/mnt/nsd0/"; sudo ./db_bench --benchmarks="overwrite,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=134217728 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri="" --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --writes=19660800 --use_existing_db=true --use_terark_table=false --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > overwrite.txt

```

### 2. Test with ByteStore Cluster
- If you have ByteStore storage cluster, you should compile with ByteStore and disable MockIO
```
cd build
cmake .. -DWITH_BYTESTORE=ON -DMOCK_BYTESTORE=OFF
make -j32
```
- Run `db_bench` with bytestore uri `blob://[region-name]/[cluster-name]/public/`
```
# filluniquerandom 50GB 
b="`blob://xxx/xxx/public/"; ./db_bench --benchmarks="filluniquerandom,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=134217728 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri=$b --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --use_existing_db=false --use_terark_table=false  --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > filluniquerandom.txt


# overwrite 150GB
b="`blob://xxx/xxx/public/"; ./db_bench --benchmarks="filluniquerandom,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=134217728 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri=$b --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --writes=19660800 --use_existing_db=true --use_terark_table=false --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > overwrite.txt
```

### 3. Test with Other Storage Backend
- If you want to use another storage backend (other than ByteStore) as the underlying storage for Terark-DS, Terark-DS provides a mock interface for ByteStore. Users need to implement the relevant initialization and read-write interfaces of the corresponding storage system in the `bytestore_fs/mock.cc` by themselves.
- After implementing the mock interface, you can compile with ByteStoreEnv and MockIO. 
```
cd build
cmake .. -DWITH_BYTESTORE=ON -DMOCK_BYTESTORE=ON
make -j32
```
- Run `db_bench` with custom configurations, similar to the ByteStore cluster test.
