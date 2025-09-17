#!/bin/bash

cd build
cmake .. && make -j64

# load 50GB
b="/mnt/nsd0/"; sudo ./db_bench --benchmarks="filluniquerandom,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none  --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=8388608 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri="" --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --use_existing_db=false --use_terark_table=false  --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > filluniquerandom.txt

# 1 threads overwrite 150GB
b="/mnt/nsd0/"; sudo ./db_bench --benchmarks="overwrite,stats" --key_size=24 --value_size=16384 --blob_size=512 --compression_type=none --mmap_read=false --use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --cache_size=8388608 --statistics --threads=1 --max_background_jobs=16 --max_write_buffer_number=4 --compaction_readahead_size=2097152 --bytestore_uri="" --db="${b}dbbench" --bloom_bits=10 --level_compaction_dynamic_level_bytes=true --writable_file_max_buffer_size=1048576 --level0_file_num_compaction_trigger=4 --num=6553600 --writes=19660800 --use_existing_db=true --use_terark_table=false --target_file_size_base=134217728 --target_blob_file_size=268435456 --write_buffer_size=134217728 --small_kv_ratio=0.5 > overwrite.txt
