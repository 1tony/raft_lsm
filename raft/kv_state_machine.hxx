/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include "libnuraft/nuraft.hxx"
#include"../lsm/lsm.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>

#include <string.h>

using namespace nuraft;

namespace kv_server {
class kv_state_machine : public state_machine {
public:
    kv_state_machine(bool async_snapshot = false)
        : cur_value_(0)
        , last_committed_idx_(0)
        , async_snapshot_(async_snapshot)
        {}

    ~kv_state_machine() {}

    enum op_type : int {
        PUT = 0x0,
        RM = 0x1,
        SET = 0x2
    };

    struct op_payload {
        op_type type_;
        int key;
        int value;
    };

    static ptr<buffer> enc_log(const op_payload& payload) {
        ptr<buffer> ret = buffer::alloc(sizeof(op_payload));
        buffer_serializer bs(ret);
        bs.put_raw(&payload, sizeof(op_payload));

        return ret;
    }

    static void dec_log(buffer& log, op_payload& payload_out) {
        // Decode from Raft log to {operator, operand} pair.
        assert(log.size() == sizeof(op_payload));//暂时搁置
        buffer_serializer bs(log);
        memcpy(&payload_out, bs.get_raw(log.size()), sizeof(op_payload));
    }

    void printLSM(){
        lsm.printElts();
    }
    void rangeLSM(int k1,int k2){
        auto res = lsm.range(k1,k2);
        if (!res.empty()){
            for (int i = 0; i < res.size(); ++i){
                std::cout << res[i].key << ":" << res[i].value << " "<<std::endl;
            }
        }
    }

    void lookupLSM(int k1){
        int v1;
        bool found = lsm.lookup(k1,v1);
        if (found) {
            std::cout << v1<<std::endl;;
        }
    }

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) {
        // Nothing to do with pre-commit in this example.
        return nullptr;
    }

    ptr<buffer> commit(const ulong log_idx,buffer &data){
        //提交，还是要根据操作，来对kv_map做对应的处理，先默认add
        //1、解析数据
        op_payload payload;
        dec_log(data,payload);//将data中的数据反序并复制到payload

        //2 将数据读取后，需要根据符号的指示进行相关工作
        if(payload.type_ == kv_state_machine::op_type::PUT){
            //kv_map_[payload.key]=payload.value;
            lsm.insert_key(payload.key,payload.value);
        }else if(payload.type_ == kv_state_machine::op_type::RM){
            //kv_map_[payload.key] = INT8_MIN;//设定一个无效值
            lsm.delete_key(payload.key);
        }
        
        last_committed_idx_ = log_idx;
        //3、返回log_idx作为结果
        ptr<buffer> ret = buffer::alloc( sizeof(log_idx) );
        buffer_serializer bs(ret);
        bs.put_u64(log_idx);
        return ret;

    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) {//后面再考虑是否加入配置文件的输入
        // Nothing to do with configuration change. Just update committed index.
        last_committed_idx_ = log_idx;
    }

    void rollback(const ulong log_idx, buffer& data) {
        // Nothing to do with rollback,
        // as this example doesn't do anything on pre-commit.
    }

    int read_logical_snp_obj(snapshot& s,
                             void*& user_snp_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj)
    {
        ptr<snapshot_ctx> ctx = nullptr;
        {   std::lock_guard<std::mutex> ll(snapshots_lock_);
            auto entry = snapshots_.find(s.get_last_log_idx());
            if (entry == snapshots_.end()) {
                // Snapshot doesn't exist.
                data_out = nullptr;
                is_last_obj = true;
                return 0;
            }
            ctx = entry->second;
        }

        if (obj_id == 0) {
            // Object ID == 0: first object, put dummy data.
            data_out = buffer::alloc( sizeof(int32) );
            buffer_serializer bs(data_out);
            bs.put_i32(0);
            is_last_obj = false;

        } else {
            // Object ID > 0: second object, put actual value.
            data_out = buffer::alloc( sizeof(ulong) );
            buffer_serializer bs(data_out);
            bs.put_raw( &ctx->kv_map_s_,sizeof(ctx->kv_map_s_));//bs中存放的日志中的value值
            is_last_obj = true;
        }
        return 0;
    }

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              buffer& data,
                              bool is_first_obj,
                              bool is_last_obj)
    {
        if (obj_id == 0) {
            // Object ID == 0: it contains dummy value, create snapshot context.
            ptr<buffer> snp_buf = s.serialize();
            ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
            create_snapshot_internal(ss);

        } else {
            // Object ID > 0: actual snapshot value.
            buffer_serializer bs(data);
            std::unordered_map<int,int> *local_map = static_cast<std::unordered_map<int,int>*>(bs.data());//这里还要考虑如何转换
            //std::unordered_map<int,int> local_map;
            std::lock_guard<std::mutex> ll(snapshots_lock_);
            auto entry = snapshots_.find(s.get_last_log_idx());
            assert(entry != snapshots_.end());
            entry->second->kv_map_s_ = *local_map;
        }
        // Request next object.
        obj_id++;
    }

    bool apply_snapshot(snapshot& s) {
        //将快照中的内容进行移植当本地
        std::lock_guard<std::mutex> ll(snapshots_lock_);
        auto entry = snapshots_.find(s.get_last_log_idx());
        if (entry == snapshots_.end()) return false;

        ptr<snapshot_ctx> ctx = entry->second;
        //kv_map_ = ctx->kv_map_s_;
        //将日志进行回放
        for(auto t:ctx->kv_map_s_){
            auto key = t.first;
            auto value = t.second;
            lsm.insert_key(key,value);
            std::cout<<"调试ing"<<std::endl;
        }
        return true;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) {
        // In this example, `read_logical_snp_obj` doesn't create
        // `user_snp_ctx`. Nothing to do in this function.
    }

    ptr<snapshot> last_snapshot() {
        // Just return the latest snapshot.
        std::lock_guard<std::mutex> ll(snapshots_lock_);
        auto entry = snapshots_.rbegin();
        if (entry == snapshots_.rend()) return nullptr;

        ptr<snapshot_ctx> ctx = entry->second;
        return ctx->snapshot_;
    }

    ulong last_commit_index() {
        return last_committed_idx_;
    }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done)
    {
        if (!async_snapshot_) {
            // Create a snapshot in a synchronous way (blocking the thread).
            create_snapshot_sync(s, when_done);
        } else {
            // Create a snapshot in an asynchronous way (in a different thread).
            create_snapshot_async(s, when_done);
        }
    }

    int64_t get_current_value() const { return cur_value_; }
    std::unordered_map<int,int> get_current_map() {
        std::lock_guard<std::mutex> ll(snapshots_lock_);
        std::unordered_map<int,int> temp;
        auto entry = snapshots_.rbegin();
        if (entry == snapshots_.rend()) return temp;
        ptr<snapshot_ctx> ctx = entry->second;
        return ctx->kv_map_s_;
    }

private:
    struct snapshot_ctx {
        snapshot_ctx( ptr<snapshot>& s, std::unordered_map<int,int> kv_map )
            : snapshot_(s), kv_map_s_(kv_map) {}
        ptr<snapshot> snapshot_;
        //在kv键值对中，快照存储每一张map表
        std::unordered_map<int,int> kv_map_s_;
        //int64_t value_;
    };

    void create_snapshot_internal(ptr<snapshot> ss) {
        std::lock_guard<std::mutex> ll(snapshots_lock_);

        // Put into snapshot map.
        //auto temp = lsm.get_log();
        //auto kv_snapshot = lsm.get_log();
        ptr<snapshot_ctx> ctx = cs_new<snapshot_ctx>(ss, lsm.get_log());//日志内容的各式需要修改
        //kv_map_ = lsm.get_log();
        snapshots_[ss->get_last_log_idx()] = ctx;

        // Maintain last 3 snapshots only.
        const int MAX_SNAPSHOTS = 3;
        int num = snapshots_.size();
        auto entry = snapshots_.begin();
        for (int ii = 0; ii < num - MAX_SNAPSHOTS; ++ii) {
            if (entry == snapshots_.end()) break;
            entry = snapshots_.erase(entry);
        }
    }

    void create_snapshot_sync(snapshot& s,
                              async_result<bool>::handler_type& when_done)
    {
        // Clone snapshot from `s`.
        ptr<buffer> snp_buf = s.serialize();
        ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
        create_snapshot_internal(ss);

        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);

        std::cout << "snapshot (" << ss->get_last_log_term() << ", "
                  << ss->get_last_log_idx() << ") has been created synchronously"
                  << std::endl;
    }

    void create_snapshot_async(snapshot& s,
                               async_result<bool>::handler_type& when_done)
    {
        // Clone snapshot from `s`.
        ptr<buffer> snp_buf = s.serialize();
        ptr<snapshot> ss = snapshot::deserialize(*snp_buf);

        // Note that this is a very naive and inefficient example
        // that creates a new thread for each snapshot creation.
        std::thread t_hdl([this, ss, when_done]{
            create_snapshot_internal(ss);

            ptr<std::exception> except(nullptr);
            bool ret = true;
            when_done(ret, except);

            std::cout << "snapshot (" << ss->get_last_log_term() << ", "
                      << ss->get_last_log_idx() << ") has been created asynchronously"
                      << std::endl;
        });
        t_hdl.detach();
    }

    // State machine's current value.
    std::atomic<int64_t> cur_value_;

    //std::unordered_map<int,int> kv_map_;//暂时用于kv键值对
    //初始化每个节点下的LSM
    LSM<int,int> lsm=LSM<int,int>(10,3,0.5,1024,20);
    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx_;

    // Keeps the last 3 snapshots, by their Raft log numbers.
    std::map< uint64_t, ptr<snapshot_ctx> > snapshots_;

    // Mutex for `snapshots_`.
    std::mutex snapshots_lock_;

    // If `true`, snapshot will be created asynchronously.
    bool async_snapshot_;
};

}; // namespace kv_server

