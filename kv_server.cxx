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
//g++ -o kv kv_server.cxx in_memory_log_store.cxx logger.cc -lnuraft -lssl -lpthread
#include "raft/kv_state_machine.hxx"
#include "raft/in_memory_state_mgr.hxx"
#include "raft/logger_wrapper.hxx"

#include "libnuraft/nuraft.hxx"

#include "test_common.h"

#include <iostream>
#include <sstream>

#include <stdio.h>

using namespace nuraft;

namespace kv_server {

static raft_params::return_method_type CALL_TYPE
    = raft_params::blocking;
//  = raft_params::async_handler;

static bool ASYNC_SNAPSHOT_CREATION = false;

#include "raft/example_common.hxx"

kv_state_machine* get_sm() {
    return static_cast<kv_state_machine*>( stuff.sm_.get() );
}

void handle_result(ptr<TestSuite::Timer> timer,
                   raft_result& result,
                   ptr<std::exception>& err)
{
    if (result.get_result_code() != cmd_result_code::OK) {
        // Something went wrong.
        // This means committing this log failed,
        // but the log itself is still in the log store.
        std::cout << "failed: " << result.get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    ptr<buffer> buf = result.get();
    uint64_t ret_value = buf->get_ulong();
    std::cout << "succeeded, "
              << TestSuite::usToString( timer->getTimeUs() )
              << ", return value: "
              << ret_value
              << ", state machine value: "
              << get_sm()->get_current_value()
              << std::endl;
}

void append_log(const std::string& cmd,
                const std::vector<std::string>& tokens)
{
    char cmd_char = cmd[0];
    int operand = atoi( tokens[0].substr(1).c_str() );
    kv_state_machine::op_type op = kv_state_machine::SET;
    int key = 0,value = 0;
    if(cmd == "put") {
        op = kv_state_machine::op_type::PUT;
        key = atoi(tokens[1].c_str());
        value = atoi(tokens[2].c_str());
    }
    else if(cmd == "rm") {
        op = kv_state_machine::op_type::RM;
        key = atoi(tokens[1].c_str());
    }
    ptr<buffer> new_log = kv_state_machine::enc_log( {op, key,value});//通过状态机，构建一个新的日志

    // To measure the elapsed time.
    ptr<TestSuite::Timer> timer = cs_new<TestSuite::Timer>();

    // Do append.（都没有涉及到计算）
    
    ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );//调用的一个raft实例添加日志

    if (!ret->get_accepted()) {
        // Log append rejected, usually because this node is not a leader.
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    // Log append accepted, but that doesn't mean the log is committed.//log会先写入本地，但并不意味着被提交
    // Commit result can be obtained below.

    if (CALL_TYPE == raft_params::blocking) {
        // Blocking mode:
        //   `append_entries` returns after getting a consensus,
        //   so that `ret` already has the result from state machine.
        ptr<std::exception> err(nullptr);
        handle_result(timer, *ret, err);

    } else if (CALL_TYPE == raft_params::async_handler) {
        // Async mode:
        //   `append_entries` returns immediately.
        //   `handle_result` will be invoked asynchronously,
        //   after getting a consensus.
        ret->when_ready( std::bind( handle_result,
                                    timer,
                                    std::placeholders::_1,
                                    std::placeholders::_2 ) );

    } else {
        assert(0);
    }
}

void print_lsm(const std::string& cmd,
                const std::vector<std::string>& tokens){
    if(tokens[1] == "s") get_sm()->printLSM();
    else if(tokens[1]=="g") get_sm()->lookupLSM(atoi(tokens[2].c_str()));
    else if(tokens[1]=="r") get_sm()->rangeLSM(atoi(tokens[2].c_str()),atoi(tokens[3].c_str()));
    }

void print_status(const std::string& cmd,
                  const std::vector<std::string>& tokens)
{
    ptr<log_store> ls = stuff.smgr_->load_log_store();

    std::cout
        << "my server id: " << stuff.server_id_ << std::endl
        << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
        << "Raft log range: ";
    if (ls->start_index() >= ls->next_slot()) {
        // Start index can be the same as next slot when the log store is empty.
        std::cout << "(empty)" << std::endl;
    } else {
        std::cout << ls->start_index()
                  << " - " << (ls->next_slot() - 1) << std::endl;
    }
    std::cout
        << "last committed index: "
            << stuff.raft_instance_->get_committed_log_idx() << std::endl
        << "current term: "
            << stuff.raft_instance_->get_term() << std::endl
        << "last snapshot log index: "
            << (stuff.sm_->last_snapshot()
                ? stuff.sm_->last_snapshot()->get_last_log_idx() : 0) << std::endl
        << "last snapshot log term: "
            << (stuff.sm_->last_snapshot()
                ? stuff.sm_->last_snapshot()->get_last_log_term() : 0) << std::endl;
    
    std::cout<< "KV state machine key_value: "<<std::endl;
    auto kv_map = static_cast<std::unordered_map<int,int>>(get_sm()->get_current_map());
    std::cout<<"key "<<"value"<<std::endl;
    for(auto &t:kv_map) {std::cout<<t.first<<"-"<<t.second<<std::endl;}
}

void help(const std::string& cmd,
          const std::vector<std::string>& tokens)
{
    std::cout
    << "modify value: <+|-|*|/><operand>\n"
    << "    +: add <operand> to state machine's value.\n"
    << "    -: subtract <operand> from state machine's value.\n"
    << "    *: multiple state machine'value by <operand>.\n"
    << "    /: divide state machine's value by <operand>.\n"
    << "    e.g.) +123\n"
    << "\n"
    << "add server: add <server id> <address>:<port>\n"
    << "    e.g.) add 2 127.0.0.1:20000\n"
    << "\n"
    << "get current server status: st (or stat)\n"
    << "\n"
    << "get the list of members: ls (or list)\n"
    << "\n";
}

bool do_cmd(const std::vector<std::string>& tokens) {
    if (!tokens.size()) return true;

    const std::string& cmd = tokens[0];

    if (cmd == "q" || cmd == "exit") {
        stuff.launcher_.shutdown(5);
        stuff.reset();
        return false;

    } else if ( cmd == "put"||cmd == "rm") {//先处理添加的过程
        // e.g.) put 1 1
        append_log(cmd, tokens);//如果使用kv形式，那就是从这里添加日志

    } else if(cmd == "lsm"){//后处理删除的过程
        //e.g.) rm 1
        print_lsm(cmd,tokens);
    } else if ( cmd == "add" ) {
        // e.g.) add 2 localhost:12345
        add_server(cmd, tokens);

    } else if ( cmd == "st" || cmd == "stat" ) {
        print_status(cmd, tokens);

    } else if ( cmd == "ls" || cmd == "list" ) {
        server_list(cmd, tokens);

    } else if ( cmd == "h" || cmd == "help" ) {
        help(cmd, tokens);
    }
    return true;
}

void check_additional_flags(int argc, char** argv) {
    for (int ii = 1; ii < argc; ++ii) {
        if (strcmp(argv[ii], "--async-handler") == 0) {
            CALL_TYPE = raft_params::async_handler;
        } else if (strcmp(argv[ii], "--async-snapshot-creation") == 0) {
            ASYNC_SNAPSHOT_CREATION = true;
        }
    }
}

void kv_usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " <server id> <IP address and port> [<options>]";
    ss << std::endl << std::endl;
    ss << "    options:" << std::endl;
    ss << "      --async-handler: use async type handler." << std::endl;
    ss << "      --async-snapshot-creation: create snapshots asynchronously."
       << std::endl << std::endl;

    std::cout << ss.str();
    exit(0);
    // char* local[];
    // local = "1 localhost:10001"
    // return argv
}

}; // namespace kv_server;
using namespace kv_server;

int main(int argc, char** argv) {
    if (argc < 3) {
        //kv_usage(argc, argv);
        argc = 3;
        argv[1] = "2";
        argv[2] ="localhost:10002";
    }
    set_server_info(argc, argv);
    check_additional_flags(argc, argv);

    std::cout << "    -- Replicated kvulator with Raft --" << std::endl;
    std::cout << "                         Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    if (CALL_TYPE == raft_params::async_handler) {
        std::cout << "    async handler is enabled" << std::endl;
    }
    if (ASYNC_SNAPSHOT_CREATION) {
        std::cout << "    snapshots are created asynchronously" << std::endl;
    }
    init_raft( cs_new<kv_state_machine>(ASYNC_SNAPSHOT_CREATION) );
    loop();

    return 0;
}

