#pragma once
#include "shared_kv.h"
#include <map>
#include <string>
#include <vector>

// Abstract DB interface (simplified YCSB-like)
class DB {
public:
    virtual int read(const std::string& table, const std::string& key,
                     std::map<std::string, std::string>& result) = 0;

    virtual int insert(const std::string& table, const std::string& key,
                       const std::map<std::string, std::string>& values) = 0;

    virtual int update(const std::string& table, const std::string& key,
                       const std::map<std::string, std::string>& values) = 0;

    virtual int delete_op(const std::string& table, const std::string& key) = 0;

    virtual ~DB() {}
};

// Forward declaration only - implementation in SharedKV_YCSB.cpp
class SharedKV_YCSB : public DB {
private:
    void* base;
    SharedHashTable* table;
    bool is_cxl_mode;
    int numa_node;

public:
    // Default constructor: CXL mode on node 2
    SharedKV_YCSB(int numa_node = 2);

    // Legacy constructor for PMem/device mode
    SharedKV_YCSB(const char* path);

    virtual ~SharedKV_YCSB();

    int read(const std::string& table_name, const std::string& key,
             std::map<std::string, std::string>& result) override;

    int insert(const std::string& table_name, const std::string& key,
               const std::map<std::string, std::string>& values) override;

    int update(const std::string& table_name, const std::string& key,
               const std::map<std::string, std::string>& values) override;

    int delete_op(const std::string& table_name, const std::string& key) override;
};