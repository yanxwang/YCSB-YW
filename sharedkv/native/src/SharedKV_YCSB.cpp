#include "shared_kv.h"
#include "ycsb_wrapper.h"
#include <map>
#include <string>
#include <sstream>
#include <numa.h>
#include <sys/mman.h>

// Implementation of SharedKV_YCSB methods

// Constructor for PMem/file mode
SharedKV_YCSB::SharedKV_YCSB(const char* dev_path) : is_cxl_mode(false), numa_node(-1) {
    base = map_shared_memory(dev_path, SHM_SIZE);
    table = (SharedHashTable*)base;

    // Initialize table if magic not set
    if (table->magic != MAGIC_INIT) {
        table->magic = MAGIC_INIT;
        table->free_offset.store(sizeof(SharedHashTable));
        table->reserved = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            table->buckets[i].head_offset = 0;
            table->buckets[i].lock.flag.clear(std::memory_order_relaxed);
        }
        __sync_synchronize();
    }
}

// Constructor for CXL/NUMA mode
SharedKV_YCSB::SharedKV_YCSB(int node) : is_cxl_mode(true), numa_node(node) {
    base = allocate_cxl_memory(node, SHM_SIZE);
    table = (SharedHashTable*)base;

    // Initialize table (always fresh in CXL mode since it's volatile DRAM)
    table->magic = MAGIC_INIT;
    table->free_offset.store(sizeof(SharedHashTable));
    table->reserved = 0;
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        table->buckets[i].head_offset = 0;
        table->buckets[i].lock.flag.clear(std::memory_order_relaxed);
    }
    __sync_synchronize();
}

// Destructor
SharedKV_YCSB::~SharedKV_YCSB() {
    if (is_cxl_mode && base != nullptr) {
        munmap(base, SHM_SIZE);
    }
    // For PMem mode, munmap would be done here if needed
}

// Helper: Serialize map to string
std::string serialize_values(const std::map<std::string, std::string>& values) {
    std::ostringstream oss;
    for (const auto& pair : values) {
        oss << pair.first << "=" << pair.second << ";";
    }
    return oss.str();
}

// Helper: Deserialize string to map
std::map<std::string, std::string> deserialize_values(const std::string& str) {
    std::map<std::string, std::string> result;
    std::istringstream iss(str);
    std::string token;
    
    while (std::getline(iss, token, ';')) {
        if (token.empty()) continue;
        size_t pos = token.find('=');
        if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            result[key] = value;
        }
    }
    return result;
}

int SharedKV_YCSB::read(const std::string &table_name,
                        const std::string &key,
                        std::map<std::string,std::string> &result) {
    std::string val;
    if (kv_get(table, base, key, val)) {
        result = deserialize_values(val);
        return 0; // success
    }
    return 1; // not found
}

int SharedKV_YCSB::insert(const std::string &table_name,
                          const std::string &key,
                          const std::map<std::string,std::string> &values) {
    if (values.empty()) {
        return 1; // error: no values to insert
    }
    
    // Serialize all fields into a single value
    std::string serialized = serialize_values(values);
    kv_put(table, base, key, serialized);
    return 0; // success
}

int SharedKV_YCSB::update(const std::string &table_name,
                          const std::string &key,
                          const std::map<std::string,std::string> &values) {
    return insert(table_name, key, values); // overwrite
}

int SharedKV_YCSB::delete_op(const std::string &table_name,
                             const std::string &key) {
    return kv_delete(table, base, key) ? 0 : 1;
}