#include "kv_request.h"
#include "uintr_threading.h"
#include <cstring>
#include <algorithm>

// ============================================================================
// KVRequest implementation
// Request and Response message structures for key-value operations
// ============================================================================

void KVRequest::set_key(const std::string& key) {
    key_len = std::min(key.size(), (size_t)(sizeof(key_data) - 1));
    memcpy(key_data, key.data(), key_len);
    key_data[key_len] = '\0';
}

void KVRequest::set_value(const std::string& value) {
    value_len = value.size();
    if (value_len > 0) {
        value_data = new char[value_len + 1];
        memcpy(value_data, value.data(), value_len);
        value_data[value_len] = '\0';
    } else {
        value_data = nullptr;
    }
}

std::string KVRequest::get_key() const {
    return std::string(key_data, key_len);
}

std::string KVRequest::get_value() const {
    if (value_data && value_len > 0) {
        return std::string(value_data, value_len);
    }
    return std::string();
}

void KVRequest::cleanup() {
    if (value_data) {
        delete[] value_data;
        value_data = nullptr;
    }
    value_len = 0;
}

// ============================================================================
// KVResponse implementation
// ============================================================================

void KVResponse::set_result(const std::string& result) {
    result_len = std::min(result.size(), (size_t)(sizeof(result_data) - 1));
    memcpy(result_data, result.data(), result_len);
    result_data[result_len] = '\0';
}

std::string KVResponse::get_result() const {
    return std::string(result_data, result_len);
}
