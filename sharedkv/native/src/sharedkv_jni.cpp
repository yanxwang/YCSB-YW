#include <jni.h>
#include <string>
#include <map>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include "shared_kv.h"
#include "ycsb_wrapper.h"
#include "kv_request.h"

// ============================================================================
// Helper functions (shared by both modes)
// ============================================================================

// Helper to convert jstring to std::string
std::string jstring_to_string(JNIEnv* env, jstring jstr) {
    if (!jstr) return "";
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    std::string str(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return str;
}

// Helper to convert Java Map to C++ map
std::map<std::string, std::string> jmap_to_map(JNIEnv* env, jobject jmap) {
    std::map<std::string, std::string> result;

    jclass mapClass = env->FindClass("java/util/Map");
    jmethodID entrySet = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    jobject set = env->CallObjectMethod(jmap, entrySet);

    jclass setClass = env->FindClass("java/util/Set");
    jmethodID iterator = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    jobject iter = env->CallObjectMethod(set, iterator);

    jclass iteratorClass = env->FindClass("java/util/Iterator");
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");

    jclass entryClass = env->FindClass("java/util/Map$Entry");
    jmethodID getKey = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    jmethodID getValue = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

    while (env->CallBooleanMethod(iter, hasNext)) {
        jobject entry = env->CallObjectMethod(iter, next);
        jstring key = (jstring)env->CallObjectMethod(entry, getKey);
        jstring value = (jstring)env->CallObjectMethod(entry, getValue);

        result[jstring_to_string(env, key)] = jstring_to_string(env, value);

        env->DeleteLocalRef(entry);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
    }

    return result;
}

// Helper to put entries into Java Map
void put_to_jmap(JNIEnv* env, jobject jmap, const std::string& key, const std::string& value) {
    jclass mapClass = env->FindClass("java/util/Map");
    jmethodID put = env->GetMethodID(mapClass, "put",
                                     "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jstring jkey = env->NewStringUTF(key.c_str());
    jstring jvalue = env->NewStringUTF(value.c_str());

    env->CallObjectMethod(jmap, put, jkey, jvalue);

    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
}

// ============================================================================
// Multi-threaded mode: Thread-local client ID management
// ============================================================================

static thread_local uint32_t tls_client_id = UINT32_MAX;
static std::atomic<uint32_t> next_client_id{0};
static std::unordered_map<std::thread::id, uint32_t> thread_to_client_map;
static std::mutex client_map_mutex;

uint32_t get_client_id(SharedKVContext* ctx) {
    // Fast path: Check thread-local storage
    if (tls_client_id != UINT32_MAX) {
        return tls_client_id;
    }

    // Slow path: Assign new client ID
    std::lock_guard<std::mutex> lock(client_map_mutex);
    auto tid = std::this_thread::get_id();
    auto it = thread_to_client_map.find(tid);

    if (it != thread_to_client_map.end()) {
        tls_client_id = it->second;
        return tls_client_id;
    }

    // Assign new client ID (each thread gets unique ID)
    uint32_t client_id = next_client_id.fetch_add(1, std::memory_order_relaxed);

    // Check if we exceeded num_clients
    if (client_id >= ctx->num_clients) {
        fprintf(stderr, "[JNI] ERROR: Thread count (%u) exceeds num_clients (%u). "
                "Increase sharedkv.num_clients property!\n",
                client_id + 1, ctx->num_clients);
        // Fallback: wrap around (will cause response mismatch issues!)
        client_id = client_id % ctx->num_clients;
        fprintf(stderr, "[JNI] WARNING: Using shared client_id=%u (responses may mismatch)\n",
                client_id);
    }

    thread_to_client_map[tid] = client_id;
    tls_client_id = client_id;

    fprintf(stderr, "[JNI] Thread %lu assigned client_id=%u\n",
            std::hash<std::thread::id>{}(tid), client_id);

    return client_id;
}

// ============================================================================
// JNI exports
// ============================================================================

extern "C" {

// ----------------------------------------------------------------------------
// Single-threaded mode (legacy, for backward compatibility)
// ----------------------------------------------------------------------------

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInit(JNIEnv* env, jobject obj, jstring devicePath) {
    std::string path = jstring_to_string(env, devicePath);
    try {
        SharedKV_YCSB* db = new SharedKV_YCSB(path.c_str());
        return reinterpret_cast<jlong>(db);
    } catch (const std::exception& e) {
        fprintf(stderr, "[JNI] nativeInit failed: %s\\n", e.what());
        return 0;
    }
}

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInitCXL(JNIEnv* env, jobject obj, jint numaNode) {
    try {
        SharedKV_YCSB* db = new SharedKV_YCSB(static_cast<int>(numaNode));
        return reinterpret_cast<jlong>(db);
    } catch (const std::exception& e) {
        fprintf(stderr, "[JNI] nativeInitCXL failed: %s\\n", e.what());
        return 0;
    }
}

JNIEXPORT void JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeDestroy(JNIEnv* env, jobject obj, jlong handle) {
    SharedKV_YCSB* db = reinterpret_cast<SharedKV_YCSB*>(handle);
    delete db;
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeRead(JNIEnv* env, jobject obj,
                                                       jlong handle, jstring key, jobject result) {
    SharedKV_YCSB* db = reinterpret_cast<SharedKV_YCSB*>(handle);
    std::string k = jstring_to_string(env, key);
    std::map<std::string, std::string> res;

    int ret = db->read("", k, res);

    if (ret == 0) {
        for (const auto& pair : res) {
            put_to_jmap(env, result, pair.first, pair.second);
        }
    }

    return ret;
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInsert(JNIEnv* env, jobject obj,
                                                         jlong handle, jstring key, jobject values) {
    SharedKV_YCSB* db = reinterpret_cast<SharedKV_YCSB*>(handle);
    std::string k = jstring_to_string(env, key);
    std::map<std::string, std::string> vals = jmap_to_map(env, values);

    return db->insert("", k, vals);
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeUpdate(JNIEnv* env, jobject obj,
                                                         jlong handle, jstring key, jobject values) {
    SharedKV_YCSB* db = reinterpret_cast<SharedKV_YCSB*>(handle);
    std::string k = jstring_to_string(env, key);
    std::map<std::string, std::string> vals = jmap_to_map(env, values);

    return db->update("", k, vals);
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeDelete(JNIEnv* env, jobject obj,
                                                         jlong handle, jstring key) {
    SharedKV_YCSB* db = reinterpret_cast<SharedKV_YCSB*>(handle);
    std::string k = jstring_to_string(env, key);

    return db->delete_op("", k);
}

// ----------------------------------------------------------------------------
// Multi-threaded mode (UINTR-based architecture)
// ----------------------------------------------------------------------------

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInitThreaded(
    JNIEnv* env, jobject obj, jint numaNode, jint numClients, jint numWorkers,
    jint queueDepth, jint ringBufferSize) {

    fprintf(stderr, "[JNI] nativeInitThreaded: numa_node=%d, num_clients=%d, num_workers=%d, "
            "queue_depth=%d, ring_buffer=%d\\n",
            numaNode, numClients, numWorkers, queueDepth, ringBufferSize);

    try {
        SharedKVContext* ctx = get_or_create_context(
            static_cast<uint32_t>(numClients),
            static_cast<uint32_t>(numWorkers),
            static_cast<int>(numaNode),
            static_cast<size_t>(queueDepth),
            static_cast<size_t>(ringBufferSize)
        );

        fprintf(stderr, "[JNI] SharedKVContext created at %p\\n", ctx);
        return reinterpret_cast<jlong>(ctx);
    } catch (const std::exception& e) {
        fprintf(stderr, "[JNI] nativeInitThreaded failed: %s\\n", e.what());
        return 0;
    }
}

JNIEXPORT void JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeDestroyThreaded(JNIEnv* env, jobject obj, jlong handle) {
    fprintf(stderr, "[JNI] nativeDestroyThreaded called (handle=%ld)\\n", handle);
    // Note: We don't destroy the singleton context here since multiple Java threads may be using it
    // The context will be destroyed when the JVM shuts down or destroy_context() is explicitly called
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeReadThreaded(
    JNIEnv* env, jobject obj, jlong handle, jstring key, jobject result) {

    SharedKVContext* ctx = reinterpret_cast<SharedKVContext*>(handle);
    if (!ctx) {
        fprintf(stderr, "[JNI] nativeReadThreaded: invalid handle\\n");
        return 1;
    }

    uint32_t client_id = get_client_id(ctx);
    std::string k = jstring_to_string(env, key);

    // Create request
    KVRequest* req = new KVRequest();
    req->op_type = KVOpType::READ;
    req->set_key(k);
    req->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

    // Submit request (blocking with 5000ms timeout)
    KVResponse resp = ctx->submit_request(client_id, req, 5000);

    // Process response
    if (resp.status == KVStatus::SUCCESS) {
        std::string result_str = resp.get_result();

        // Parse result as "field1=value1,field2=value2,..."
        // For now, assume simple key-value format (YCSB typically stores multiple fields)
        // We'll store the entire result as a single field for simplicity
        put_to_jmap(env, result, "value", result_str);
        return 0;
    } else if (resp.status == KVStatus::NOT_FOUND) {
        return 1;
    } else {
        fprintf(stderr, "[JNI] nativeReadThreaded: error status\\n");
        return 1;
    }
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInsertThreaded(
    JNIEnv* env, jobject obj, jlong handle, jstring key, jobject values) {

    SharedKVContext* ctx = reinterpret_cast<SharedKVContext*>(handle);
    if (!ctx) {
        fprintf(stderr, "[JNI] nativeInsertThreaded: invalid handle\\n");
        return 1;
    }

    uint32_t client_id = get_client_id(ctx);
    std::string k = jstring_to_string(env, key);
    std::map<std::string, std::string> vals = jmap_to_map(env, values);

    // Serialize values as "field1=value1,field2=value2,..."
    std::string serialized_value;
    for (const auto& pair : vals) {
        if (!serialized_value.empty()) serialized_value += ",";
        serialized_value += pair.first + "=" + pair.second;
    }

    // Create request
    KVRequest* req = new KVRequest();
    req->op_type = KVOpType::INSERT;
    req->set_key(k);
    req->set_value(serialized_value);
    req->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

    // Submit request (blocking with 5000ms timeout)
    KVResponse resp = ctx->submit_request(client_id, req, 5000);

    if (resp.status == KVStatus::SUCCESS) {
        return 0;
    } else {
        fprintf(stderr, "[JNI] nativeInsertThreaded: error status\\n");
        return 1;
    }
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeUpdateThreaded(
    JNIEnv* env, jobject obj, jlong handle, jstring key, jobject values) {

    SharedKVContext* ctx = reinterpret_cast<SharedKVContext*>(handle);
    if (!ctx) {
        fprintf(stderr, "[JNI] nativeUpdateThreaded: invalid handle\\n");
        return 1;
    }

    uint32_t client_id = get_client_id(ctx);
    std::string k = jstring_to_string(env, key);
    std::map<std::string, std::string> vals = jmap_to_map(env, values);

    // Serialize values as "field1=value1,field2=value2,..."
    std::string serialized_value;
    for (const auto& pair : vals) {
        if (!serialized_value.empty()) serialized_value += ",";
        serialized_value += pair.first + "=" + pair.second;
    }

    // Create request
    KVRequest* req = new KVRequest();
    req->op_type = KVOpType::UPDATE;
    req->set_key(k);
    req->set_value(serialized_value);
    req->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

    // Submit request (blocking with 5000ms timeout)
    KVResponse resp = ctx->submit_request(client_id, req, 5000);

    if (resp.status == KVStatus::SUCCESS) {
        return 0;
    } else {
        fprintf(stderr, "[JNI] nativeUpdateThreaded: error status\\n");
        return 1;
    }
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeDeleteThreaded(
    JNIEnv* env, jobject obj, jlong handle, jstring key) {

    SharedKVContext* ctx = reinterpret_cast<SharedKVContext*>(handle);
    if (!ctx) {
        fprintf(stderr, "[JNI] nativeDeleteThreaded: invalid handle\\n");
        return 1;
    }

    uint32_t client_id = get_client_id(ctx);
    std::string k = jstring_to_string(env, key);

    // Create request
    KVRequest* req = new KVRequest();
    req->op_type = KVOpType::DELETE;
    req->set_key(k);
    req->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

    // Submit request (blocking with 5000ms timeout)
    KVResponse resp = ctx->submit_request(client_id, req, 5000);

    if (resp.status == KVStatus::SUCCESS) {
        return 0;
    } else if (resp.status == KVStatus::NOT_FOUND) {
        return 1;
    } else {
        fprintf(stderr, "[JNI] nativeDeleteThreaded: error status\\n");
        return 1;
    }
}

} // extern "C"
