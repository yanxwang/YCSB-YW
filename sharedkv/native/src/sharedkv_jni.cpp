#include <jni.h>
#include <string>
#include <map>
#include "shared_kv.h"
#include "ycsb_wrapper.h"

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

extern "C" {

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInit(JNIEnv* env, jobject obj, jstring devicePath) {
    std::string path = jstring_to_string(env, devicePath);
    try {
        SharedKV_YCSB* db = new SharedKV_YCSB(path.c_str());
        return reinterpret_cast<jlong>(db);
    } catch (const std::exception& e) {
        // Return 0 to indicate failure
        return 0;
    }
}

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeInitCXL(JNIEnv* env, jobject obj, jint numaNode) {
    try {
        SharedKV_YCSB* db = new SharedKV_YCSB(static_cast<int>(numaNode));
        return reinterpret_cast<jlong>(db);
    } catch (const std::exception& e) {
        // Return 0 to indicate failure
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

} // extern "C"