/**
 * Copyright (c) 2025 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.sharedkv;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * SharedKV client for YCSB.
 */
public class SharedKVClient extends DB {

  private long nativeHandle;
  private static volatile boolean contextInitialized = false;
  private static volatile long sharedContextHandle = 0;  // Remember the shared context
  private static final Object INIT_LOCK = new Object();

  static {
    System.loadLibrary("sharedkv_jni");
  }

  // Single-threaded mode native methods (legacy)
  private native long nativeInit(String devicePath);

  private native long nativeInitCXL(int numaNode);

  private native void nativeDestroy(long handle);

  private native int nativeRead(long handle, String key, Map<String, String> result);

  private native int nativeInsert(long handle, String key, Map<String, String> values);

  private native int nativeUpdate(long handle, String key, Map<String, String> values);

  private native int nativeDelete(long handle, String key);

  // Multi-threaded mode native methods (UINTR-based)
  private native long nativeInitThreaded(int numaNode, int numClients, int numWorkers);

  private native void nativeDestroyThreaded(long handle);

  private native int nativeReadThreaded(long handle, String key, Map<String, String> result);

  private native int nativeInsertThreaded(long handle, String key, Map<String, String> values);

  private native int nativeUpdateThreaded(long handle, String key, Map<String, String> values);

  private native int nativeDeleteThreaded(long handle, String key);

  @Override
  public void init() throws DBException {
    try {
      // Check threading mode
      String threading = getProperties().getProperty("sharedkv.threading", "single");
      String mode = getProperties().getProperty("sharedkv.mode", "cxl");

      if ("multi".equalsIgnoreCase(threading)) {
        // Multi-threaded UINTR-based mode
        synchronized (INIT_LOCK) {
          if (!contextInitialized) {
            // First thread initializes the shared context
            int numaNode = Integer.parseInt(getProperties().getProperty("sharedkv.numa_node", "2"));
            int numClients = Integer.parseInt(getProperties().getProperty("sharedkv.num_clients", "16"));
            int numWorkers = Integer.parseInt(getProperties().getProperty("sharedkv.num_workers", "8"));

            System.err.println("DEBUG: Initializing SharedKV in multi-threaded mode");
            System.err.println("DEBUG: NUMA node=" + numaNode + ", clients=" + numClients + ", workers=" + numWorkers);

            sharedContextHandle = nativeInitThreaded(numaNode, numClients, numWorkers);
            nativeHandle = sharedContextHandle;
            contextInitialized = true;
          } else {
            // Subsequent threads reuse the existing context
            System.err.println("DEBUG: Reusing existing SharedKV context (handle=" + sharedContextHandle + ")");
            nativeHandle = sharedContextHandle;
          }
        }
      } else {
        // Single-threaded mode (legacy)
        if ("pmem".equalsIgnoreCase(mode)) {
          // Legacy PMem mode (for backward compatibility)
          String devicePath = getProperties().getProperty("sharedkv.device", "/dev/pmem0");
          System.err.println("DEBUG: Initializing SharedKV in PMem mode with device: " + devicePath);
          nativeHandle = nativeInit(devicePath);
        } else {
          // Default CXL mode: use NUMA node allocation
          String numaNodeStr = getProperties().getProperty("sharedkv.numa_node", "2");
          int numaNode = Integer.parseInt(numaNodeStr);
          System.err.println("DEBUG: Initializing SharedKV in single-threaded CXL mode on NUMA node: " + numaNode);
          nativeHandle = nativeInitCXL(numaNode);
        }
      }

      System.err.println("DEBUG: Native handle: " + nativeHandle);
      if (nativeHandle == 0) {
        throw new DBException("Failed to initialize SharedKV - nativeInit returned 0");
      }
      System.err.println("DEBUG: SharedKV initialized successfully");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("ERROR: JNI library error: " + e.getMessage());
      e.printStackTrace();
      throw new DBException("JNI error: " + e.getMessage());
    } catch (Exception e) {
      System.err.println("ERROR during init: " + e.getMessage());
      e.printStackTrace();
      throw new DBException("Initialization failed: " + e.getMessage());
    }
  }

  @Override
  public void cleanup() throws DBException {
    // In multi-threaded mode, we don't destroy the shared context
    // It persists across load/run phases and is only destroyed at JVM shutdown
    String threading = getProperties().getProperty("sharedkv.threading", "single");
    if ("multi".equalsIgnoreCase(threading)) {
      // Just clear the per-thread handle, but keep the shared context alive
      nativeHandle = 0;
      System.err.println("DEBUG: Cleanup called (multi-threaded mode, context preserved)");
    } else {
      // Single-threaded mode: destroy the per-thread instance
      if (nativeHandle != 0) {
        nativeDestroy(nativeHandle);
        nativeHandle = 0;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Map<String, String> stringResult = new HashMap<>();

    String threading = getProperties().getProperty("sharedkv.threading", "single");
    int ret;
    if ("multi".equalsIgnoreCase(threading)) {
      ret = nativeReadThreaded(nativeHandle, key, stringResult);
    } else {
      ret = nativeRead(nativeHandle, key, stringResult);
    }

    if (ret == 0) {
      for (Map.Entry<String, String> entry : stringResult.entrySet()) {
        result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
      return Status.OK;
    }
    return Status.NOT_FOUND;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      System.err.println("DEBUG: Insert called - handle=" + nativeHandle + " key=" + key);
      Map<String, String> stringValues = new HashMap<>();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        stringValues.put(entry.getKey(), entry.getValue().toString());
      }
      System.err.println("DEBUG: Calling nativeInsert with " + stringValues.size() + " values");

      String threading = getProperties().getProperty("sharedkv.threading", "single");
      int ret;
      if ("multi".equalsIgnoreCase(threading)) {
        ret = nativeInsertThreaded(nativeHandle, key, stringValues);
      } else {
        ret = nativeInsert(nativeHandle, key, stringValues);
      }

      System.err.println("DEBUG: nativeInsert returned: " + ret);
      return ret == 0 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      System.err.println("ERROR during insert: " + e.getMessage());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> stringValues = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      stringValues.put(entry.getKey(), entry.getValue().toString());
    }

    String threading = getProperties().getProperty("sharedkv.threading", "single");
    int ret;
    if ("multi".equalsIgnoreCase(threading)) {
      ret = nativeUpdateThreaded(nativeHandle, key, stringValues);
    } else {
      ret = nativeUpdate(nativeHandle, key, stringValues);
    }

    return ret == 0 ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String threading = getProperties().getProperty("sharedkv.threading", "single");
    int ret;
    if ("multi".equalsIgnoreCase(threading)) {
      ret = nativeDeleteThreaded(nativeHandle, key);
    } else {
      ret = nativeDelete(nativeHandle, key);
    }
    return ret == 0 ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}