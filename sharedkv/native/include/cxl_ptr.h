#pragma once

// ============================================================================
// CXL Offset-Based Smart Pointer
//
// On multi-host CXL, each host mmaps the same physical CXL memory at a
// different virtual address. Raw pointers are meaningless across hosts.
// CXLPtr<T> stores only an offset (8 bytes), resolving to an absolute
// address via:   Global_Addr = CXL_Base_Addr + Offset
//
// The CXL base address is stored in a thread_local variable. On x86-64
// Linux, thread_local access compiles to a single fs-segment load:
//   mov rax, fs:[tls_offset]
// The compiler will CSE (Common Subexpression Eliminate) this in hot loops,
// keeping the base in a register.
//
// Key properties:
//   - sizeof(CXLPtr<T>) == 8, trivially copyable
//   - Offset 0 == null (consistent with existing cxl_shared.h conventions)
//   - Can be stored directly in CXL shared memory (replaces uint64_t offset)
//   - Overloaded -> and * for transparent dereferencing
//   - Pointer arithmetic: ptr + n advances by n * sizeof(T)
//   - Debug bounds checking via CXL_PTR_DEBUG
// ============================================================================

#include <cstdint>
#include <cstddef>
#include <type_traits>

// ============================================================================
// CXLBase — Thread-Local Base Address Management
//
// Call CXLBase::set() once per thread after mmapping CXL memory.
// All CXLPtr<T>::get() calls resolve against this base.
// ============================================================================

class CXLBase {
    static inline thread_local char* base_ = nullptr;

#ifdef CXL_PTR_DEBUG
    static inline thread_local uint64_t size_ = 0;
#endif

public:
    static void set(void* base) {
        base_ = static_cast<char*>(base);
    }

#ifdef CXL_PTR_DEBUG
    static void set(void* base, uint64_t total_size) {
        base_ = static_cast<char*>(base);
        size_ = total_size;
    }

    static uint64_t size() { return size_; }
#endif

    // On x86-64 Linux: compiles to  mov rax, fs:[offset]
    // Compiler will hoist this out of loops via CSE.
    __attribute__((always_inline))
    static char* get() { return base_; }
};

// ============================================================================
// CXLPtr<T> — Offset-Based Pointer Template
// ============================================================================

template<typename T>
class CXLPtr {
    uint64_t off_;

public:
    // --- Construction ---

    // Default: null pointer (offset 0)
    CXLPtr() : off_(0) {}

    // From raw offset (e.g., from CXLMemoryRegion::allocate())
    explicit CXLPtr(uint64_t offset) : off_(offset) {}

    // From raw pointer (computes offset against current thread's base)
    static CXLPtr from_raw(const T* ptr) {
        if (!ptr) return CXLPtr();
        return CXLPtr(reinterpret_cast<const char*>(ptr) - CXLBase::get());
    }

    // --- Resolve: offset → absolute address ---

    __attribute__((always_inline))
    T* get() const {
#ifdef CXL_PTR_DEBUG
        if (off_ != 0 && off_ >= CXLBase::size()) {
            __builtin_trap();  // Offset out of bounds
        }
#endif
        // Compiles to:  lea rax, [base_reg + off_]
        // where base_reg is the CSE'd thread_local load
        return off_ ? reinterpret_cast<T*>(CXLBase::get() + off_) : nullptr;
    }

    __attribute__((always_inline))
    T& operator*() const {
        return *get();
    }

    __attribute__((always_inline))
    T* operator->() const {
        return get();
    }

    // --- Direct field access (folded addressing) ---
    //
    // operator-> returns T*, forcing the compiler to materialize base+offset
    // as an intermediate pointer. When accessing multiple fields in a loop,
    // this prevents the compiler from folding into [base + index + disp].
    //
    // read_field() bypasses T*, reading directly from base + offset + field_offset
    // in a single [base + index + disp] addressing mode instruction.
    //
    // Usage:
    //   ptr.read_field(&CXLKVEntry::value_len)   // movl disp(%base, %off), %reg
    //   ptr.read_field(&CXLKVEntry::next_offset)  // movq disp(%base, %off), %reg
    //
    // vs operator-> (two instructions):
    //   ptr->value_len    // addq %base, %off; movl disp(%off), %reg

    // read_field / write_field take a compile-time byte offset.
    // Use with offsetof():
    //   ptr.read_field<uint32_t>(offsetof(CXLKVEntry, value_len))
    //   ptr.read_field<uint64_t>(offsetof(CXLKVEntry, next_offset))

    template<typename F>
    __attribute__((always_inline))
    F read_field(size_t field_offset) const {
        return *reinterpret_cast<const F*>(CXLBase::get() + off_ + field_offset);
    }

    template<typename F>
    __attribute__((always_inline))
    void write_field(size_t field_offset, F value) const {
        *reinterpret_cast<F*>(CXLBase::get() + off_ + field_offset) = value;
    }

    // --- Null check ---

    explicit operator bool() const { return off_ != 0; }
    bool is_null() const { return off_ == 0; }

    // --- Raw offset access ---

    uint64_t offset() const { return off_; }

    // --- Pointer arithmetic ---
    //     Advances by n * sizeof(T), like native pointer arithmetic.

    CXLPtr operator+(ptrdiff_t n) const {
        return CXLPtr(off_ + static_cast<uint64_t>(n * static_cast<ptrdiff_t>(sizeof(T))));
    }

    CXLPtr operator-(ptrdiff_t n) const {
        return CXLPtr(off_ - static_cast<uint64_t>(n * static_cast<ptrdiff_t>(sizeof(T))));
    }

    CXLPtr& operator++() {
        off_ += sizeof(T);
        return *this;
    }

    CXLPtr operator++(int) {
        CXLPtr tmp = *this;
        off_ += sizeof(T);
        return tmp;
    }

    CXLPtr& operator--() {
        off_ -= sizeof(T);
        return *this;
    }

    CXLPtr operator--(int) {
        CXLPtr tmp = *this;
        off_ -= sizeof(T);
        return tmp;
    }

    // Difference between two pointers (in elements, not bytes)
    ptrdiff_t operator-(CXLPtr other) const {
        return static_cast<ptrdiff_t>(off_ - other.off_) /
               static_cast<ptrdiff_t>(sizeof(T));
    }

    T& operator[](ptrdiff_t n) const {
        return *(*this + n).get();
    }

    // --- Type cast ---
    //     Reinterprets the offset as pointing to a different type.
    //     Analogous to reinterpret_cast<U*>(ptr).

    template<typename U>
    CXLPtr<U> cast() const {
        return CXLPtr<U>(off_);
    }

    // --- Comparison (pure offset comparison, no base needed) ---

    bool operator==(CXLPtr other) const { return off_ == other.off_; }
    bool operator!=(CXLPtr other) const { return off_ != other.off_; }
    bool operator<(CXLPtr other) const  { return off_ < other.off_; }
    bool operator<=(CXLPtr other) const { return off_ <= other.off_; }
    bool operator>(CXLPtr other) const  { return off_ > other.off_; }
    bool operator>=(CXLPtr other) const { return off_ >= other.off_; }
};

// Verify CXLPtr is exactly 8 bytes and trivially copyable
static_assert(sizeof(CXLPtr<int>) == sizeof(uint64_t),
              "CXLPtr must be exactly 8 bytes");
static_assert(std::is_trivially_copyable<CXLPtr<int>>::value,
              "CXLPtr must be trivially copyable for CXL shared memory");

// ============================================================================
// CXLPtr<void> — Type-Erased Variant
//
// Stores an offset without type info. Use cast<T>() to recover the type.
// No operator-> or operator* (same as void*).
// ============================================================================

template<>
class CXLPtr<void> {
    uint64_t off_;

public:
    CXLPtr() : off_(0) {}
    explicit CXLPtr(uint64_t offset) : off_(offset) {}

    static CXLPtr from_raw(const void* ptr) {
        if (!ptr) return CXLPtr();
        return CXLPtr(static_cast<const char*>(ptr) - CXLBase::get());
    }

    __attribute__((always_inline))
    void* get() const {
        return off_ ? static_cast<void*>(CXLBase::get() + off_) : nullptr;
    }

    explicit operator bool() const { return off_ != 0; }
    bool is_null() const { return off_ == 0; }
    uint64_t offset() const { return off_; }

    template<typename U>
    CXLPtr<U> cast() const { return CXLPtr<U>(off_); }

    bool operator==(CXLPtr other) const { return off_ == other.off_; }
    bool operator!=(CXLPtr other) const { return off_ != other.off_; }
    bool operator<(CXLPtr other) const  { return off_ < other.off_; }
    bool operator<=(CXLPtr other) const { return off_ <= other.off_; }
    bool operator>(CXLPtr other) const  { return off_ > other.off_; }
    bool operator>=(CXLPtr other) const { return off_ >= other.off_; }
};

// ============================================================================
// Convenience functions
// ============================================================================

// Create a typed CXL pointer from an offset
template<typename T>
inline CXLPtr<T> cxl_make_ptr(uint64_t offset) {
    return CXLPtr<T>(offset);
}

// Get the offset of a raw pointer relative to CXL base
template<typename T>
inline uint64_t cxl_offset_of(const T* ptr) {
    if (!ptr) return 0;
    return static_cast<uint64_t>(
        reinterpret_cast<const char*>(ptr) - CXLBase::get()
    );
}