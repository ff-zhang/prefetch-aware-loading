//
// Created by Felix Zhang on 2/24/26.
//

#pragma once

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
    #include <immintrin.h>
    #if defined(_MSC_VER)
        #define cpu_pause() _mm_pause()
    #else
        #define cpu_pause() __builtin_ia32_pause()
    #endif

#elif defined(__aarch64__) || defined(_M_ARM64) || defined(__arm__) || defined(_M_ARM)
    #if defined(_MSC_VER)
        #include <intrin.h>
        #define cpu_pause() __yield()
    #else
        #define cpu_pause() __asm__ volatile("yield" ::: "memory")
    #endif

#else
    // Prevent compiler reordering and over-optimization
    std::atomic_signal_fence(std::memory_order_seq_cst)

#endif
