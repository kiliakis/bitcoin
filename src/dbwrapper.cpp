// Copyright (c) 2012-2019 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <dbwrapper.h>

#include <memory>
#include <random.h>

#include <leveldb/cache.h>
#include <leveldb/env.h>
#include <leveldb/filter_policy.h>
#include <memenv.h>
#include <stdint.h>
#include <algorithm>

/* MegaKV headers */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <setjmp.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <sched.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_memory.h>
#include <rte_memcpy.h>
#include <rte_memzone.h>
#include <rte_tailq.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_launch.h>
#include <rte_atomic.h>
#include <rte_cycles.h>
#include <rte_prefetch.h>
#include <rte_lcore.h>
#include <rte_per_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_interrupts.h>
#include <rte_pci.h>
#include <rte_random.h>
#include <rte_debug.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <rte_ethdev.h>
#include <rte_ring.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_byteorder.h>


#define _GNU_SOURCE
#define __USE_GNU

#define MBUF_SIZE (2048 + sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM)
#define NB_MBUF  2048

/*
 * RX and TX Prefetch, Host, and Write-back threshold values should be
 * carefully set for optimal performance. Consult the network
 * controller's datasheet and supporting DPDK documentation for guidance
 * on how these parameters should be set.
 */
#define RX_PTHRESH 8 /**< Default values of RX prefetch threshold reg. */
#define RX_HTHRESH 8 /**< Default values of RX host threshold reg. */
#define RX_WTHRESH 4 /**< Default values of RX write-back threshold reg. */

/*
 * These default values are optimized for use with the Intel(R) 82599 10 GbE
 * Controller and the DPDK ixgbe PMD. Consider using other values for other
 * network controllers and/or network drivers.
 */
#define TX_PTHRESH 36 /**< Default values of TX prefetch threshold reg. */
#define TX_HTHRESH 0  /**< Default values of TX host threshold reg. */
#define TX_WTHRESH 0  /**< Default values of TX write-back threshold reg. */

#define MAX_PKT_BURST 1
#define BURST_TX_DRAIN_US 100 /* TX drain every ~100us */

/*
 * Configurable number of RX/TX ring descriptors
 */
#define RTE_TEST_RX_DESC_DEFAULT 128
#define RTE_TEST_TX_DESC_DEFAULT 512
static uint16_t nb_rxd = RTE_TEST_RX_DESC_DEFAULT;
static uint16_t nb_txd = RTE_TEST_TX_DESC_DEFAULT;

struct mbuf_table {
    unsigned len;
    struct rte_mbuf *m_table[MAX_PKT_BURST];
};

#define MAX_RX_QUEUE_PER_LCORE 16
#define MAX_TX_QUEUE_PER_PORT 16
struct lcore_queue_conf {
    struct mbuf_table tx_mbufs[MAX_TX_QUEUE_PER_PORT];
} __rte_cache_aligned;
struct lcore_queue_conf lcore_queue_conf[NUM_QUEUE];

static const struct rte_eth_conf port_conf = {
    .rxmode = {
        .mq_mode = ETH_MQ_RX_RSS,
        .max_rx_pkt_len = ETHER_MAX_LEN,
        .split_hdr_size = 0,
        .header_split   = 0, /**< Header Split disabled */
        .hw_ip_checksum = 0, /**< IP checksum offload disabled */
        .hw_vlan_filter = 0, /**< VLAN filtering disabled */
        .jumbo_frame    = 0, /**< Jumbo Frame Support disabled */
        .hw_strip_crc   = 0, /**< CRC stripped by hardware */
    },
    .rx_adv_conf = {
        .rss_conf = {
            .rss_key = NULL,
            .rss_hf = ETH_RSS_IP,
        },
    },
    .txmode = {
        .mq_mode = ETH_MQ_TX_NONE,
    },
};

static const struct rte_eth_rxconf rx_conf = {
    .rx_thresh = {
        .pthresh = RX_PTHRESH,
        .hthresh = RX_HTHRESH,
        .wthresh = RX_WTHRESH,
    },
};

static const struct rte_eth_txconf tx_conf = {
    .tx_thresh = {
        .pthresh = TX_PTHRESH,
        .hthresh = TX_HTHRESH,
        .wthresh = TX_WTHRESH,
    },
    .tx_free_thresh = 0, /* Use PMD default values */
    .tx_rs_thresh = 0, /* Use PMD default values */
    /*
     * As the example won't handle mult-segments and offload cases,
     * set the flag by default.
     */
    .txq_flags = ETH_TXQ_FLAGS_NOMULTSEGS | ETH_TXQ_FLAGS_NOOFFLOADS,
};

struct rte_mempool *recv_pktmbuf_pool[NUM_QUEUE];
struct rte_mempool *send_pktmbuf_pool = NULL;
#define NUM_MAX_CORE 32
/* Per-port statistics struct */
struct benchmark_core_statistics {
    uint64_t tx;
    uint64_t rx;
    uint64_t dropped;
    int enable;
} __rte_cache_aligned;
struct benchmark_core_statistics core_statistics[NUM_MAX_CORE];

/* A tsc-based timer responsible for triggering statistics printout */
#define TIMER_MILLISECOND 2000000ULL /* around 1ms at 2 Ghz */
#define MAX_TIMER_PERIOD 86400 /* 1 day max */
static int64_t timer_period = 5 * TIMER_MILLISECOND * 1000; /* default period is 5 seconds */

struct timeval startime;
struct timeval endtime;
uint64_t ts_count[NUM_QUEUE], ts_total[NUM_QUEUE];

typedef struct context_s {
    unsigned int core_id;
    unsigned int queue_id;
} context_t;

#define EIU_HEADER_LEN  42//14+20+8 = 42
#define ETHERNET_HEADER_LEN 14

/* 1500 bytes MTU + 14 Bytes Ethernet header */
int pktlen;



class CBitcoinLevelDBLogger : public leveldb::Logger {
public:
    // This code is adapted from posix_logger.h, which is why it is using vsprintf.
    // Please do not do this in normal code
    void Logv(const char * format, va_list ap) override {
            if (!LogAcceptCategory(BCLog::LEVELDB)) {
                return;
            }
            char buffer[500];
            for (int iter = 0; iter < 2; iter++) {
                char* base;
                int bufsize;
                if (iter == 0) {
                    bufsize = sizeof(buffer);
                    base = buffer;
                }
                else {
                    bufsize = 30000;
                    base = new char[bufsize];
                }
                char* p = base;
                char* limit = base + bufsize;

                // Print the message
                if (p < limit) {
                    va_list backup_ap;
                    va_copy(backup_ap, ap);
                    // Do not use vsnprintf elsewhere in bitcoin source code, see above.
                    p += vsnprintf(p, limit - p, format, backup_ap);
                    va_end(backup_ap);
                }

                // Truncate to available space if necessary
                if (p >= limit) {
                    if (iter == 0) {
                        continue;       // Try again with larger buffer
                    }
                    else {
                        p = limit - 1;
                    }
                }

                // Add newline if necessary
                if (p == base || p[-1] != '\n') {
                    *p++ = '\n';
                }

                assert(p <= limit);
                base[std::min(bufsize - 1, (int)(p - base))] = '\0';
                LogPrintf("leveldb: %s", base);  /* Continued */
                if (base != buffer) {
                    delete[] base;
                }
                break;
            }
    }
};

static void SetMaxOpenFiles(leveldb::Options *options) {
    // On most platforms the default setting of max_open_files (which is 1000)
    // is optimal. On Windows using a large file count is OK because the handles
    // do not interfere with select() loops. On 64-bit Unix hosts this value is
    // also OK, because up to that amount LevelDB will use an mmap
    // implementation that does not use extra file descriptors (the fds are
    // closed after being mmap'ed).
    //
    // Increasing the value beyond the default is dangerous because LevelDB will
    // fall back to a non-mmap implementation when the file count is too large.
    // On 32-bit Unix host we should decrease the value because the handles use
    // up real fds, and we want to avoid fd exhaustion issues.
    //
    // See PR #12495 for further discussion.

    int default_open_files = options->max_open_files;
#ifndef WIN32
    if (sizeof(void*) < 8) {
        options->max_open_files = 64;
    }
#endif
    LogPrint(BCLog::LEVELDB, "LevelDB using max_open_files=%d (default=%d)\n",
             options->max_open_files, default_open_files);
}

static leveldb::Options GetOptions(size_t nCacheSize)
{
    leveldb::Options options;
    options.block_cache = leveldb::NewLRUCache(nCacheSize / 2);
    options.write_buffer_size = nCacheSize / 4; // up to two write buffers may be held in memory simultaneously
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.compression = leveldb::kNoCompression;
    options.info_log = new CBitcoinLevelDBLogger();
    if (leveldb::kMajorVersion > 1 || (leveldb::kMajorVersion == 1 && leveldb::kMinorVersion >= 16)) {
        // LevelDB versions before 1.16 consider short writes to be corruption. Only trigger error
        // on corruption in later versions.
        options.paranoid_checks = true;
    }
    SetMaxOpenFiles(&options);
    return options;
}

// CDBBatch

void CDBBatch::Clear()
{
    batch.Clear();
    size_estimate = 0;
}

// this is a SET job type, it just adds to the buffer.
template <typename K, typename V>
void CDBBatch::Write(const K& key, const V& value)
{
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    leveldb::Slice slKey(ssKey.data(), ssKey.size());

    ssValue.reserve(DBWRAPPER_PREALLOC_VALUE_SIZE);
    ssValue << value;
    ssValue.Xor(dbwrapper_private::GetObfuscateKey(parent));
    leveldb::Slice slValue(ssValue.data(), ssValue.size());

    batch.Put(slKey, slValue);
    // LevelDB serializes writes as:
    // - byte: header
    // - varint: key length (1 byte up to 127B, 2 bytes up to 16383B, ...)
    // - byte[]: key
    // - varint: value length
    // - byte[]: value
    // The formula below assumes the key and value are both less than 16k.
    size_estimate += 3 + (slKey.size() > 127) + slKey.size() + (slValue.size() > 127) + slValue.size();
    ssKey.clear();
    ssValue.clear();
}

// We don't support erase
template <typename K>
void CDBBatch::Erase(const K& key)
{
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    leveldb::Slice slKey(ssKey.data(), ssKey.size());

    batch.Delete(slKey);
    // LevelDB serializes erases as:
    // - byte: header
    // - varint: key length
    // - byte[]: key
    // The formula below assumes the key is less than 16kB.
    size_estimate += 2 + (slKey.size() > 127) + slKey.size();
    ssKey.clear();
}


// CDBIterator

CDBIterator::~CDBIterator() { delete piter; }
bool CDBIterator::Valid() const { return piter->Valid(); }
void CDBIterator::SeekToFirst() { piter->SeekToFirst(); }
void CDBIterator::Next() { piter->Next(); }

template<typename K> void CDBIterator::Seek(const K& key)
{
    CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    leveldb::Slice slKey(ssKey.data(), ssKey.size());
    piter->Seek(slKey);
}


template<typename K> bool CDBIterator::GetKey(K& key)
{
    leveldb::Slice slKey = piter->key();
    try {
        CDataStream ssKey(slKey.data(), slKey.data() + slKey.size(), SER_DISK, CLIENT_VERSION);
        ssKey >> key;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

template<typename V> bool CDBIterator::GetValue(V& value)
{
    leveldb::Slice slValue = piter->value();
    try {
        CDataStream ssValue(slValue.data(), slValue.data() + slValue.size(), SER_DISK, CLIENT_VERSION);
        ssValue.Xor(dbwrapper_private::GetObfuscateKey(parent));
        ssValue >> value;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

unsigned int CDBIterator::GetValueSize()
{
    return piter->value().size();
}


CDBWrapper::CDBWrapper(const fs::path& path, size_t nCacheSize, bool fMemory, bool fWipe, bool obfuscate)
    : m_name{path.stem().string()}
{
    penv = nullptr;
    readoptions.verify_checksums = true;
    iteroptions.verify_checksums = true;
    iteroptions.fill_cache = false;
    syncoptions.sync = true;
    options = GetOptions(nCacheSize);
    options.create_if_missing = true;
    if (fMemory) {
        penv = leveldb::NewMemEnv(leveldb::Env::Default());
        options.env = penv;
    } else {
        if (fWipe) {
            LogPrintf("Wiping LevelDB in %s\n", path.string());
            leveldb::Status result = leveldb::DestroyDB(path.string(), options);
            dbwrapper_private::HandleError(result);
        }
        TryCreateDirectories(path);
        LogPrintf("Opening LevelDB in %s\n", path.string());
    }
    leveldb::Status status = leveldb::DB::Open(options, path.string(), &pdb);
    dbwrapper_private::HandleError(status);
    LogPrintf("Opened LevelDB successfully\n");

    if (gArgs.GetBoolArg("-forcecompactdb", false)) {
        LogPrintf("Starting database compaction of %s\n", path.string());
        pdb->CompactRange(nullptr, nullptr);
        LogPrintf("Finished database compaction of %s\n", path.string());
    }

    // The base-case obfuscation key, which is a noop.
    obfuscate_key = std::vector<unsigned char>(OBFUSCATE_KEY_NUM_BYTES, '\000');

    bool key_exists = Read(OBFUSCATE_KEY_KEY, obfuscate_key);

    if (!key_exists && obfuscate && IsEmpty()) {
        // Initialize non-degenerate obfuscation if it won't upset
        // existing, non-obfuscated data.
        std::vector<unsigned char> new_key = CreateObfuscateKey();

        // Write `new_key` so we don't obfuscate the key with itself
        Write(OBFUSCATE_KEY_KEY, new_key);
        obfuscate_key = new_key;

        LogPrintf("Wrote new obfuscate key for %s: %s\n", path.string(), HexStr(obfuscate_key));
    }

    LogPrintf("Using obfuscation key for %s: %s\n", path.string(), HexStr(obfuscate_key));
}

CDBWrapper::~CDBWrapper()
{
    delete pdb;
    pdb = nullptr;
    delete options.filter_policy;
    options.filter_policy = nullptr;
    delete options.info_log;
    options.info_log = nullptr;
    delete options.block_cache;
    options.block_cache = nullptr;
    delete penv;
    options.env = nullptr;
}

// this sends a batch of write requests
bool CDBWrapper::WriteBatch(CDBBatch& batch, bool fSync)
{
    const bool log_memory = LogAcceptCategory(BCLog::LEVELDB);
    double mem_before = 0;
    if (log_memory) {
        mem_before = DynamicMemoryUsage() / 1024.0 / 1024;
    }
    leveldb::Status status = pdb->Write(fSync ? syncoptions : writeoptions, &batch.batch);
    dbwrapper_private::HandleError(status);
    if (log_memory) {
        double mem_after = DynamicMemoryUsage() / 1024.0 / 1024;
        LogPrint(BCLog::LEVELDB, "WriteBatch memory usage: db=%s, before=%.1fMiB, after=%.1fMiB\n",
                 m_name, mem_before, mem_after);
    }
    return true;
}

size_t CDBWrapper::DynamicMemoryUsage() const {
    std::string memory;
    if (!pdb->GetProperty("leveldb.approximate-memory-usage", &memory)) {
        LogPrint(BCLog::LEVELDB, "Failed to get approximate-memory-usage property\n");
        return 0;
    }
    return stoul(memory);
}

// Prefixed with null character to avoid collisions with other keys
//
// We must use a string constructor which specifies length so that we copy
// past the null-terminator.
const std::string CDBWrapper::OBFUSCATE_KEY_KEY("\000obfuscate_key", 14);

const unsigned int CDBWrapper::OBFUSCATE_KEY_NUM_BYTES = 8;

/**
 * Returns a string (consisting of 8 random bytes) suitable for use as an
 * obfuscating XOR key.
 */
std::vector<unsigned char> CDBWrapper::CreateObfuscateKey() const
{
    unsigned char buff[OBFUSCATE_KEY_NUM_BYTES];
    GetRandBytes(buff, OBFUSCATE_KEY_NUM_BYTES);
    return std::vector<unsigned char>(&buff[0], &buff[OBFUSCATE_KEY_NUM_BYTES]);

}


bool CDBWrapper::IsEmpty()
{
    std::unique_ptr<CDBIterator> it(NewIterator());
    it->SeekToFirst();
    return !(it->Valid());
}

// get type
template <typename K, typename V>
bool CDBWrapper::Read(const K& key, V& value) const
{
    CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    leveldb::Slice slKey(ssKey.data(), ssKey.size());

    std::string strValue;
    leveldb::Status status = pdb->Get(readoptions, slKey, &strValue);
    if (!status.ok()) {
        if (status.IsNotFound())
            return false;
        LogPrintf("LevelDB read failure: %s\n", status.ToString());
        dbwrapper_private::HandleError(status);
    }
    try {
        CDataStream ssValue(strValue.data(), strValue.data() + strValue.size(), SER_DISK, CLIENT_VERSION);
        ssValue.Xor(obfuscate_key);
        ssValue >> value;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

// set type, this only adds data to the write buffer. 
template <typename K, typename V>
bool CDBWrapper::Write(const K& key, const V& value, bool fSync = false)
{
    CDBBatch batch(*this);
    batch.Write(key, value);
    return WriteBatch(batch, fSync);
}

// get type
template <typename K>
bool CDBWrapper::Exists(const K& key) const
{
    CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    leveldb::Slice slKey(ssKey.data(), ssKey.size());

    std::string strValue;
    leveldb::Status status = pdb->Get(readoptions, slKey, &strValue);
    if (!status.ok()) {
        if (status.IsNotFound())
            return false;
        LogPrintf("LevelDB read failure: %s\n", status.ToString());
        dbwrapper_private::HandleError(status);
    }
    return true;
}

// Not really needed
template <typename K>
bool CDBWrapper::Erase(const K& key, bool fSync = false)
{
    CDBBatch batch(*this);
    batch.Erase(key);
    return WriteBatch(batch, fSync);
}

// can be approximated possibly
template<typename K>
size_t CDBWrapper::EstimateSize(const K& key_begin, const K& key_end) const
{
    CDataStream ssKey1(SER_DISK, CLIENT_VERSION), ssKey2(SER_DISK, CLIENT_VERSION);
    ssKey1.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey2.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey1 << key_begin;
    ssKey2 << key_end;
    leveldb::Slice slKey1(ssKey1.data(), ssKey1.size());
    leveldb::Slice slKey2(ssKey2.data(), ssKey2.size());
    uint64_t size = 0;
    leveldb::Range range(slKey1, slKey2);
    pdb->GetApproximateSizes(&range, 1, &size);
    return size;
}

/**
 * Compact a certain range of keys in the database.
 */
// Used in 1-2 places, we could have a dummy implementation
template<typename K>
void CDBWrapper::CompactRange(const K& key_begin, const K& key_end) const
{
    // CDataStream ssKey1(SER_DISK, CLIENT_VERSION), ssKey2(SER_DISK, CLIENT_VERSION);
    // ssKey1.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    // ssKey2.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    // ssKey1 << key_begin;
    // ssKey2 << key_end;
    // leveldb::Slice slKey1(ssKey1.data(), ssKey1.size());
    // leveldb::Slice slKey2(ssKey2.data(), ssKey2.size());
    // pdb->CompactRange(&slKey1, &slKey2);
    return;
}



namespace dbwrapper_private {

void HandleError(const leveldb::Status& status)
{
    if (status.ok())
        return;
    const std::string errmsg = "Fatal LevelDB error: " + status.ToString();
    LogPrintf("%s\n", errmsg);
    LogPrintf("You can use -debug=leveldb to get more complete diagnostic messages\n");
    throw dbwrapper_error(errmsg);
}

const std::vector<unsigned char>& GetObfuscateKey(const CDBWrapper &w)
{
    return w.obfuscate_key;
}

} // namespace dbwrapper_private
