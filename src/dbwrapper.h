// Copyright (c) 2012-2020 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_DBWRAPPER_H
#define BITCOIN_DBWRAPPER_H

#include <clientversion.h>
#include <fs.h>
#include <serialize.h>
#include <streams.h>
#include <util/system.h>
#include <util/strencodings.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

// For MegaKV
#include <rte_mempool.h>

/* Following protocol speicific parameters should be same with MEGA */
#define PROTOCOL_MAGIC  0x1234
#define MEGA_JOB_GET 0x2
#define MEGA_JOB_SET 0x3
/* BITS_INSERT_BUF should be same with mega: config->bits_insert_buf */
#define BITS_INSERT_BUF 3 // 2^3 = 8

#define MEGA_MAGIC_NUM_LEN  2
#define MEGA_END_MARK_LEN   2

#define ZIPF_THETA 0.00
#define AFFINITY_ONE_NODE 1
#define NUM_QUEUE 4

/* Hash Table Load Factor, These should be the same with the main program
 * if PRELOAD is disabled! TODO: avoid mismatches */
#define LOAD_FACTOR 0.2
#define PRELOAD_CNT (LOAD_FACTOR * ((1 << 30)/8))
#define TOTAL_CNT (((uint32_t)1 << 31) - 1)

#define KEY_LEN         8
#define VALUE_LEN       8
#define SET_LEN     (KEY_LEN + VALUE_LEN + 8)
#define ETHERNET_MAX_FRAME_LEN  1514

#define KV_IP_ADDR (uint32_t)(789)
#define KV_UDP_PORT (uint16_t)(124)
#define LOCAL_IP_ADDR (uint32_t)(456)
#define LOCAL_UDP_PORT (uint16_t)(123)

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

#define MAX_RX_QUEUE_PER_LCORE 16
#define MAX_TX_QUEUE_PER_PORT 16
#define NUM_MAX_CORE 32

#define TIMER_MILLISECOND 2000000ULL /* around 1ms at 2 Ghz */
#define MAX_TIMER_PERIOD 86400 /* 1 day max */

#define EIU_HEADER_LEN  42//14+20+8 = 42
#define ETHERNET_HEADER_LEN 14


static uint16_t nb_rxd = RTE_TEST_RX_DESC_DEFAULT;
static uint16_t nb_txd = RTE_TEST_TX_DESC_DEFAULT;

struct mbuf_table {
    unsigned len;
    struct rte_mbuf *m_table[MAX_PKT_BURST];
};

struct lcore_queue_conf {
    struct mbuf_table tx_mbufs[MAX_TX_QUEUE_PER_PORT];
} __rte_cache_aligned;


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

// rte_mempool *recv_pktmbuf_pool[NUM_QUEUE];
// rte_mempool *send_pktmbuf_pool = NULL;

/* Per-port statistics struct */
struct benchmark_core_statistics {
    uint64_t tx;
    uint64_t rx;
    uint64_t dropped;
    int enable;
} __rte_cache_aligned;



// typedef struct context_s {
//     unsigned int core_id;
//     unsigned int queue_id;
// } context_t;


/* 1500 bytes MTU + 14 Bytes Ethernet header */
// int pktlen;


// End for MegaKV


static const size_t DBWRAPPER_PREALLOC_KEY_SIZE = 64;
static const size_t DBWRAPPER_PREALLOC_VALUE_SIZE = 1024;

class dbwrapper_error : public std::runtime_error
{
public:
    explicit dbwrapper_error(const std::string& msg) : std::runtime_error(msg) {}
};

class CDBWrapper;


class WriteBatch {
private:
    struct rte_mempool *send_pktmbuf_pool = NULL;
    lcore_queue_conf *qconf;
    unsigned int core_id;
    unsigned int queue_id;
    char *curr_pos = NULL; // points to the position of the packet we have to write. 
public:
    WriteBatch::WriteBatch();

    // struct rte_mbuf *m;

    // struct rte_mempool *recv_pktmbuf_pool[NUM_QUEUE];
    // struct rte_mempool *send_pktmbuf_pool = NULL;

    void Put(const char* key, size_t key_size, const char* value, size_t value_size);

    void Clear();

    void Delete(const char* key, size_t key_size);

};

/** These should be considered an implementation detail of the specific database.
 */
namespace dbwrapper_private {



/** Handle database error by throwing dbwrapper_error exception.
 */
void HandleError(const leveldb::Status& status);

/** Work around circular dependency, as well as for testing in dbwrapper_tests.
 * Database obfuscation should be considered an implementation detail of the
 * specific database.
 */
const std::vector<unsigned char>& GetObfuscateKey(const CDBWrapper &w);

};

/** Batch of changes queued to be written to a CDBWrapper */
class CDBBatch
{
    friend class CDBWrapper;

private:
    const CDBWrapper &parent;
    // LevelDB::WriteBatch batch;
    WriteBatch batch;

    CDataStream ssKey;
    CDataStream ssValue;

    size_t size_estimate;

public:
    /**
     * @param[in] _parent   CDBWrapper that this batch is to be submitted to
     */
    explicit CDBBatch(const CDBWrapper &_parent) : parent(_parent),
        ssKey(SER_DISK, CLIENT_VERSION),
        ssValue(SER_DISK, CLIENT_VERSION), 
        size_estimate(EIU_HEADER_LEN + MEGA_MAGIC_NUM_LEN + MEGA_END_MARK_LEN) { };

    void Clear();
    // {
    //     batch.Clear();
    //     size_estimate = 0;
    // }


    template <typename K, typename V>
    void Write(const K& key, const V& value);
    // {
    //     ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey << key;
    //     leveldb::Slice slKey(ssKey.data(), ssKey.size());

    //     ssValue.reserve(DBWRAPPER_PREALLOC_VALUE_SIZE);
    //     ssValue << value;
    //     ssValue.Xor(dbwrapper_private::GetObfuscateKey(parent));
    //     leveldb::Slice slValue(ssValue.data(), ssValue.size());

    //     batch.Put(slKey, slValue);
    //     // LevelDB serializes writes as:
    //     // - byte: header
    //     // - varint: key length (1 byte up to 127B, 2 bytes up to 16383B, ...)
    //     // - byte[]: key
    //     // - varint: value length
    //     // - byte[]: value
    //     // The formula below assumes the key and value are both less than 16k.
    //     size_estimate += 3 + (slKey.size() > 127) + slKey.size() + (slValue.size() > 127) + slValue.size();
    //     ssKey.clear();
    //     ssValue.clear();
    // }

    template <typename K>
    void Erase(const K& key);
    // {
    //     ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey << key;
    //     leveldb::Slice slKey(ssKey.data(), ssKey.size());

    //     batch.Delete(slKey);
    //     // LevelDB serializes erases as:
    //     // - byte: header
    //     // - varint: key length
    //     // - byte[]: key
    //     // The formula below assumes the key is less than 16kB.
    //     size_estimate += 2 + (slKey.size() > 127) + slKey.size();
    //     ssKey.clear();
    // }

    size_t SizeEstimate() const { return size_estimate; }
};

class CDBIterator
{
private:
    const CDBWrapper &parent;
    leveldb::Iterator *piter;

public:

    /**
     * @param[in] _parent          Parent CDBWrapper instance.
     * @param[in] _piter           The original leveldb iterator.
     */
    CDBIterator(const CDBWrapper &_parent, leveldb::Iterator *_piter) :
        parent(_parent), piter(_piter) { };
    ~CDBIterator();

    bool Valid() const;

    void SeekToFirst();

    template<typename K> void Seek(const K& key);
    // {
    //     CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    //     ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey << key;
    //     leveldb::Slice slKey(ssKey.data(), ssKey.size());
    //     piter->Seek(slKey);
    // }

    void Next();

    template<typename K> bool GetKey(K& key);
    // {
    //     leveldb::Slice slKey = piter->key();
    //     try {
    //         CDataStream ssKey(slKey.data(), slKey.data() + slKey.size(), SER_DISK, CLIENT_VERSION);
    //         ssKey >> key;
    //     } catch (const std::exception&) {
    //         return false;
    //     }
    //     return true;
    // }

    template<typename V> bool GetValue(V& value);
    // {
    //     leveldb::Slice slValue = piter->value();
    //     try {
    //         CDataStream ssValue(slValue.data(), slValue.data() + slValue.size(), SER_DISK, CLIENT_VERSION);
    //         ssValue.Xor(dbwrapper_private::GetObfuscateKey(parent));
    //         ssValue >> value;
    //     } catch (const std::exception&) {
    //         return false;
    //     }
    //     return true;
    // }

    unsigned int GetValueSize();
    // {
    //     return piter->value().size();
    // }

};

class CDBWrapper
{
    friend const std::vector<unsigned char>& dbwrapper_private::GetObfuscateKey(const CDBWrapper &w);
private:
    //! custom environment this database is using (may be nullptr in case of default environment)
    leveldb::Env* penv;

    //! database options used
    leveldb::Options options;

    //! options used when reading from the database
    leveldb::ReadOptions readoptions;

    //! options used when iterating over values of the database
    leveldb::ReadOptions iteroptions;

    //! options used when writing to the database
    leveldb::WriteOptions writeoptions;

    //! options used when sync writing to the database
    leveldb::WriteOptions syncoptions;

    //! the database itself
    leveldb::DB* pdb;

    //! the name of this database
    std::string m_name;

    //! a key used for optional XOR-obfuscation of the database
    std::vector<unsigned char> obfuscate_key;

    //! the key under which the obfuscation key is stored
    static const std::string OBFUSCATE_KEY_KEY;

    //! the length of the obfuscate key in number of bytes
    static const unsigned int OBFUSCATE_KEY_NUM_BYTES;

    std::vector<unsigned char> CreateObfuscateKey() const;

public:
    /**
     * @param[in] path        Location in the filesystem where leveldb data will be stored.
     * @param[in] nCacheSize  Configures various leveldb cache settings.
     * @param[in] fMemory     If true, use leveldb's memory environment.
     * @param[in] fWipe       If true, remove all existing data.
     * @param[in] obfuscate   If true, store data obfuscated via simple XOR. If false, XOR
     *                        with a zero'd byte array.
     */
    CDBWrapper(const fs::path& path, size_t nCacheSize, bool fMemory = false, bool fWipe = false, bool obfuscate = false);
    ~CDBWrapper();

    CDBWrapper(const CDBWrapper&) = delete;
    CDBWrapper& operator=(const CDBWrapper&) = delete;

    template <typename K, typename V>
    bool Read(const K& key, V& value) const;
    // {
    //     CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    //     ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey << key;
    //     leveldb::Slice slKey(ssKey.data(), ssKey.size());

    //     std::string strValue;
    //     leveldb::Status status = pdb->Get(readoptions, slKey, &strValue);
    //     if (!status.ok()) {
    //         if (status.IsNotFound())
    //             return false;
    //         LogPrintf("LevelDB read failure: %s\n", status.ToString());
    //         dbwrapper_private::HandleError(status);
    //     }
    //     try {
    //         CDataStream ssValue(strValue.data(), strValue.data() + strValue.size(), SER_DISK, CLIENT_VERSION);
    //         ssValue.Xor(obfuscate_key);
    //         ssValue >> value;
    //     } catch (const std::exception&) {
    //         return false;
    //     }
    //     return true;
    // }

    template <typename K, typename V>
    bool Write(const K& key, const V& value, bool fSync = false);
    // {
    //     CDBBatch batch(*this);
    //     batch.Write(key, value);
    //     return WriteBatch(batch, fSync);
    // }

    template <typename K>
    bool Exists(const K& key) const;
    // {
    //     CDataStream ssKey(SER_DISK, CLIENT_VERSION);
    //     ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey << key;
    //     leveldb::Slice slKey(ssKey.data(), ssKey.size());

    //     std::string strValue;
    //     leveldb::Status status = pdb->Get(readoptions, slKey, &strValue);
    //     if (!status.ok()) {
    //         if (status.IsNotFound())
    //             return false;
    //         LogPrintf("LevelDB read failure: %s\n", status.ToString());
    //         dbwrapper_private::HandleError(status);
    //     }
    //     return true;
    // }

    template <typename K>
    bool Erase(const K& key, bool fSync = false);
    // {
    //     CDBBatch batch(*this);
    //     batch.Erase(key);
    //     return WriteBatch(batch, fSync);
    // }

    bool WriteBatch(CDBBatch& batch, bool fSync = false);

    // Get an estimate of LevelDB memory usage (in bytes).
    size_t DynamicMemoryUsage() const;

    CDBIterator *NewIterator()
    {
        return new CDBIterator(*this, pdb->NewIterator(iteroptions));
    }

    /**
     * Return true if the database managed by this class contains no entries.
     */
    bool IsEmpty();

    template<typename K>
    size_t EstimateSize(const K& key_begin, const K& key_end) const;
    // {
    //     CDataStream ssKey1(SER_DISK, CLIENT_VERSION), ssKey2(SER_DISK, CLIENT_VERSION);
    //     ssKey1.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey2.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey1 << key_begin;
    //     ssKey2 << key_end;
    //     leveldb::Slice slKey1(ssKey1.data(), ssKey1.size());
    //     leveldb::Slice slKey2(ssKey2.data(), ssKey2.size());
    //     uint64_t size = 0;
    //     leveldb::Range range(slKey1, slKey2);
    //     pdb->GetApproximateSizes(&range, 1, &size);
    //     return size;
    // }

    /**
     * Compact a certain range of keys in the database.
     */
    template<typename K>
    void CompactRange(const K& key_begin, const K& key_end) const;
    // {
    //     CDataStream ssKey1(SER_DISK, CLIENT_VERSION), ssKey2(SER_DISK, CLIENT_VERSION);
    //     ssKey1.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey2.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    //     ssKey1 << key_begin;
    //     ssKey2 << key_end;
    //     leveldb::Slice slKey1(ssKey1.data(), ssKey1.size());
    //     leveldb::Slice slKey2(ssKey2.data(), ssKey2.size());
    //     pdb->CompactRange(&slKey1, &slKey2);
    // }

};

#endif // BITCOIN_DBWRAPPER_H
