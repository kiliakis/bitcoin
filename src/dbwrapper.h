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

#include <deque>
// #include <leveldb/db.h>
// #include <leveldb/write_batch.h>
// For MegaKV
#include <rte_mempool.h>

/* BITS_INSERT_BUF should be same with mega: config->bits_insert_buf */
#define BITS_INSERT_BUF 3 // 2^3 = 8

/* Following protocol speicific parameters should be same with MEGA */
const uint16_t PROTOCOL_MAGIC = 0x1234;

const uint16_t MEGA_MAGIC_NUM_LEN = 2;
const uint16_t MEGA_END_MARK_LEN = 2;

const uint16_t ETHERNET_MAX_FRAME_LEN = 1514;
const uint16_t EIU_HEADER_LEN = 42;      //14+20+8 = 42
const uint16_t ETHERNET_HEADER_LEN = 14;

const uint32_t KV_IP_ADDR = (uint32_t)(789);
const uint16_t KV_UDP_PORT = (uint16_t)(124);
const uint32_t LOCAL_IP_ADDR = (uint32_t)(456);
const uint16_t LOCAL_UDP_PORT = (uint16_t)(123);

#define _GNU_SOURCE
#define __USE_GNU


/*
 * Configurable number of RX/TX ring descriptors
 */
#define RTE_TEST_RX_DESC_DEFAULT 128
#define RTE_TEST_TX_DESC_DEFAULT 512


#define TIMER_MILLISECOND 2000000ULL /* around 1ms at 2 Ghz */
#define MAX_TIMER_PERIOD 86400 /* 1 day max */

const float ZIPF_THETA = 0.00;
const uint16_t AFFINITY_ONE_NODE = 1;
const uint16_t NUM_QUEUE = 4;
const uint16_t MAX_RX_QUEUE_PER_LCORE = 16;
const uint16_t MAX_TX_QUEUE_PER_PORT = 16;
const uint16_t NUM_MAX_CORE = 32;
const uint16_t MAX_PKT_BURST = 1;
const uint16_t BURST_TX_DRAIN_US = 100; /* TX drain every ~100us */
const uint16_t MBUF_SIZE = (2048 + sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM);
const uint16_t NB_MBUF =  2048;


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

/*
 * RX and TX Prefetch, Host, and Write-back threshold values should be
 * carefully set for optimal performance. Consult the network
 * controller's datasheet and supporting DPDK documentation for guidance
 * on how these parameters should be set.
 */
#define RX_PTHRESH 8 /**< Default values of RX prefetch threshold reg. */
#define RX_HTHRESH 8 /**< Default values of RX host threshold reg. */
#define RX_WTHRESH 4 /**< Default values of RX write-back threshold reg. */


static const struct rte_eth_rxconf rx_conf = {
    .rx_thresh = {
        .pthresh = RX_PTHRESH,
        .hthresh = RX_HTHRESH,
        .wthresh = RX_WTHRESH,
    },
};


/*
 * These default values are optimized for use with the Intel(R) 82599 10 GbE
 * Controller and the DPDK ixgbe PMD. Consider using other values for other
 * network controllers and/or network drivers.
 */
#define TX_PTHRESH 36 /**< Default values of TX prefetch threshold reg. */
#define TX_HTHRESH 0  /**< Default values of TX host threshold reg. */
#define TX_WTHRESH 0  /**< Default values of TX write-back threshold reg. */

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


/* Check the link status of all ports in up to 9s, and print them finally */
static void
check_all_ports_link_status(uint8_t port_num, uint32_t port_mask)
{
#define CHECK_INTERVAL 100 /* 100ms */
#define MAX_CHECK_TIME 90 /* 9s (90 * 100ms) in total */
    uint8_t portid, count, all_ports_up, print_flag = 0;
    struct rte_eth_link link;

    printf("\nChecking link status");
    fflush(stdout);
    for (count = 0; count <= MAX_CHECK_TIME; count++) {
        all_ports_up = 1;
        for (portid = 0; portid < port_num; portid++) {
            if ((port_mask & (1 << portid)) == 0)
                continue;
            memset(&link, 0, sizeof(link));
            rte_eth_link_get_nowait(portid, &link);
            /* print link status if flag set */
            if (print_flag == 1) {
                if (link.link_status)
                    printf("Port %d Link Up - speed %u "
                        "Mbps - %s\n", (uint8_t)portid,
                        (unsigned)link.link_speed,
                (link.link_duplex == ETH_LINK_FULL_DUPLEX) ?
                    ("full-duplex") : ("half-duplex\n"));
                else
                    printf("Port %d Link Down\n",
                        (uint8_t)portid);
                continue;
            }
            /* clear all_ports_up flag if any link down */
            if (link.link_status == 0) {
                all_ports_up = 0;
                break;
            }
        }
        /* after finally printing all link status, get out */
        if (print_flag == 1)
            break;

        if (all_ports_up == 0) {
            printf(".");
            fflush(stdout);
            rte_delay_ms(CHECK_INTERVAL);
        }

        /* set the print_flag if all ports up or timeout */
        if (all_ports_up == 1 || count == (MAX_CHECK_TIME - 1)) {
            print_flag = 1;
            printf("done\n");
        }
    }
}


// End for MegaKV

static const size_t DBWRAPPER_PREALLOC_KEY_SIZE = 64;
static const size_t DBWRAPPER_PREALLOC_VALUE_SIZE = 1024;

class dbwrapper_error : public std::runtime_error
{
public:
    explicit dbwrapper_error(const std::string& msg) : std::runtime_error(msg) {}
};

class CDBWrapper;

namespace megakv {
    enum request_t = {MEGA_JOB_GET=0x2, MEGA_JOB_SET=0x3, MEGA_JOB_DEL=0x4};
    struct Request {
        request_t type;
        char *key;
        char *val;
        uint16_t key_size;
        uint32_t val_size;

        Request(request_t _type, char *_key, uint16_t _key_size, char *_val=NULL, uint32_t _val_size = 0){
            type = _type;
            key = _key;
            key_size = _key_size;
            val = _val;
            val_size = _val_size;
        }

        size_t size() {
            if(type == MEGA_JOB_GET){
                return 2 + 2 + key_size;
            }else if (type == MEGA_JOB_SET){
                return 2 + 2 + 4 + key_size + val_size;
            }else if (type == MEGA_JOB_DEL){
                return 2 + 2 + key_size;
            }else{
                LogPrintf("%s\n", "Fatal MegaKV error: Invalid request type.");
                throw dbwrapper_error("Fatal MegaKV error: Invalid request type.\n");
            }
            return 0;
        }

    };
    
    struct Options
    {
        Options();
        // several fields should go here


    };
    
    class WriteBatch {
    private:
        std::deque<megakv::Request *> request_v;
        rte_mempool *send_pktmbuf_pool = NULL;
        lcore_queue_conf *qconf;
        unsigned int core_id;
        unsigned int queue_id;
    public:
        WriteBatch::WriteBatch(unsigned int _core_id, unsigned int _queue_id);

        void Put(const char* key, size_t key_size, const char* value, size_t value_size);

        void Clear();

        void Delete(const char* key, size_t key_size);

    };

} // megakv



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
    megakv::WriteBatch batch;

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

    template <typename K, typename V>
    void Write(const K& key, const V& value);

    template <typename K>
    void Erase(const K& key);

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

    template<typename V> bool GetValue(V& value);

    unsigned int GetValueSize();

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

    template <typename K, typename V>
    bool Write(const K& key, const V& value, bool fSync = false);

    template <typename K>
    bool Exists(const K& key) const;

    template <typename K>
    bool Erase(const K& key, bool fSync = false);

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

    /**
     * Compact a certain range of keys in the database.
     */
    template<typename K>
    void CompactRange(const K& key_begin, const K& key_end) const;

};

#endif // BITCOIN_DBWRAPPER_H
