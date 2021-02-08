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


lcore_queue_conf lcore_queue_conf[NUM_QUEUE];

benchmark_core_statistics core_statistics[NUM_MAX_CORE];
/* A tsc-based timer responsible for triggering statistics printout */
static int64_t timer_period = 5 * TIMER_MILLISECOND * 1000; /* default period is 5 seconds */

struct timeval startime;
struct timeval endtime;
uint64_t ts_count[NUM_QUEUE], ts_total[NUM_QUEUE];


WriteBatch::WriteBatch(unsigned int _core_id,
                       unsigned int _queue_id)
{

    // rte_mbuf *m;

    core_id = _core_id;
    queue_id = _queue_id;
    qconf = &lcore_queue_conf[queue_id];

    // unsigned long mask = 1 << core_id;
    // TODO: See other "Disabled CPU affinity" TODOs
    // if (sched_setaffinity(0, sizeof(unsigned long), (cpu_set_t *)&mask) < 0) {
    //     printf("core id = %d\n", core_id);
    //     assert(0);
    // }

    unsigned int tmp_pktlen;

    struct ether_hdr *ethh;
    struct ipv4_hdr *iph;
    struct udp_hdr *udph;

    /* for 1GB hash table, 512MB signature, 32bits, total is 128M = 2^29/2^2 = 2^27
     * load 80% of the hash table */
    const uint32_t total_cnt = (uint32_t)TOTAL_CNT;
    uint32_t preload_cnt = (uint32_t)PRELOAD_CNT;

    struct zipf_gen_state zipf_state;
    mehcached_zipf_init(&zipf_state, (uint64_t)preload_cnt - 2, (double)ZIPF_THETA, (uint64_t)21);
    //printf("LOAD_FACTOR is %f, total key cnt is %d\n", LOAD_FACTOR, total_cnt);

    for (unsigned i = 0; i < MAX_PKT_BURST; i ++) {
        rte_mbuf *m = rte_pktmbuf_alloc(send_pktmbuf_pool);
        assert (m != NULL);
        qconf->tx_mbufs[queue_id].m_table[i] = m;

        ethh = (struct ether_hdr *)rte_pktmbuf_mtod(m, unsigned char *);
        //ethh->s_addr = LOCAL_MAC_ADDR;
        ethh->ether_type = rte_cpu_to_be_16((uint16_t)(ETHER_TYPE_IPv4));

        iph = (struct ipv4_hdr *)((unsigned char *)ethh + sizeof(struct ether_hdr));
        iph->version_ihl = 0x40 | 0x05;
        iph->type_of_service = 0;
        iph->packet_id = 0;
        iph->fragment_offset = 0;
        iph->time_to_live = 64;
        iph->next_proto_id = IPPROTO_UDP;
        iph->hdr_checksum = 0;
        iph->src_addr = LOCAL_IP_ADDR;
        iph->dst_addr = KV_IP_ADDR;

        udph = (struct udp_hdr *)((unsigned char *)iph + sizeof(struct ipv4_hdr));
        udph->src_port = LOCAL_UDP_PORT;
        udph->dst_port = KV_UDP_PORT;
        udph->dgram_cksum = 0;

        char *ptr = (char *)rte_ctrlmbuf_data(m) + EIU_HEADER_LEN;
        *(uint16_t *)ptr = PROTOCOL_MAGIC;
    }

    qconf->tx_mbufs[queue_id].len = MAX_PKT_BURST;


    struct rte_mbuf **m_table;
    uint32_t *ip;
    uint32_t ip_ctr = 1;
    unsigned int port, ret;
    uint32_t get_key = 1, set_key = 1;

    /* update packet length for the workload packets */
    pktlen = length_packet[WORKLOAD_ID];

    for (i = 0; i < MAX_PKT_BURST; i ++) {
        m = qconf->tx_mbufs[queue_id].m_table[i];
        assert (m != NULL);
        rte_pktmbuf_pkt_len(m) = (uint16_t)pktlen;
        rte_pktmbuf_data_len(m) = (uint16_t)pktlen;

        ethh = (struct ether_hdr *)rte_pktmbuf_mtod(m, unsigned char *);
        iph = (struct ipv4_hdr *)((unsigned char *)ethh + sizeof(struct ether_hdr));
        udph = (struct udp_hdr *)((unsigned char *)iph + sizeof(struct ipv4_hdr));

        iph->total_length = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct ether_hdr)));
        udph->dgram_len = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct ether_hdr) - sizeof(struct ipv4_hdr)));
    }

    if (queue_id == 0) {
        gettimeofday(&startime, NULL);
    }
    core_statistics[core_id].enable = 1;


}

void WriteBatch::Put(const char* key, size_t key_size, const char* value, size_t value_size) {
    assert(payload_len + SET_LEN <= ETHERNET_MAX_FRAME_LEN);

    *(uint16_t *)ptr = MEGA_JOB_SET;
    ptr += sizeof(uint16_t); /* 2 bytes job type */
    *(uint16_t *)ptr = key_size;
    ptr += sizeof(uint16_t); /* 2 bytes key length */
    *(uint32_t *)ptr = value_size;
    ptr += sizeof(uint32_t); /* 4 bytes value length */

    /* 64 bits key */
    if (BITS_INSERT_BUF == 0)
        *(uint32_t *)(ptr + sizeof(uint32_t)) = set_key;
    else
        *(uint32_t *)(ptr + sizeof(uint32_t)) = (rte_bswap32(set_key & 0xff) << (8 - BITS_INSERT_BUF)) | (set_key);
    *(uint32_t *)(ptr) = set_key;

    ptr += KEY_LEN;


    /* Now I copy the value */
    *(uint32_t *)(ptr) = set_key;

    ptr += VALUE_LEN;

    payload_len += SET_LEN;

    set_key ++;
    if (set_key >= preload_cnt) {
        break;
    }
    return;
}

void WriteBatch::Clear() {
    return;
}

void WriteBatch::Delete(const char* key, size_t key_size) {
    return;
}


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
    // In the beginning of the packet:
    // - I have the ethernet header length,
    // - the magic_num
    // - in the end, the end mark
    // EIU_HEADER_LEN + MEGA_MAGIC_NUM_LEN + MEGA_END_MARK_LEN
    size_estimate = EIU_HEADER_LEN + MEGA_MAGIC_NUM_LEN + MEGA_END_MARK_LEN;
}

// this is a SET job type, it just adds to the buffer.
template <typename K, typename V>
void CDBBatch::Write(const K& key, const V& value)
{
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    // leveldb::Slice slKey(ssKey.data(), ssKey.size());

    ssValue.reserve(DBWRAPPER_PREALLOC_VALUE_SIZE);
    ssValue << value;
    ssValue.Xor(dbwrapper_private::GetObfuscateKey(parent));
    // leveldb::Slice slValue(ssValue.data(), ssValue.size());

    batch.Put(ssKey.data(), ssKey.size(), ssValue.data(), ssValue.size());
    // LevelDB serializes writes as:
    // - byte: header
    // - varint: key length (1 byte up to 127B, 2 bytes up to 16383B, ...)
    // - byte[]: key
    // - varint: value length
    // - byte[]: value
    // The formula below assumes the key and value are both less than 16k.
    // size_estimate += 3 + (slKey.size() > 127) + slKey.size() + (slValue.size() > 127) + slValue.size();

    // MegaKV serializes writes as:
    // - 2 bytes job type
    // - 2 bytes key length
    // - 4 bytes value length
    // - then the key
    // - then the value
    size_estimate += 8 + ssKey.size() + ssValue.size();

    ssKey.clear();
    ssValue.clear();
}

// We don't support erase
template <typename K>
void CDBBatch::Erase(const K& key)
{
    ssKey.reserve(DBWRAPPER_PREALLOC_KEY_SIZE);
    ssKey << key;
    // leveldb::Slice slKey(ssKey.data(), ssKey.size());

    batch.Delete(ssKey.data(), ssKey.size());
    // LevelDB serializes erases as:
    // - byte: header
    // - varint: key length
    // - byte[]: key
    // The formula below assumes the key is less than 16kB.
    // size_estimate += 2 + (slKey.size() > 127) + slKey.size();

    // MegaKV does not support deletes, but if we did, they would be in the form:
    // - 2 bytes job type
    // - 2 bytes key length
    // - then the key
    // size_estimate += 4 +_ssKey.size();
    // Since it is not supported, the size_estimate is not increased
    size_estimate += 0;

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
// Here I should actually send the write requests

bool CDBWrapper::WriteBatch(CDBBatch& batch, bool fSync)
{
    const bool log_memory = LogAcceptCategory(BCLog::LEVELDB);
    port = 0;
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
    unsigned int port, ret;
    port = 0;
    assert(qconf->tx_mbufs[queue_id].len == MAX_PKT_BURST);
    ret = rte_eth_tx_burst(port, (uint16_t)queue_id, m_table, (uint16_t)qconf->tx_mbufs[queue_id].len);

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
