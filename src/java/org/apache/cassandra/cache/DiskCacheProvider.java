package org.apache.cassandra.cache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.cache.CapiRowCacheProvider.HashFunction;
import org.apache.cassandra.cache.SerializingCacheProvider.RowCacheSerializer;
import org.apache.cassandra.cache.capi.CapiChunkDriver;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class DiskCacheProvider implements org.apache.cassandra.cache.CacheProvider<RowCacheKey, IRowCacheEntry> {
    private static final Logger logger = LoggerFactory.getLogger(DiskCacheProvider.class);

    public static final int ENTRY_SIZE = 4096;

    public static final int DEFAULT_L2CACHE = 128 * 1024 * 1024;
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    public static final String PROP_DIR = "odirect.dir";
    public static final long DEFAULT_SIZE_IN_GB_BYTES = 1L; // 1 gb

    public static AtomicLong touched = new AtomicLong();
    public static AtomicLong cacheHit = new AtomicLong();
    public static AtomicLong cacheMiss = new AtomicLong();
    public static AtomicLong cachePush = new AtomicLong();
    public static AtomicLong swapin = new AtomicLong();
    public static AtomicLong swapinMiss = new AtomicLong();
    public static AtomicLong swapinErr = new AtomicLong();
    public static AtomicLong remove = new AtomicLong();

    public interface LibC extends Library {
        //int read(int fd, byte[] buf, int count);
        int read(int fd, ByteBuffer buf, int count);

        int write(int fd, ByteBuffer buf, int count);

        int fsync(int fd);

        int open(String path, int flags, int mode);

        int open(String path, int flags);

        int close(int fd);

        int lseek(int fd, long offset, int whence);

        int posix_memalign(PointerByReference pp, long alignment, long size);

        void free(Pointer p);

        String strerror(int errnum);

    }

    public static final LibC LIBC = (LibC) Native.loadLibrary(Platform.C_LIBRARY_NAME, LibC.class);

    private static final int O_RDONLY = 00000000;
    private static final int O_WRONLY = 00000001;
    private static final int O_RDWR = 00000002;
    private static final int O_DIRECT;
    private static final int O_SYNC = 04000000;
    private static final int SEEK_SET = 0;
    private static final int S_IRUSR = 0000400;
    private static final int S_IRWXU = 0000700;

    static {
        Native.register(Platform.C_LIBRARY_NAME);

        if ("ppc64le".equals(System.getProperty("os.arch")))
            O_DIRECT = 00400000;
        else
            O_DIRECT = 00040000;
    }

    public static native void free(Pointer p);

    private static native int open(String pathname, int flags, int mode);

    public native int close(int fd);

    private static native NativeLong lseek(int fd, NativeLong offset, int whence);

    private static native NativeLong read(int fd, Pointer buf, NativeLong count);

    private static native NativeLong write(int fd, Pointer buf, NativeLong count);

    private static native String strerror(int errnum);

    private static native int fsync(int fd, Pointer buf, NativeLong count);

    public static int openDirect(String pathname) throws IOException {
        int ret = LIBC.open(pathname, O_RDWR | O_DIRECT, S_IRWXU);

        if (ret < 0)
            throw new IOException("open error: " + pathname + ", " + LIBC.strerror(Native.getLastError()));

        return ret;
    }

    @Override
    public ICache<RowCacheKey, IRowCacheEntry> create() {
        return new FileRowCache(DatabaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024);
    }

    public static class FileRowCache implements ICache<RowCacheKey, IRowCacheEntry> {

        private final Cache<RowCacheKey, IRowCacheEntry> map;
        private final ISerializer<IRowCacheEntry> serializer = new RowCacheSerializer();
        final HashFunction hashFunc;
        final AtomicInteger size = new AtomicInteger();
        final int[] filters;
        final ReentrantLock[] monitors = new ReentrantLock[Runtime.getRuntime().availableProcessors() * 64];
        final int[] fds = new int[Runtime.getRuntime().availableProcessors() * 64];
        final long numOfBlocks;
        final long numOfBlocksForEach;
        final long sizeInBytes;

        FileRowCache(long capacity) {
            this.map = Caffeine.newBuilder().weigher(new Weigher<RowCacheKey, IRowCacheEntry>(){
				@Override
				public int weigh(RowCacheKey key, IRowCacheEntry value) {
					 long serializedSize = serializer.serializedSize(value);
	                    if (serializedSize > Integer.MAX_VALUE)
	                        throw new IllegalArgumentException("Unable to allocate " + serializedSize + " bytes");

	                    return (int) serializedSize;
				}
            }).maximumWeight(DEFAULT_L2CACHE).build();
            
            String hashClass = System.getProperty("capi.hash");
            try {
                hashFunc = hashClass == null ? new HashFunction() {
                    public int hashCode(byte[] key) {
                        return Arrays.hashCode(key);
                    }

                    @Override
                    public String toString(byte[] bb) {
                        return "unknown";
                    }
                } : (HashFunction) Class.forName(hashClass).newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            // -Dodirect.dir=/home/horii/filename:<GB_SIZE>
            String dirNameSize = System.getProperty(PROP_DIR);
            if (dirNameSize == null) {
                logger.error("format error in " + PROP_DIR + ": " + dirNameSize);
                throw new IllegalStateException("no valid device name in " + PROP_DIR);
            }

            String[] dirNameSizes = dirNameSize.split(":");
            if (dirNameSizes.length != 2) {
                logger.error("format error in " + PROP_DIR + ": " + dirNameSize);
                throw new IllegalStateException("no valid device name in " + PROP_DIR);
            }

            String dirName = dirNameSizes[0];
            sizeInBytes = 1024L * 1024L * 1024L * Long.parseLong(dirNameSizes[1]);
            long sizeForEachTmp = sizeInBytes / monitors.length;
            if (sizeInBytes % monitors.length != 0)
                sizeForEachTmp++;

            numOfBlocks = sizeInBytes / ENTRY_SIZE;
            long maskSize = numOfBlocks / 32L + (numOfBlocks % 32L == 0 ? 0 : 1);
            filters = new int[(int) maskSize];

            int numOfFiles = monitors.length;
            numOfBlocksForEach = numOfBlocks / numOfFiles + 1;

            sizeForEachTmp = (sizeForEachTmp / ENTRY_SIZE) * ENTRY_SIZE;
            if (sizeForEachTmp % ENTRY_SIZE != 0)
                sizeForEachTmp += ENTRY_SIZE;

            File[] files = new File[monitors.length];
            for (int i = 0; i < numOfFiles; ++i) {
                files[i] = new File(dirName + "/odirect" + i);
                try {
                    RandomAccessFile rFile = new RandomAccessFile(files[i], "rw");
                    rFile.setLength(numOfBlocksForEach * ENTRY_SIZE);
                    rFile.close();
                } catch (Exception e) {
                    throw new IllegalStateException("no valid device name in " + PROP_DIR, e);
                }
            }

            try {
                for (int i = 0; i < fds.length; ++i) {
                    fds[i] = (openDirect(files[i].getAbsolutePath()));
                }
            } catch (IOException e) {
                throw new IllegalStateException("no valid device name in " + PROP_DIR, e);
            }

            for (int i = 0; i < monitors.length; ++i)
                monitors[i] = new ReentrantLock();

        }

        public int hashCode(RowCacheKey key) {
            int result = key.tableId.hashCode();
            result = 31 * result + (key != null ? hashFunc.hashCode(key.key) : 0);
            return result;
        }

        public boolean equals(RowCacheKey key, ByteBuffer keyBB) {
        	TableId tableId = TableId.fromUUID(new UUID(keyBB.getLong(), keyBB.getLong()));
            if (!tableId.equals(key.tableId))
                return false;
            if (!readString(keyBB).equals(key.indexName))
                return false;
            if (keyBB.remaining() != key.key.length)
                return false;

            for (int i = 0; i < key.key.length; ++i)
                if (key.key[i] != keyBB.get())
                    return false;
            return true;
        }

        public void serializeKey(RowCacheKey key, ByteBuffer bb) {
            try {
                // logger.info("serializeKey=" + bb + ": " + key);
                bb.putLong(key.tableId.asUUID().getMostSignificantBits());
                bb.putLong(key.tableId.asUUID().getLeastSignificantBits());
                writeString(bb, key.indexName);
                bb.put(key.key);
            } catch (BufferOverflowException ex) {
                logger.error("ideal: " + (key.tableId.serializedSize() + getByteSizeForString(key.indexName) + key.key.length) + ", actual=" + bb.capacity());
                throw ex;
            }
        }

        public void serializeValue(IRowCacheEntry value, ByteBuffer bb) {
            try {
                serializer.serialize(value, new DataOutputBufferFixed(bb));
            } catch (IOException e) {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }

        public void writeString(ByteBuffer bb, String str) {
            byte[] bytes = str.getBytes();
            bb.putInt(bytes.length);
            bb.put(bytes);
        }

        public String readString(ByteBuffer bb) {
            int length = bb.getInt();
            byte[] bytes = new byte[length];
            bb.get(bytes);
            return new String(bytes);
        }

        public int getByteSizeForString(String str) {
            return str.getBytes().length + 4;
        }

        public RowCacheKey deserializeKey(ByteBuffer bb) {
            TableId tableId = TableId.fromUUID(new UUID(bb.getLong(), bb.getLong()));
            String indexName = readString(bb);
           
            ByteBuffer keyBody = ByteBuffer.allocateDirect(bb.remaining());
            keyBody.put(bb);
            return new RowCacheKey(tableId, indexName, keyBody);
        }

        public IRowCacheEntry deserializeValue(ByteBuffer bb) {
            try {
                return serializer.deserialize(new DataInputBuffer(bb, false));
            } catch (IOException e) {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }

        public int keySize(RowCacheKey k) {
        	 return k.tableId.serializedSize() + getByteSizeForString(k.indexName) + k.key.length;
        }

        public int valueSize(IRowCacheEntry v) {
            int size = (int) serializer.serializedSize(v);

            if (size == Integer.MAX_VALUE)
                throw new IllegalStateException();

            return size + 1;
        }

        void lock(int hash) {
            hash = Math.abs(hash);
            ReentrantLock monitor = monitors[hash % monitors.length];
            monitor.lock();
        }

        void unlock(int hash) {
            hash = Math.abs(hash);
            ReentrantLock monitor = monitors[hash % monitors.length];
            monitor.unlock();
        }

        int fd(int hash) {
            int idx = (int) (Math.abs(hash) / numOfBlocksForEach);
            return fds[idx];
        }

        int pos(int hash) {
            hash = Math.abs(hash);
            return (int) (hash % numOfBlocksForEach) * ENTRY_SIZE;
        }

        boolean exist(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash % filters.length;
                int maxPos = hash % 32;
                int mask = filters[maskIdx];
                int maskFilter = 1 << maxPos;
                return (mask & maskFilter) != 0;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        void filled(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash % filters.length;
                int maxPos = hash % 32;
                int maskFilter = 1 << maxPos;
                filters[maskIdx] |= maskFilter;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        void invalidate(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash % filters.length;
                int maxPos = hash % 32;
                int maskFilter = 1 << maxPos;
                filters[maskIdx] &= ~maskFilter;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        long getLBA(int hash) {
            return Math.abs(hash) % numOfBlocks;
        }

        @Override
        public long capacity() {
            return sizeInBytes;
        }

        @Override
        public void setCapacity(long capacity) {
            logger.warn("setCapacity is not supportted.");
        }

        @Override
        public int size() {
            return size.get();
        }

        @Override
        public long weightedSize() {
            return map.estimatedSize();
        }

        @Override
        public void put(RowCacheKey key, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                putToFile(key, hash, value, false);
                map.put(key, value);
                filled(hash, false);
                cachePush.incrementAndGet();

            } finally {
                unlock(hash);
            }
        }

        private void putToFile(RowCacheKey key, int hash, IRowCacheEntry value, boolean withLock) {
            int keySize = keySize(key);
            int valueSize = valueSize(value);

            if (keySize + valueSize > ENTRY_SIZE - 8)
                return;

            ByteBuffer bb = ByteBuffer.allocateDirect(ENTRY_SIZE);
            bb.putInt(keySize);
            bb.putInt(valueSize);
            serializeKey(key, bb);
            serializeValue(value, bb);

            if (withLock)
                lock(hash);

            try {
                bb.rewind();
                int fd = fd(hash);
                LIBC.lseek(fd, pos(hash), SEEK_SET);
                int ret;
                if ((ret = LIBC.write(fd, bb, ENTRY_SIZE)) != ENTRY_SIZE) {
                    logger.warn("write error");
                    throw new Exception("write error: msg=" + LIBC.strerror(Native.getLastError()) + ", fd=" + fd + ", pos=" + pos(hash) + ", ret=" + ret);
                }
                LIBC.fsync(fd);

            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                return;
            } finally {
                if (withLock)
                    unlock(hash);
            }

        }

        @Override
        public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (exist(hash, false))
                    return false;

                map.put(key, value);
                putToFile(key, hash, value, false);
                filled(hash, false);

                cachePush.incrementAndGet();

                return true;
            } finally {
                unlock(hash);
            }
        }

        @Override
        public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (!exist(hash, false))
                    return false;

                IRowCacheEntry current = map.getIfPresent(key);
                if (current == null) {
                    current = getFromFile(key, hash);
                    if (current == null) {
                        logger.error("filter is not consistent.");
                        return false;
                    }
                }

                map.put(key, value);
                putToFile(key, hash, value, false);
                //filled(hash, false);

                cachePush.incrementAndGet();

                return true;
            } finally {
                unlock(hash);
            }
        }

        IRowCacheEntry getFromFile(RowCacheKey key, int hash) {
            ByteBuffer keyAndValueBB = ByteBuffer.allocateDirect(ENTRY_SIZE);
            try {
                int fd = fd(hash);
                LIBC.lseek(fd, pos(hash), SEEK_SET);
                LIBC.read(fd, keyAndValueBB, ENTRY_SIZE);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return null;
            }

            int keySizeInEntry = keyAndValueBB.getInt(); // header 1
            int valueSizeInEntry = keyAndValueBB.getInt(); // header 2

            int keySize = keySize(key);
            if (keySize != keySizeInEntry) {
                //logger.info("key size is different.: arg=" + keySize + ", cache=" + keySizeInEntry);
                return null;
            }

            ByteBuffer keyBB = ByteBuffer.allocateDirect(keySize);
            serializeKey(key, keyBB);
            keyBB.rewind();

            ByteBuffer keyBBInEntry = ((ByteBuffer) keyAndValueBB.limit(keySize + 8)).slice();
            keyBBInEntry.rewind();

            if (keyBB.equals(keyBBInEntry)) {
                keyAndValueBB.rewind().position(keySize + 8).limit(keySize + valueSizeInEntry + 8);
                ByteBuffer valueBBInEntry = keyAndValueBB.slice();
                return deserializeValue(valueBBInEntry);
            } else {
                //logger.info("key value is different.: arg=" + keyBB.capacity() + ", cache=" + keyBBInEntry.capacity());
                return null;
            }
        }

        @Override
        public IRowCacheEntry get(RowCacheKey key) {

            int hash = hashFunc.hashCode(key.key);

            IRowCacheEntry entry = map.getIfPresent(key);
            if (entry == null) {
                if (exist(hash, false)) {
                    entry = getFromFile(key, hash);
                    if (entry != null)
                        swapin.incrementAndGet();
                    else
                        swapinMiss.incrementAndGet();
                }
            }

            if (entry == null) {
                if (cacheMiss.incrementAndGet() % logUnit == 0)
                    log();
            } else {
                if (cacheHit.incrementAndGet() % logUnit == 0)
                    log();
            }
            return entry;
        }

        @Override
        public void remove(RowCacheKey key) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (!exist(hash, false))
                    return;

                invalidate(hash, false);

            } finally {
                unlock(hash);
            }
        }

        @Override
        public void clear() {
            for (ReentrantLock lock : monitors)
                lock.lock();
            try {
                for (int i = 0; i < filters.length; ++i)
                    filters[i] = 0;
            } finally {
                for (ReentrantLock lock : monitors)
                    lock.unlock();
            }
        }

        @Override
        public Iterator<RowCacheKey> keyIterator() {
            clear();
            return new Iterator<RowCacheKey>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public RowCacheKey next() {
                    return null;
                }
            };
        }

        @Override
        public Iterator<RowCacheKey> hotKeyIterator(int n) {
            return new Iterator<RowCacheKey>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public RowCacheKey next() {
                    return null;
                }
            };
        }

        @Override
        public boolean containsKey(RowCacheKey key) {
            return get(key) != null;
        }

    }

    static long logUnit = 100000L;
    static long lastLog = System.currentTimeMillis();
    static long lastFileRead = 0L;
    static long lastFileRowRead = 0L;

    static synchronized void log() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastLog;
        lastLog = now;

        long currentFileRead = swapin.get() + swapinMiss.get();
        long count = currentFileRead - lastFileRead;
        lastFileRead = currentFileRead;

        long currentFileRowRead = CapiChunkDriver.executed.get();
        long rowCount = currentFileRowRead - lastFileRowRead;
        lastFileRowRead = currentFileRowRead;

        logger.info("cache hit/miss/push : " + cacheHit + "/" + cacheMiss + "/" + cachePush + ", "//
                + "total swapped-in (success/miss/error/remove) : " + swapin + "/" + swapinMiss + "/" + swapinErr + "/" + remove + ", throughput (cache/capi): " + (count / (double) elapsed) * 1000.0 + "/" + (rowCount / (double) elapsed) * 1000.0);
    }

}
