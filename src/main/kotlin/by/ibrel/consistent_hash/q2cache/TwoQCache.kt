package by.ibrel.consistent_hash.q2cache

class TwoQCache<K, V>(maxSize: Int) {
    /**
     * Primary container
     */
    private val map: HashMap<K?, V>

    /**
     * Sets for 2Q algorithm
     */
    private val mapIn: LinkedHashSet<K?>
    private val mapOut: LinkedHashSet<K?>
    private val mapHot: LinkedHashSet<K>
    protected var quarter = .25f

    /**
     * Size of this cache in units. Not necessarily the number of elements.
     */
    //private int size;
    private var sizeIn = 0
    private var sizeOut = 0
    private var sizeHot = 0
    private var maxSizeIn = 0
    private var maxSizeOut = 0
    private var maxSizeHot = 0
    private var putCount = 0
    private var createCount = 0
    private var evictionCount = 0
    private var hitCount = 0
    private var missCount = 0

    /**
     * Two queues cache
     *
     * @param maxSize for caches that do not override [.sizeOf], this is
     * this is the maximum sum of the sizes of the entries in this cache.
     */
    init {
        require(maxSize > 0) { "maxSize <= 0" }
        calcMaxSizes(maxSize)
        map = HashMap(0, 0.75f)
        mapIn = LinkedHashSet()
        mapOut = LinkedHashSet()
        mapHot = LinkedHashSet()
    }

    /**
     * Sets sizes:
     * mapIn  ~ 25% // 1st lvl - store for input keys, FIFO
     * mapOut ~ 50% // 2nd lvl - store for keys goes from input to output, FIFO
     * mapHot ~ 25% // hot lvl - store for keys goes from output to hot, LRU
     *
     * @param maxSize if mapIn/mapOut sizes == 0, works like LRU for mapHot
     */
    private fun calcMaxSizes(maxSize: Int) {
        require(maxSize > 0) { "maxSize <= 0" }
        synchronized(this) {

            //sizes
            maxSizeIn = (maxSize * quarter).toInt()
            maxSizeOut = maxSizeIn * 2
            maxSizeHot = maxSize - maxSizeOut - maxSizeIn
        }
    }

    /**
     * Sets the size of the cache.
     *
     * @param maxSize The new maximum size.
     */
    fun resize(maxSize: Int) {
        calcMaxSizes(maxSize)
        synchronized(this) {
            val copy = HashMap(map)
            evictAll()
            val it: Iterator<K?> = copy.keys.iterator()
            while (it.hasNext()) {
                val key = it.next()
                put(key, copy[key])
            }
        }
    }

    /**
     * Returns the value for `key` if it exists in the cache or can be
     * created by `#create`. If a value was returned, it is moved to the
     * head of the queue. This returns null if a value is not cached and cannot
     * be created.
     */
    operator fun get(key: K?): V? {
        if (key == null) {
            throw NullPointerException("key == null")
        }
        var mapValue: V?
        synchronized(this) {
            mapValue = map[key]
            if (mapValue != null) {
                hitCount++
                if (mapHot.contains(key)) {
                    // add & trim (LRU)
                    mapHot.remove(key)
                    mapHot.add(key)
                } else {
                    if (mapOut.contains(key)) {
                        mapHot.add(key)
                        sizeHot += safeSizeOf(key, mapValue)
                        trimMapHot()
                        sizeOut -= safeSizeOf(key, mapValue)
                        mapOut.remove(key)
                    }
                }
                return mapValue
            }
            missCount++
        }

        /*
         * Attempt to create a value. This may take a long time, and the map
         * may be different when create() returns. If a conflicting value was
         * added to the map while create() was working, we leave that value in
         * the map and release the created value.
         */
        val createdValue = create(key) ?: return null
        synchronized(this) {
            createCount++
            return if (!map.containsKey(key)) {
                // There was no conflict, create
                put(key, createdValue)
            } else {
                map[key]
            }
        }
    }

    /**
     * Caches `value` for `key`.
     *
     * @return the previous value mapped by `key`.
     */
    fun put(key: K?, value: V?): V? {
        if (key == null || value == null) {
            throw NullPointerException("key == null || value == null")
        }
        if (map.containsKey(key)) {
            // if already have - replace it.
            // Cache size may be overheaded at this moment
            synchronized(this) {
                val oldValue = map[key]
                if (mapIn.contains(key)) {
                    sizeIn -= safeSizeOf(key, oldValue)
                    sizeIn += safeSizeOf(key, value)
                }
                if (mapOut.contains(key)) {
                    sizeOut -= safeSizeOf(key, oldValue)
                    sizeOut += safeSizeOf(key, value)
                }
                if (mapHot.contains(key)) {
                    sizeHot -= safeSizeOf(key, oldValue)
                    sizeHot += safeSizeOf(key, value)
                }
            }
            return map.put(key, value)
        }
        var result: V
        synchronized(this) {
            putCount++
            val sizeOfValue = safeSizeOf(key, value)
            //if there are free page slots then put value into a free page slot
            val hasFreeSlot = add2slot(key, safeSizeOf(key, value))
            if (hasFreeSlot) {
                // add 2 free slot & exit
                map[key] = value
                result = value
            } else {
                // no free slot, go to trim mapIn/mapOut
                if (trimMapIn(sizeOfValue)) {
                    //put X into the reclaimed page slot
                    map[key] = value
                    result = value
                } else {
                    map[key] = value
                    mapHot.add(key)
                    sizeHot += safeSizeOf(key, value)
                    trimMapHot()
                    result = value
                }
            }
        }
        return result
    }

    /**
     * Remove items by LRU from mapHot
     */
    fun trimMapHot() {
        while (!(sizeHot <= maxSizeHot || mapHot.isEmpty())) {
            var key: K
            var value: V?
            synchronized(this) {

                check(!(sizeHot < 0 || mapHot.isEmpty() && sizeHot != 0)) {
                    (javaClass.name
                            + ".sizeOf() is reporting inconsistent results!")
                }
//                if (sizeHot <= maxSizeHot || mapHot.isEmpty()) {
//                    return
//                }
                // we add new item before, so next return first (LRU) item
                key = mapHot.iterator().next()
                mapHot.remove(key)
                value = map[key]
                sizeHot -= safeSizeOf(key, value)
                map.remove(key)
                evictionCount++
            }
            entryRemoved(true, key, value, null)
        }
    }

    /**
     * Remove items by FIFO from mapIn & mapOut
     *
     * @param sizeOfValue size of
     * @return boolean is trim
     */
    private fun trimMapIn(sizeOfValue: Int): Boolean {
        var result = false
        if (maxSizeIn < sizeOfValue) {
            return result
        } else {
            while (mapIn.iterator().hasNext()) {
                var keyIn: K?
                var valueIn: V?
                if (!mapIn.iterator().hasNext()) {
                    print("err")
                }
                keyIn = mapIn.iterator().next()
                valueIn = map[keyIn]
                if (sizeIn + sizeOfValue <= maxSizeIn || mapIn.isEmpty()) {
                    //put X into the reclaimed page slot
                    if (keyIn == null) {
                        print("err")
                    }
                    mapIn.add(keyIn)
                    sizeIn += sizeOfValue
                    result = true
                    break
                }
                //page out the tail of mapIn, call it Y
                mapIn.remove(keyIn)
                val removedItemSize = safeSizeOf(keyIn, valueIn)
                sizeIn -= removedItemSize

                // add identifier of Y to the head of mapOut
                while (mapOut.iterator().hasNext()) {
                    var keyOut: K?
                    var valueOut: V?
                    if (sizeOut + removedItemSize <= maxSizeOut || mapOut.isEmpty()) {
                        // put Y into the reclaimed page slot
                        mapOut.add(keyIn)
                        sizeOut += removedItemSize
                        break
                    }
                    //remove identifier of Z from the tail of mapOut
                    keyOut = mapOut.iterator().next()
                    mapOut.remove(keyOut)
                    valueOut = map[keyOut]
                    sizeOut -= safeSizeOf(keyOut, valueOut)
                }
            }
        }
        return result
    }

    /**
     * Check for free slot in any container and add if exists
     *
     * @param key key
     * @param sizeOfValue size
     * @return true if key added
     */
    private fun add2slot(key: K, sizeOfValue: Int): Boolean {
        var hasFreeSlot = false
        if (!hasFreeSlot && maxSizeIn >= sizeIn + sizeOfValue) {
            mapIn.add(key)
            sizeIn += sizeOfValue
            hasFreeSlot = true
        }
        if (!hasFreeSlot && maxSizeOut >= sizeOut + sizeOfValue) {
            mapOut.add(key)
            sizeOut += sizeOfValue
            hasFreeSlot = true
        }
        if (!hasFreeSlot && maxSizeHot >= sizeHot + sizeOfValue) {
            mapHot.add(key)
            sizeHot += sizeOfValue
            hasFreeSlot = true
        }
        return hasFreeSlot
    }

    /**
     * Removes the entry for `key` if it exists.
     *
     * @return the previous value mapped by `key`.
     */
    fun remove(key: K?): V? {
        if (key == null) {
            throw NullPointerException("key == null")
        }
        var previous: V?
        synchronized(this) {
            previous = map.remove(key)
            if (previous != null) {
                if (mapIn.contains(key)) {
                    sizeIn -= safeSizeOf(key, previous)
                    mapIn.remove(key)
                }
                if (mapOut.contains(key)) {
                    sizeOut -= safeSizeOf(key, previous)
                    mapOut.remove(key)
                }
                if (mapHot.contains(key)) {
                    sizeHot -= safeSizeOf(key, previous)
                    mapHot.remove(key)
                }
            }
        }
        if (previous != null) {
            entryRemoved(false, key, previous, null)
        }
        return previous
    }

    /**
     * Called for entries that have been evicted or removed. This method is
     * invoked when a value is evicted to make space, removed by a call to
     * [.remove], or replaced by a call to [.put]. The default
     * implementation does nothing.
     *
     *
     *
     * The method is called without synchronization: other threads may
     * access the cache while this method is executing.
     *
     * @param evicted  true if the entry is being removed to make space, false
     * if the removal was caused by a [.put] or [.remove].
     * @param newValue the new value for `key`, if it exists. If non-null,
     * this removal was caused by a [.put]. Otherwise it was caused by
     * an eviction or a [.remove].
     */
    protected fun entryRemoved(evicted: Boolean, key: K, oldValue: V?, newValue: V?) {}

    /**
     * Called after a cache miss to compute a value for the corresponding key.
     * Returns the computed value or null if no value can be computed. The
     * default implementation returns null.
     *
     *
     *
     * The method is called without synchronization: other threads may
     * access the cache while this method is executing.
     *
     *
     *
     * If a value for `key` exists in the cache when this method
     * returns, the created value will be released with [.entryRemoved]
     * and discarded. This can occur when multiple threads request the same key
     * at the same time (causing multiple values to be created), or when one
     * thread calls [.put] while another is creating a value for the same
     * key.
     */
    protected fun create(key: K): V? {
        return null
    }

    private fun safeSizeOf(key: K?, value: V?): Int {
        val result = sizeOf(key, value)
        check(result >= 0) { "Negative size: $key=$value" }
        return result
    }

    /**
     * Returns the size of the entry for `key` and `value` in
     * user-defined units.  The default implementation returns 1 so that size
     * is the number of entries and max size is the maximum number of entries.
     *
     *
     *
     * An entry's size must not change while it is in the cache.
     */
    protected fun sizeOf(key: K?, value: V?): Int {
        return 1
    }

    /**
     * Clear the cache, calling [.entryRemoved] on each removed entry.
     */
    @Synchronized
    fun evictAll() {
        val it = map.keys.iterator()
        while (it.hasNext()) {
            val key = it.next()
            it.remove()
            remove(key)
        }
        mapIn.clear()
        mapOut.clear()
        mapHot.clear()
        sizeIn = 0
        sizeOut = 0
        sizeHot = 0
    }

    /**
     * For caches that do not override [.sizeOf], this returns the number
     * of entries in the cache. For all other caches, this returns the sum of
     * the sizes of the entries in this cache.
     */
    @Synchronized
    fun size(): Int {
        return sizeIn + sizeOut + sizeHot
    }

    /**
     * For caches that do not override [.sizeOf], this returns the maximum
     * number of entries in the cache. For all other caches, this returns the
     * maximum sum of the sizes of the entries in this cache.
     */
    @Synchronized
    fun maxSize(): Int {
        return maxSizeIn + maxSizeOut + maxSizeHot
    }

    /**
     * Returns the number of times [.get] returned a value that was
     * already present in the cache.
     */
    @Synchronized
    fun hitCount(): Int {
        return hitCount
    }

    /**
     * Returns the number of times [.get] returned null or required a new
     * value to be created.
     */
    @Synchronized
    fun missCount(): Int {
        return missCount
    }

    /**
     * Returns the number of times [.create] returned a value.
     */
    @Synchronized
    fun createCount(): Int {
        return createCount
    }

    /**
     * Returns the number of times [.put] was called.
     */
    @Synchronized
    fun putCount(): Int {
        return putCount
    }

    /**
     * Returns the number of values that have been evicted.
     */
    @Synchronized
    fun evictionCount(): Int {
        return evictionCount
    }

    /**
     * Returns a copy of the current contents of the cache, ordered from least
     * recently accessed to most recently accessed.
     */
    @Synchronized
    fun snapshot(): Map<K?, V> {
        return HashMap(map)
    }

    @Synchronized
    override fun toString(): String {
        val accesses = hitCount + missCount
        val hitPercent = if (accesses != 0) 100 * hitCount / accesses else 0
        return String.format(
            "Cache[size=%d,maxSize=%d,hits=%d,misses=%d,hitRate=%d%%," +
                    "]",
            size(), maxSize(), hitCount, missCount, hitPercent
        ) + "\n map:" + map.toString()
    }

    val mapHotSnapshot: List<Any?>
        get() {
            val result: MutableList<Any?> = ArrayList()
            val it: Iterator<K> = mapHot.iterator()
            while (it.hasNext()) {
                val key = it.next()
                result.add(key)
                result.add(map[key])
            }
            return result
        }
}
