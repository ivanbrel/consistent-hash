package by.ibrel.consistent_hash


fun interface HashFunction {
    fun hash(key: String): Long
}
