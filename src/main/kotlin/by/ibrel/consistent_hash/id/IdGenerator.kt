package by.ibrel.consistent_hash.id

fun interface IdGenerator {

    fun nextId(): Long
}
