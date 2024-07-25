package by.ibrel.consistent_hash

interface Node {
    fun key(): String

    fun nextId() : Long
}
