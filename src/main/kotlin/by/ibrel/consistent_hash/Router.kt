package by.ibrel.consistent_hash

interface Router<T : Node> {
    fun addNode(node: T, virtualNodeCount: UInt)
    fun removeNode(node: T)
    fun route(key: String): T?
}
