package by.ibrel.consistent_hash

class VirtualNode<T : Node>(private val physicalNode: T, private val replicaIndex: Int) : Node {

    override fun key() = "${physicalNode.key()}-${replicaIndex}"

    override fun nextId() = physicalNode.nextId()

    fun isVirtualNodeOf(pNode: T) = physicalNode.key() == pNode.key()

    fun getPhysicalNode(): T = physicalNode
}
