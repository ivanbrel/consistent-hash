package by.ibrel.consistent_hash

import java.util.SortedMap
import java.util.TreeMap


class ConsistentHashRouter<T : Node>(
    private val ring: SortedMap<Long, VirtualNode<T>> = TreeMap(),
    private val hashFunction: HashFunction
) : Router<T> {

    constructor(nodes: Collection<T>, virtualNodeCount: UInt) : this(hashFunction = MD5HashFunction()) {
        nodes.forEach { addNode(it, virtualNodeCount) }
    }

    constructor(nodes: Collection<T>, virtualNodeCount: UInt, hashFunction: HashFunction) : this(hashFunction = hashFunction) {
        nodes.forEach { addNode(it, virtualNodeCount) }
    }

    override fun addNode(node: T, virtualNodeCount: UInt) {
        val existingReplicas: Int = getExistingReplicas(node)
        for (i in 0 until virtualNodeCount.toInt()) {
            val vNode = VirtualNode(node, i + existingReplicas)
            ring[hashFunction.hash(node.key())] = vNode
        }
    }

    override fun removeNode(node: T) {
        val it = ring.keys.iterator()
        while (it.hasNext()) {
            val key = it.next()
            val virtualNode = ring[key]
            if (virtualNode?.isVirtualNodeOf(node) == true) {
                it.remove()
            }
        }
    }

    override fun route(key: String): T? {
        if (ring.isEmpty()) {
            return null
        }
        val hashVal = hashFunction.hash(key)
        val tailMap = ring.tailMap(hashVal)
        val nodeHashVal = if (tailMap.isNotEmpty()) tailMap.firstKey() else ring.firstKey()
        return ring[nodeHashVal]?.getPhysicalNode()
    }

    private fun getExistingReplicas(node: T): Int {
        var replicas = 0
        for (vNode in ring.values) {
            if (vNode.isVirtualNodeOf(node)) {
                replicas++
            }
        }
        println("replicas:$replicas")
        return replicas
    }

}
