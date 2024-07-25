package by.ibrel.consistent_hash

import by.ibrel.consistent_hash.id.SnowflakeIdGeneratorImpl
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicInteger


internal class ConsistentHashRouterTest {

    private val EPOCH_BITS = 41
    private val NODE_ID_BITS = 10
    private val SEQUENCE_BITS = 12
    private val epoch = Instant.now().toEpochMilli();

    @Test
    internal fun distributionNodeGet__given_should() {

        //given
        val nodeOne = TestNode("1:1:0", epoch)
        val nodeTwo = TestNode("1:1:1", epoch)
        val nodeThree = TestNode("1:1:2", epoch)
        val nodeFour = TestNode("1:1:3", epoch)

        var count = 100

        val requests = generateSequence {
            (count--).takeIf { it > 0 }
        }.toList()

        //when

        //then
        val consistentHashRouter = ConsistentHashRouter(listOf(nodeOne, nodeTwo, nodeThree, nodeFour), 10u)
        println(goRoute(consistentHashRouter, requests))

        consistentHashRouter.removeNode(nodeTwo)
        println(goRoute(consistentHashRouter, requests))

        consistentHashRouter.addNode(TestNode("1:1:10", epoch), 5u)
        println(goRoute(consistentHashRouter, requests))
    }

    private fun goRoute(
        consistentHashRouter: Router<TestNode>,
        requests: List<Int>
    ): TreeMap<String, AtomicInteger>? {
        val res = TreeMap<String, AtomicInteger>()
        for (request in requests) {
            val mynode: TestNode? = consistentHashRouter.route(request.toString())
            mynode?.let { res.putIfAbsent(it.key(), AtomicInteger()) }
            res[mynode?.key()]?.incrementAndGet()
            val id = mynode?.nextId()
            println("Node ${mynode?.key()} id:${id?.let { parse(it) }}")
        }
        return res
    }

    private fun parse(id: Long): String {
        val maskNodeId = (1L shl NODE_ID_BITS) - 1 shl SEQUENCE_BITS
        val maskSequence = (1L shl SEQUENCE_BITS) - 1
        val timestamp = (id shr NODE_ID_BITS + SEQUENCE_BITS) + epoch
        val nodeId = id and maskNodeId shr SEQUENCE_BITS
        val sequence = id and maskSequence
        return "time: $timestamp | nodeId: $nodeId | sequence: $sequence"
    }
}

internal class TestNode(private val nodeId: String, epoch: Long) : Node {

    private val idGenerator =
        SnowflakeIdGeneratorImpl(nodeId.hashCode().toULong(), Clock.systemUTC(), epoch)

    override fun key() = nodeId

    override fun nextId() = idGenerator.nextId()

    override fun toString() = "TestNode(nodeId=$nodeId)"
}
