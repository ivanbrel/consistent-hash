package by.ibrel.consistent_hash.id

import java.time.Clock
import java.time.Instant


class SnowflakeIdGeneratorImpl(private val nodeId: ULong, private val clock: Clock, private val customEpoch: Long) :
    IdGenerator {

    private val UNUSED_BITS = 1 // Sign bit, Unused (always set to 0)

    private val EPOCH_BITS = 41
    private val NODE_ID_BITS = 10
    private val SEQUENCE_BITS = 12

    private val maxSequence = (1L shl SEQUENCE_BITS) - 1

    @Volatile
    private var lastTimestamp = -1L

    @Volatile
    private var sequence = 0L

    @Synchronized
    override fun nextId(): Long {
        var currentTimestamp = timestamp()
        check(currentTimestamp >= lastTimestamp) { "Invalid System Clock!" }
        if (currentTimestamp == lastTimestamp) {
            sequence = ++sequence and maxSequence
            if (sequence == 0L) {
                // Sequence Exhausted, wait till next millisecond.
                currentTimestamp = waitNextMillis(currentTimestamp)
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0
        }
        lastTimestamp = currentTimestamp
        return (currentTimestamp shl NODE_ID_BITS + SEQUENCE_BITS or (nodeId.toLong() shl SEQUENCE_BITS)
                or sequence)
    }


    // Get current timestamp in milliseconds, adjust for the custom epoch.
    private fun timestamp(): Long {
        return Instant.now(clock).toEpochMilli() - customEpoch
    }

    // Block and wait till next millisecond
    private fun waitNextMillis(currentTimestamp: Long): Long {
        var currentTimestamp = currentTimestamp
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestamp()
        }
        return currentTimestamp
    }

    override fun toString(): String {
        return ("Snowflake Settings [EPOCH_BITS=" + EPOCH_BITS + ", NODE_ID_BITS=" + NODE_ID_BITS
                + ", SEQUENCE_BITS=" + SEQUENCE_BITS + ", CUSTOM_EPOCH=" + customEpoch
                + ", NodeId=" + nodeId + "]")
    }
}
