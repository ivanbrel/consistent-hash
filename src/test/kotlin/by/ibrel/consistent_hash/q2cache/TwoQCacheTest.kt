package by.ibrel.consistent_hash.q2cache

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class TwoQCacheTest {

    private lateinit var testSubject: TwoQCache<Int, String>

    @BeforeEach
    fun setUp() {
        testSubject = TwoQCache(50)
    }

    @Test
    internal fun name__given_should() {

        var count = 100

        val requests = generateSequence {
            (count--).takeIf { it > 0 }
        }.toList()

        requests.forEach { testSubject.put(it, "$it + word") }

    }
}
