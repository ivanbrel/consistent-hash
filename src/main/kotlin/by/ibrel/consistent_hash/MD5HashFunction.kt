package by.ibrel.consistent_hash

import java.security.MessageDigest

class MD5HashFunction(private val md: MessageDigest = MessageDigest.getInstance("MD5")) : HashFunction {
    override fun hash(key: String): Long {
        md.reset()
        md.update(key.toByteArray())
        val digest: ByteArray = md.digest()

        var h: Long = 0
        for (i in 0..3) {
            h = h shl 8
            h = h or (digest[i].toInt() and 0xFF).toLong()
        }
        return h
    }
}
