package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()
    private val serializers = mutableMapOf<String, KSerializer<*>>()
    private val cbor = Cbor()

    init {
        db.options().also {
            it.setTransactionTimeout(3000)
            it.setTransactionRetryLimit(5)
        }
    }

    suspend inline fun <reified T : Any> read(key: String) = read(key, T::class)

    @Suppress("UNCHECKED_CAST")
    suspend fun <T : Any> read(key: String, kClass: KClass<T>): T? {
        val bytes = readBytes(key)
        if (bytes === null)
            return null
        //Tuple.fromBytes(Tuple.fromBytes(bytes).getBytes(0)).getString(0)
        return when (kClass) {
            String::class -> Tuple.fromBytes(bytes).getString(0) as T
            Int::class -> decodeInt(Tuple.fromBytes(bytes).getBytes(0)) as T
            Long::class -> Tuple.fromBytes(bytes).getLong(0) as T
            Boolean::class -> Tuple.fromBytes(bytes).getBoolean(0) as T
            Double::class -> Tuple.fromBytes(bytes).getDouble(0) as T
            else -> {
                val className = kClass.simpleName ?: throw RuntimeException("No classname")
                val serializer = getSerializer(className, kClass)
                deserializeObject(bytes, serializer as KSerializer<T?>)
            }
        }
    }

    private fun <T : Any> getSerializer(className: String, kClass: KClass<out T>): KSerializer<*> {
        return serializers.computeIfAbsent(className) { _ ->
            val serializerMethod = kClass.companionObject?.members?.first { it.name == "serializer" }
                    ?: throw RuntimeException("No serializer")
            serializerMethod.call(kClass.companionObjectInstance) as KSerializer<*>
        }
    }

    private fun <T> deserializeObject(bytes: ByteArray?, serializer: KSerializer<T>): T? {
        return if (bytes === null)
            null
        else cbor.load(serializer, bytes)
    }

    private suspend fun readBytes(key: String): ByteArray? {
        return readWithRetry { it[Tuple.from(key).pack()] }
    }

    private suspend fun readWithRetry(block: (Transaction) -> CompletableFuture<ByteArray?>): ByteArray? {
        db.createTransaction().use { tr ->
            var t = tr
            var res: ByteArray?
            while (true) {
                try {
                    res = block(t).await()
                } catch (e: RuntimeException) {
                    t = t.onError(e).await()
                    continue
                }
                return res
            }
        }
        return null
    }

    private suspend fun writeBytesDbAsync(key: String, value: ByteArray) {
        updateWithRetry { it[Tuple.from(key).pack()] = value }
    }

    @Suppress("UNCHECKED_CAST")
    suspend fun <T : Any> write(key: String, o: T?) {
        if (o === null) {
            writeBytesDbAsync(key, byteArrayOf())
            return
        }

        val bytes = when (val kClass = o::class) {
            String::class, Long::class, Boolean::class, Double::class -> Tuple.from(o).pack()
            Int::class -> encodeInt(o as Int)
            else -> {
                val className = kClass.simpleName ?: throw RuntimeException("No classname")
                val serializer = getSerializer(className, kClass)
                cbor.dump(serializer as KSerializer<T>, o)
            }
        }
        writeBytesDbAsync(key, bytes)
    }

    suspend fun clear(key: String) {
        updateWithRetry { it.clear(Tuple.from(key).pack()) }
    }

    private suspend fun updateWithRetry(block: (Transaction) -> Unit) {
        db.createTransaction().use { tr ->
            var t = tr
            while (true) {
                try {
                    //t.clear(Tuple.from(key).pack())
                    block(t)
                    t.commit().await()
                    break
                } catch (e: RuntimeException) {
                    t = t.onError(e).await()
                }
            }
        }
    }

    suspend fun clear(startKey: String, endKey: String) {
        updateWithRetry { it.clear(Tuple.from(startKey).pack(), Tuple.from(endKey).pack()) }
    }

    inline fun <reified T : Any> readBlocking(key: String) = readBlocking(key, T::class)

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> readBlocking(key: String, kClass: KClass<T>): T? {
        val className = kClass.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer(className, kClass)
        return readObjectBlocking(key, serializer as KSerializer<T?>)
    }

    private fun <T> readObjectBlocking(key: String, serializer: KSerializer<T>): T? {
        val bytes = readBytesBlocking(key)
        return if (bytes === null)
            null
        else cbor.load(serializer, Tuple.fromBytes(bytes).getBytes(0))
    }

    private fun readBytesBlocking(key: String): ByteArray? =
            db.read { return@read it[Tuple.from(key).pack()] }.join()

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> writeBlocking(key: String, o: T) {
        val kClass = o::class
        val className = kClass.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer(className, kClass)
        writeBytesBlock(key, cbor.dump(serializer as KSerializer<T>, o))
    }

    private fun writeBytesBlock(key: String, value: ByteArray) {
        db.createTransaction().use { tr ->
            tr[Tuple.from(key).pack()] = Tuple.from(value).pack()
            tr.commit().join()
        }
    }

    private fun encodeInt(value: Int): ByteArray {
        val output = ByteArray(4)
        ByteBuffer.wrap(output).putInt(value)
        return output
    }

    private fun decodeInt(value: ByteArray): Int {
        if (value.size != 4)
            throw IllegalArgumentException("Array must be of size 4")
        return ByteBuffer.wrap(value).int
    }
}