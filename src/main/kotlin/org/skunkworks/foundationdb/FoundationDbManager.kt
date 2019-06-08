package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import org.skunkworks.foundationdb.serialization.Data
import org.skunkworks.foundationdb.serialization.Payload
import java.util.concurrent.CompletableFuture
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()
    private val serializers = mutableMapOf<String, KSerializer<*>>()
    private val cbor = Cbor()

    suspend fun execute() {
        writeDbAsync("hello", "Daaaaa")

        val o = Data(1, Payload("gorem ipsum dolor sit amet"))
        writeObjectDb("mellow", o)

        val mellow: Data? = readObjectDb("mellow")

        println("Mellow $mellow")

        val bytes = readDb("hello")
        if (bytes === null) {
            println("Key not found")
            return
        }
        val hello = Tuple.fromBytes(bytes).getString(0)
        println("Hello $hello")
    }

    private suspend fun readDb(key: String): ByteArray? = db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()

    private suspend inline fun <reified T> readObjectDb(key: String): T? {
        val className = T::class.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer<T>(className)
        return readObjectDb(key, serializer as KSerializer<T?>)
    }

    private inline fun <reified T> getSerializer(className: String): KSerializer<*> {
        return serializers.computeIfAbsent(className) { _ ->
            val serializerMethod = T::class.companionObject?.members?.first { it.name == "serializer" }
                    ?: throw RuntimeException("No serializer")
            serializerMethod.call(T::class.companionObjectInstance) as KSerializer<*>
        }
    }

    private suspend inline fun <T> readObjectDb(key: String, serializer: KSerializer<T>): T? {
        val bytes = readBytes(key)
        return if (bytes === null)
            null
        else cbor.load(serializer, Tuple.fromBytes(bytes).getBytes(0))
    }

    private suspend inline fun readBytes(key: String): ByteArray? =
            db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()

    private suspend fun writeDbAsync(key: String, value: String) =
            db.runAsync {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
                return@runAsync CompletableFuture.completedFuture(null)
            }.await()

    private suspend fun writeBytesDb(key: String, value: ByteArray) =
            db.runAsync {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
                return@runAsync CompletableFuture.completedFuture(null)
            }.await()

    private suspend fun writeBytesDbAsync(key: String, value: ByteArray): Void? {
        val tr = db.createTransaction()
        tr[Tuple.from(key).pack()] = Tuple.from(value).pack()
        return tr.commit().whenComplete { _, _ -> tr.close() }.await()
    }

    private suspend inline fun <reified T> writeObjectDb(key: String, o: T): Void? {
        val className = T::class.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer<T>(className)
        return writeBytesDbAsync(key, cbor.dump(serializer as KSerializer<T>, o))
    }
}