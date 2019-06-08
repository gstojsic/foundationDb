package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.cbor.Cbor
import java.util.concurrent.CompletableFuture
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()
    private val cbor = Cbor()

    suspend fun execute() {
        writeDbAsync("hello", "Daaaaa")

        val o = Data(1, Payload("lorem ipsum dolor sit amet"))
        val dumpBytes = cbor.dump(Data.serializer(), o)
        writeBytesDbAsync("mellow", dumpBytes)

//        val dta: Data? = readObjectDb("mellow", Data.Companion.serializer())
        val dta: Data? = readObjectDb("mellow")

        val bytes = readDb("hello")
        if (bytes === null) {
            println("Key not found")
            return
        }
        val hello = Tuple.fromBytes(bytes).getString(0)
        println("Hello $hello")
    }

    private suspend fun readDb(key: String): ByteArray? = db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()
    //    private suspend inline fun <reified T: KSerializer<T>> readObjectDb(key: String): T? =

    private suspend inline fun <reified T> readObjectDb(key: String): T? {
        val serializer = T::class.companionObject?.members?.first { it.name == "serializer" }
                ?: throw RuntimeException("No serializer")

        println(serializer)

        println(serializer.call(T::class.companionObjectInstance))

        return readObjectDb(key, serializer.call(T::class.companionObjectInstance) as KSerializer<T?>)
    }

    private suspend inline fun <T> readObjectDb(key: String, serializer: KSerializer<T>): T? =
            db.readAsync { return@readAsync it[Tuple.from(key).pack()] }
                    .thenApply { if (it === null) null else cbor.load(serializer, Tuple.fromBytes(it).getBytes(0)) }.await()

//    private suspend inline fun <reified T> readObjectDb(key: String): T? = db.readAsync {
//        val bytes = it[Tuple.from(key).pack()]
//        return@readAsync cbor.load(Data.serializer(), bytes)
//    }.await()

    private suspend fun writeDbAsync(key: String, value: String) =
            db.runAsync {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
                return@runAsync CompletableFuture.completedFuture(null)
            }.await()

    private suspend fun writeBytesDbAsync(key: String, value: ByteArray) =
            db.runAsync {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
                return@runAsync CompletableFuture.completedFuture(null)
            }.await()

    private fun writeDb(key: String, value: String) =
            db.run {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
            }

    @Serializable
    data class Data(
            val id: Int,
            @Serializable(with = IX::class) val payload: Payload
//            @ContextualSerialization val payload: Payload
    )
    //) : KSerializer<Data> by serializer()

    data class Payload(val content: String)

    @Serializer(forClass = Payload::class)
    object IX
}