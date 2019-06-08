package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()
    private val serializers = mutableMapOf<String, KSerializer<*>>()
    private val cbor = Cbor()

//    suspend fun execute() {
//        writeDbAsync("hello", "Bananas")
//
//        val o = Data(1, Payload("gorem ipsum dolor sit amet"))
//        write("mellow", o)
//
//        val mellow: Data? = read("mellow")
//
//        println("Mellow $mellow")
//
//        val bytes = readDb("hello")
//        if (bytes === null) {
//            println("Key not found")
//            return
//        }
//        val hello = Tuple.fromBytes(bytes).getString(0)
//        println("Hello $hello")
//    }

//    private suspend fun readDb(key: String): ByteArray? = db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()

    suspend inline fun <reified T : Any> read(key: String) = read(key, T::class)

    suspend fun <T : Any> read(key: String, kClass: KClass<T>): T? {
        val className = kClass.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer(className, kClass)
        return readObjectDb(key, serializer as KSerializer<T?>)
    }

    private fun <T : Any> getSerializer(className: String, kClass: KClass<out T>): KSerializer<*> {
        return serializers.computeIfAbsent(className) { _ ->
            val serializerMethod = kClass.companionObject?.members?.first { it.name == "serializer" }
                    ?: throw RuntimeException("No serializer")
            serializerMethod.call(kClass.companionObjectInstance) as KSerializer<*>
        }
    }

    private suspend fun <T> readObjectDb(key: String, serializer: KSerializer<T>): T? {
        val bytes = readBytes(key)
        return if (bytes === null)
            null
        else cbor.load(serializer, Tuple.fromBytes(bytes).getBytes(0))
    }

    private suspend inline fun readBytes(key: String): ByteArray? =
            db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()

    //    private suspend fun writeDbAsync(key: String, value: String) =
//            db.runAsync {
//                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
//                return@runAsync CompletableFuture.completedFuture(null)
//            }.await()
//
    private suspend fun writeBytesDbAsync(key: String, value: ByteArray): Void? {
        val tr = db.createTransaction()
        tr[Tuple.from(key).pack()] = Tuple.from(value).pack()
        return tr.commit().whenComplete { _, _ -> tr.close() }.await()
    }

    suspend fun <T : Any> write(key: String, o: T): Void? {
        val kClass = o::class
        val className = kClass.simpleName ?: throw RuntimeException("No classname")
        val serializer = getSerializer(className, kClass)
        return writeBytesDbAsync(key, cbor.dump(serializer as KSerializer<T>, o))
    }
}