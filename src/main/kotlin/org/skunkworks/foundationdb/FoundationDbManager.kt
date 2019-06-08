package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()

    suspend fun execute() {
        writeDbAsync("hello", "Neeeee")

        val bytes = readDb("hello")
        if (bytes === null) {
            println("Key not found")
            return
        }

        val hello = Tuple.fromBytes(bytes).getString(0)
        println("Hello $hello")
    }

    private suspend fun readDb(key: String): ByteArray? = db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()
    private suspend fun writeDbAsync(key: String, value: String) =
            db.runAsync {
                return@runAsync CompletableFuture.supplyAsync {
                    it[Tuple.from(key).pack()] = Tuple.from(value).pack()
                }
            }.await()

    private fun writeDb(key: String, value: String) =
            db.run {
                it[Tuple.from(key).pack()] = Tuple.from(value).pack()
            }

//    @Serializable
//    data class Data(
//            val id: Int,
//            @Serializable val payload: Payload
//    )
//
//    data class Payload(val content: String)

}