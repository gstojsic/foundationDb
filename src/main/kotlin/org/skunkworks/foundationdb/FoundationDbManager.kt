package org.skunkworks.foundationdb

import com.apple.foundationdb.FDB
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import java.util.concurrent.CompletableFuture


class FoundationDbManager {
    private val fdb = FDB.selectAPIVersion(610)
    private val db = fdb.open()

    suspend fun execute() {
        writeDbAsync("hello", "Daaaaa")

        val bytes = readDb("hello")
        if (bytes === null) {
            println("Key not found")
            return
        }

        val hello = Tuple.fromBytes(bytes).getString(0)
        println("Hello $hello")

        val o = Data(1, Payload("lorem ipsum dolor sit amet"))
        val jsonConfiguration = JsonConfiguration(
                prettyPrint = true
        )
        val json = Json(configuration = jsonConfiguration)
        println(json.stringify(Data.serializer(), o))
    }

    private suspend fun readDb(key: String): ByteArray? = db.readAsync { return@readAsync it[Tuple.from(key).pack()] }.await()
    private suspend fun writeDbAsync(key: String, value: String) =
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

    data class Payload(val content: String)

    @Serializer(forClass = Payload::class)
    object IX
}