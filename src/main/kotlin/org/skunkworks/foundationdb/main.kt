package org.skunkworks.foundationdb

import kotlinx.coroutines.runBlocking
import org.skunkworks.foundationdb.serialization.Data
import org.skunkworks.foundationdb.serialization.Payload

fun main() {
    val manager = FoundationDbManager()

    runBlocking {
        val mellowOriginal = Data(1, Payload("gorem ipsum dolor sit amet"))
        manager.write("mellow", mellowOriginal)

        val mellow: Data? = manager.read("mellow")

        println("Mellow $mellow")
    }
}