package org.skunkworks.foundationdb

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import org.skunkworks.foundationdb.serialization.Data
import org.skunkworks.foundationdb.serialization.Payload
import kotlin.system.measureTimeMillis

const val key = "mellow"
const val limit = 100000
//const val limit = 10
val manager = FoundationDbManager()

fun main() {
//    var data : List<Data?> = emptyList()
//    val time = measureTimeMillis {
//        data = (1..limit).map { i ->
//            val k = "$key-$i"
//            val mellowOriginal = Data(i, Payload("gorem ipsum dolor sit amet $i"))
//            manager.writeBlocking(k, mellowOriginal)
//
//            manager.readBlocking(k) as Data?
//        }
//    }
//    println("Data: $data")
//    println("Time: $time")

    runBlocking {
        //manager.clear("$key-1", "$key-$limit")

        val timeErase = measureTimeMillis {
            (1..limit).map { i ->
                val k = "$key-$i"
                async { manager.clear(k) }
            }.joinAll()
        }
        println("Time erase: $timeErase")

//        var data: List<Data?> = readData(manager)
//        println("Count after erase: ${data.count()}")

        val timeWrite = measureTimeMillis {
            (1..limit).map { i ->
                val k = "$key-$i"
                val mellowOriginal = Data(i, Payload("gorem ipsum dolor sit amet $i"))
//                val mellowOriginal = "gorem ipsum dolor sit amet $i"
                async { manager.write(k, mellowOriginal) }
            }.joinAll()
        }
        println("Time write: $timeWrite")
        println("Data write/sec: ${limit.toDouble() * 1000 / timeWrite.toDouble()}")

        var data: List<Data?> = emptyList()
//        var data: List<String?> = emptyList()
        val timeRead = measureTimeMillis {
            data = readData(manager)
        }

        println("Data: $data")
        println("Data size: ${data.count()}")
        println("Time read: $timeRead")
        println("Data read/sec: ${data.count().toDouble() * 1000 / timeRead.toDouble()}")
        println("Total time: ${timeRead + timeWrite}")
    }
}

private suspend fun readData(manager: FoundationDbManager): List<Data?> {
//private suspend fun readData(manager: FoundationDbManager): List<String?> {
    val jobs = (1..limit).map { i ->
        val k = "$key-$i"
        coroutineScope { async { manager.read(k) as Data? } }
//        coroutineScope { async { manager.read(k) as String? } }
    }
    jobs.joinAll()
    return jobs.map { it.await() }.filter { it !== null }
}