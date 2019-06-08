package org.skunkworks.foundationdb

import kotlinx.coroutines.runBlocking

fun main() {
    val manager = FoundationDbManager()
    runBlocking { manager.execute() }
}