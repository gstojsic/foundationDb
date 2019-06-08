package org.skunkworks.foundationdb.serialization

import kotlinx.serialization.Serializer

data class Payload(val content: String)

@Serializer(forClass = Payload::class)
object IX
