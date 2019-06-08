package org.skunkworks.foundationdb.serialization

import kotlinx.serialization.Serializable

@Serializable
data class Data(
        val id: Int,
        @Serializable(with = IX::class) val payload: Payload
)
