package com.github.devsanso.mypipline.conf

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.toml.TomlFactory
import com.fasterxml.jackson.dataformat.toml.TomlMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

data class UrlConf(
    val url : String,
    val user : String,
    val passwd : String
)

data class Conf(val config : UrlConf)

fun load(path : String): Conf {
    val f = File(path)

    val tomlMapper = ObjectMapper(TomlFactory())
        .registerModule(KotlinModule.Builder().build())

    val conf: Conf = tomlMapper.readValue(File(path))
    val data = tomlMapper.readValue(f, Conf::class.java)

    return data
}