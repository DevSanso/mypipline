package com.github.devsanso.mypipline

import com.github.devsanso.mypipline.conf.load
import com.github.devsanso.mypipline.version.AbsVersion
import com.github.devsanso.mypipline.version.Version1
import org.jetbrains.exposed.v1.jdbc.Database
import java.net.InetSocketAddress
import java.net.URI
import java.util.SortedMap
import kotlin.reflect.KClass
import kotlin.system.exitProcess

val versionMap : SortedMap<Double, KClass<out AbsVersion>> = sortedMapOf(
    1.0 to Version1::class,
)

fun install(configDB : Database) {
    val constructor = versionMap[1.0]?.constructors?.first()
    val ver = constructor?.call(configDB);

    ver?.upgrade()
}

fun main(args: Array<String>) {
    lateinit var configDB : Database

    if (args.size < 2) {
        println("args count not match ${args.size}")
        return
    }

    try {
        val conf = load(args[1])
        configDB = Database.connect(url = conf.config.url, user = conf.config.user, password = conf.config.passwd)
    } catch(e: Exception) {
        println("FAIL: init jdbc connection")
        println(e.message)
        exitProcess(2)
    }

    try {
        when (args[0]) {
            "install" -> install(configDB)
            else -> throw Exception("unknown option ${args[0]}")
        }
    } catch(e: Exception) {
        println("FAIL: install error")
        println(e.message)
        println(e.stackTrace.joinToString("\n"))
    }
}