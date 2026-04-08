package com.github.devsanso.mypipline.version

interface IVersion {
    fun getVersion(): Double
    fun upgrade()
    fun downgrade()
}