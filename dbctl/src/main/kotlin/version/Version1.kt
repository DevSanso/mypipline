package com.github.devsanso.mypipline.version

import com.github.devsanso.mypipline.version.config.ConfigVersion1
import org.jetbrains.exposed.v1.jdbc.Database

class Version1(configDB : Database) : AbsVersion() {
    override val config  = ConfigVersion1(configDB)

    override fun getVersion(): Double = 1.0
}