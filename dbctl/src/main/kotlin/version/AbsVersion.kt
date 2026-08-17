package com.github.devsanso.mypipline.version

abstract class AbsVersion : IVersion {
    abstract val config : IVersion

    abstract override fun getVersion(): Double

    override fun upgrade() {
        config.upgrade()
    }

    override fun downgrade() {
        config.downgrade()
    }
}