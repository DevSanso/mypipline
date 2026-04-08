package com.github.devsanso.mypipline.version.config

import com.github.devsanso.mypipline.version.IVersion
import com.github.devsanso.mypipline.version.config.ConfigVersion1.MyPipPlanScript.planId
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.migration.jdbc.MigrationUtils

class ConfigVersion1(val db : Database) : IVersion {
    // mypip_connection_info
    object MyPipConnectionInfo : Table("mypip_connection_info") {
        val identifier = text("identifier")
        val id = integer("id")
        val maxSize = long("max_size")
        val name = text("name")
        val connType = text("conn_type")
        val connName = text("conn_name")
        val connUser = text("conn_user")
        val connAddr = text("conn_addr")
        val connPasswd = text("conn_passwd")
        val connTimeout = integer("conn_timeout")

        val odbcDriver = text("odbc_driver").nullable()
        val odbcCurrentTimeQuery = text("odbc_current_time_query").nullable()
        val odbcCurrentTimeColName = text("odbc_current_time_col_name").nullable()

        override val primaryKey = PrimaryKey(identifier, name)
    }


    // mypip_plan_chain_bind_param
    object MyPipPlanChainBindParam : Table("mypip_plan_chain_bind_param") {
        val id = long("id")
        val chainId = text("chain_id")
        val idx = long("idx")
        val key = text("key")
        val bindId = text("bind_id")
        val row = long("row").nullable()

        override val primaryKey = PrimaryKey(id, chainId)
    }


    // mypip_plan_chain_args
    object MyPipPlanChainArgs : Table("mypip_plan_chain_args") {
        val id = long("id")
        val chainId = text("chain_id")
        val data = text("data")
        val idx = long("idx")

        override val primaryKey = PrimaryKey(id, chainId)
    }


    // mypip_plan_chain_mapping
    object MyPipPlanChainMapping : Table("mypip_plan_chain_mapping") {
        val chainId = text("chain_id")
        val mappingType = text("mapping_type")
        val ranking = integer("ranking")
        val argsOrBindId = long("args_or_bind_id")

        override val primaryKey = PrimaryKey(chainId, ranking)

        init {
            uniqueIndex(chainId, argsOrBindId)
        }
    }


    // mypip_plan_chain
    object MyPipPlanChain : Table("mypip_plan_chain") {
        val id = text("id")
        val planId = integer("plan_id")
        val nextChainId = text("next_chain_id")
        val connection = text("connection")
        val query = text("query")

        override val primaryKey = PrimaryKey(id)

        init {
            index(false, planId)
            index(false, nextChainId)
        }
    }


    // mypip_plan_script
    object MyPipPlanScript : Table("mypip_plan_script") {
        val id = integer("id")
        val planId = integer("plan_id")
        val lang = text("lang")
        val file = text("file")

        override val primaryKey = PrimaryKey(id)

        init {
            uniqueIndex(planId)
        }
    }


    // mypip_plan
    object MyPipPlan : Table("mypip_plan") {
        val identifier = text("identifier").nullable()
        val id = integer("id")
        val name = text("name")
        val typeName = text("type_name")
        val enable = bool("enable").default(true)
        val intervalConnection = text("interval_connection").nullable()
        val intervalSecond = long("interval_second")

        override val primaryKey = PrimaryKey(identifier, id)
    }


    // mypip_plan_script_data
    object MyPipPlanScriptData : Table("mypip_plan_script_data") {
        val identifier = text("identifier")
        val scriptFile = text("script_file")
        val scriptData = text("script_data")

        override val primaryKey = PrimaryKey(identifier, scriptFile)
    }


    // mypip_plan_toml
    object MyPipPlanToml : Table("mypip_plan_toml") {
        val identifier = text("identifier")
        val name = text("name")
        val tomlData = text("toml_data")
        val enable = bool("enable")

        override val primaryKey = PrimaryKey(identifier, name)
    }

    object MyPipUser : Table("mypip_user") {
        val identifier = text("identifier")
        val name = text("name")
        val password = text("password")
        val isAdmin = bool("is_admin")

        override val primaryKey = PrimaryKey(name)

        init {
            uniqueIndex(identifier)
        }
    }

    override fun getVersion(): Double = 1.0

    override fun upgrade() {
        transaction(db) {
            SchemaUtils.create(MyPipConnectionInfo)
            SchemaUtils.create(MyPipPlanChainMapping)
            SchemaUtils.create(MyPipPlanChainArgs)
            SchemaUtils.create(MyPipPlanChainBindParam)
            SchemaUtils.create(MyPipPlanChain)
            SchemaUtils.create(MyPipPlanScript)
            SchemaUtils.create(MyPipPlan)
            SchemaUtils.create(MyPipPlanScriptData)
            SchemaUtils.create(MyPipPlanToml)
            SchemaUtils.create(MyPipUser)
        }
    }

    override fun downgrade() {
        throw Exception("init version, not exists downgrade")
    }

}