/*
 * Copyright 2021-2022 Hichem Fazai (https://github.com/hfazai).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.github.hfazai.exposed.dataframe.tests

import org.jetbrains.exposed.sql.*

object Users : Table() {
    val id = integer("id").autoIncrement()
    val name = varchar("name", length = 20)

    override val primaryKey = PrimaryKey(id)
}

fun Users.insertData() {
    listOf("Hichem", "Mohamed", "Ali").forEach { username ->
        insert {
            it[name] = username
        }
    }
}

fun connectToDB() {
    Database.connect(
        url = "jdbc:h2:mem:test",
        driver = "org.h2.Driver",
        databaseConfig = DatabaseConfig.invoke {
            sqlLogger = StdOutSqlLogger
        }
    )
}
