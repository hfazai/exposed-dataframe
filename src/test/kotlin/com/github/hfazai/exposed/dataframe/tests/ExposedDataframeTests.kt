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

import kotlin.test.assertEquals

import com.github.hfazai.exposed.dataframe.columns

import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

class ExposedDataframeTests {

    companion object {
        @BeforeClass
        @JvmStatic
        fun initConnection() {
            connectToDB()
        }
    }

    @Before
    fun init() {
        transaction {
            SchemaUtils.create(Users)
            Users.insertData()
        }
    }

     @After
     fun tearDown() {
         transaction {
             SchemaUtils.drop(Users)
         }
     }

    @Test
    fun `build dataframe columns from exposed table`() {
        val columns = Users.columns()

        assertEquals(Users.columns.size, columns.size)
        columns.forEachIndexed { index, column ->
            assertEquals(Users.columns[index].name, column.name())
        }
    }
}
