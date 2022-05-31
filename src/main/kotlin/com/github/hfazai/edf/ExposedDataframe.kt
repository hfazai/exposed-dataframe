/*
 * Copyright 2022-2022 Hichem Fazai (https://github.com/hfazai).
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
package com.github.hfazai.edf

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.Column
import org.jetbrains.kotlinx.dataframe.api.column
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf

fun Table.columns() : List<Column> = columns.map { column -> column.getDfColumn() }

fun Table.dataframe() : AnyFrame {
    val tableColumns = columns()

    val data = selectAll().mapIndexed { index, resultRow ->
        columns.map { resultRow[it] }
    }

    return dataFrameOf(*tableColumns.toTypedArray()) (
        *data.flatten().toTypedArray()
    )
}

private fun <T> org.jetbrains.exposed.sql.Column<T>.getDfColumn(): Column = column<T>(name)
