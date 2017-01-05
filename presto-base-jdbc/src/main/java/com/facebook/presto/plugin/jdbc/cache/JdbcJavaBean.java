/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc.cache;

import java.util.List;

/**
 * Created by bianzexin on 16/12/23.
 */
public class JdbcJavaBean {
    private List<String> columns;
    private Object[] values;

    public JdbcJavaBean(List<String> columns) {
        this.columns = columns;
        this.values = new Object[columns.size()];
    }

    public Object getFieldObjectValue(int index) {
        return values[index];
    }

    public void setFieldObjectValue(int index, Object value) {
        values[index] = value;
    }

    public List<String> getColumns() {
        return columns;
    }
}
