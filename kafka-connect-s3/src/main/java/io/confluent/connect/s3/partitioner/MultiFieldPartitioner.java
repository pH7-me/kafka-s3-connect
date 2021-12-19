/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * For support of field partition on map
 * Author : Priyesh Padaliya
 *
 */

package io.confluent.connect.s3.partitioner;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.util.DataUtils;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiFieldPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log =
      LoggerFactory.getLogger(io.confluent.connect.storage.partitioner.FieldPartitioner.class);
  private List<String> fieldNames;

  public MultiFieldPartitioner() {}

  public void configure(Map<String, Object> config) {
    this.fieldNames = (List) config.get("partition.field.name");
    this.delim = (String) config.get("directory.delim");
  }

  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    if (value instanceof Struct) {
      Schema valueSchema = sinkRecord.valueSchema();
      Struct struct = (Struct) value;
      StringBuilder builder = new StringBuilder();
      Iterator var6 = this.fieldNames.iterator();

      while (var6.hasNext()) {
        String fieldName = (String) var6.next();
        if (builder.length() > 0) {
          builder.append(this.delim);
        }

        Object partitionKey = struct.get(fieldName);
        Schema.Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            builder.append(fieldName + "=" + record.toString());
            break;
          case STRING:
            builder.append(fieldName + "=" + (String) partitionKey);
            break;
          case BOOLEAN:
            boolean booleanRecord = (Boolean) partitionKey;
            builder.append(fieldName + "=" + Boolean.toString(booleanRecord));
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
      }

      return builder.toString();
    } else {
      return checkForMap(value);
    }
  }

  private String checkForMap(Object value) {
    if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      StringBuilder builder = new StringBuilder();
      for (String fieldName : fieldNames) {
        if (builder.length() > 0) {
          builder.append(this.delim);
        }
        Object fieldValue = "null";
        try {
          fieldValue = DataUtils.getNestedFieldValue(map, fieldName);
        } catch (DataException e) {
          log.warn(
              "{} is unable to parse field - {} from record - {}",
              this.getClass(),
              fieldName,
              value);
        }
        String[] nestedFieldList = fieldName.split("\\.");
        String partitionName = nestedFieldList[nestedFieldList.length - 1];
        if (fieldValue instanceof Number) {
          Number record = (Number) fieldValue;
          builder.append(partitionName + "=" + record.toString());
        } else if (fieldValue instanceof String) {
          builder.append(partitionName + "=" + (String) fieldValue);
        } else if (fieldValue instanceof Boolean) {
          boolean booleanRecord = (boolean) fieldValue;
          builder.append(partitionName + "=" + Boolean.toString(booleanRecord));
        } else {
          log.error(
              "Unsupported type '{}' for user-defined timestamp field.", fieldValue.getClass());
          throw new PartitionException(
              "Error extracting timestamp from record field: " + fieldName);
        }
      }
      return builder.toString();
    } else {
      log.error("Value is not Struct or Map type.");
      throw new PartitionException("Error encoding partition.");
    }
  }

  public List<T> partitionFields() {
    if (this.partitionFields == null) {
      this.partitionFields =
          this.newSchemaGenerator(this.config).newPartitionFields(Utils.join(this.fieldNames, ","));
    }

    return this.partitionFields;
  }
}
