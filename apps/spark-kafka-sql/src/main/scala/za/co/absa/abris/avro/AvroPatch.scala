package za.co.absa.abris.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, Row}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.{ScalaConfluentKafkaAvroDeserializer, SchemaManager}
import za.co.absa.abris.avro.serde.{AvroDecoder, AvroToRowConverter, AvroToRowEncoderFactory}

object AvroPatch {

  // no way to access the key using the standard absa library

  implicit class AvroDeserializer(dataframe: Dataset[Row]) extends AvroDecoder {

    /**
     * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
     * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
     * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
     *
     * Refer to the [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] documentation to better understand how this
     * operation is performed.
     */
    def fromConfluentAvroForKey(destinationColumn: String, schemaRegistryConf: Map[String, String]): Dataset[Row] = {
      fromConfluentAvro(destinationColumn, schemaRegistryConf, isKey = true)
    }

    def fromConfluentAvroForValue(destinationColumn: String, schemaRegistryConf: Map[String, String]): Dataset[Row] = {
      fromConfluentAvro(destinationColumn, schemaRegistryConf, isKey = false)
    }

    private def fromConfluentAvro(
      destinationColumn: String,
      schemaRegistryConf: Map[String, String],
      isKey: Boolean
    ): Dataset[Row] = {
      val dataSchema = schemaFromRegistry(schemaRegistryConf, isKey)

      val originalSchema = dataframe.schema

      // sets the Avro schema into the destination field
      val destinationIndex = originalSchema.fields
        .toList
        .indexWhere(_.name.toLowerCase == destinationColumn.toLowerCase)
      originalSchema.fields(destinationIndex) = StructField(
        destinationColumn,
        SparkAvroConversions.toSqlType(dataSchema),
        nullable = false
      )

      implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

      dataframe
        .mapPartitions(
          partition => {

            val readSchema = schemaFromRegistry(
              schemaRegistryConf,
              isKey
            ) // can't reuse dataSchema here as it's not serializable
            val topic = Some(schemaRegistryConf(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC))

            // here we explicitly pass in the schema we used to enerate the DataFrame schema to act as the read
            // schema instead of relying on what Abris code does which is to just use the write schema
            // if you don't do this, reading old data with the latest schema which has a new optional field will result in a...
            // Caused by: java.lang.ArrayIndexOutOfBoundsException: 76
            //    at org.apache.spark.sql.catalyst.expressions.GenericRow.get(rows.scala:174) ~[spark-catalyst_2.11-2.4.3.jar:2.4.3]

            val avroReader = new ScalaConfluentKafkaAvroDeserializer(topic, Some(readSchema))
            avroReader.configureSchemaRegistry(schemaRegistryConf)
            val avroToRowConverter = new AvroToRowConverter(None)

            partition.map(
              avroRecord => {

                val sparkType = avroToRowConverter.convert(
                  avroReader.deserialize(
                    avroRecord
                      .get(destinationIndex)
                      .asInstanceOf[Array[Byte]]
                  )
                )
                val array: Array[Any] = new Array(avroRecord.size)

                for (i <- 0 until avroRecord.size) {
                  array(i) = avroRecord.get(i)
                }
                array(destinationIndex) = sparkType
                Row.fromSeq(array)
              }
            )
          }
        )
    }

  }

  private def schemaFromRegistry(schemaRegistryConf: Map[String, String], isKey: Boolean): Schema = {
    val schemaTuple = AvroSchemaUtils.loadForKeyAndValue(schemaRegistryConf)
    if (isKey) schemaTuple._1 else schemaTuple._2
  }

}
