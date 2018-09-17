package com.startapp.data.spark.ds.sqs

import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.Message
import com.startapp.data.spark.ds.sqs.client.{AbstractSqsDeletePolicy, CommitPolicy, SqsClient}
import com.startapp.data.spark.ds.sqs.client.SqsClient.SqsClientBuilder
import com.startapp.data.spark.ds.sqs.messages.{SqsMessage, SqsMessageReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.startapp.data.spark.ds.sqs.utils.Consts._

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{
  private val logger = Logger.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val queueURL = parameters.getOrElse(QUEUE_URL, sys.error(s"missing $QUEUE_URL configuration"))
    val region = parameters.get(REGION)
    val accessKey = parameters.get(ACCESS_KEY)
    val secretKey = parameters.get(ACCESS_SECRET)
    val minMessages = parameters.get(MIN_MESSAGES)
    val maxMessages = parameters.get(MAX_MESSAGES)
    val prefixPath = parameters.getOrElse(FILE_PREFIX, "")

    val innerFormat = parameters.getOrElse(INNER_FORMAT, sys.error(s"missing $INNER_FORMAT configuration"))
    val messageParserClass = parameters.getOrElse(MESSAGE_PARSER_CLASS, S3_MESSAGE_PARSER_CLASS)
    val commitClassPath = parameters.get(DELETE_POLICY).map(CommitPolicy.withName).flatMap(DELETE_POLICYS.get)
      .getOrElse(parameters.getOrElse(DELETE_POLICY_CLASS, QUERY_SUCCESS_COMMIT_CLASS))

    val innerParameters = parameters
      .filter(_._1.startsWith("inner."))
      .map(kv => (kv._1.substring("inner.".length), kv._2))

    if((accessKey.isDefined && secretKey.isEmpty) || (accessKey.isEmpty && secretKey.isDefined)){
      sys.error(s"missing ${if(accessKey.isDefined) ACCESS_SECRET else ACCESS_KEY} configuration.")
    }

    val clientBuilder = new SqsClientBuilder(queueURL)
      .withRegion(region.map(Regions.valueOf))
      .withMinimumMessages(minMessages.map(_.toInt).getOrElse(0))
      .withMaximumMessages(maxMessages.map(_.toInt).getOrElse(Int.MaxValue))

    val client = if(accessKey.isDefined && secretKey.isDefined){
      clientBuilder.withCredentials(accessKey.get, secretKey.get).build()
    } else {
      clientBuilder.build()
    }

    val sqsMessageReader = Class.forName(messageParserClass).newInstance().asInstanceOf[SqsMessageReader[SqsMessage]]
    val sqsMessages = client.getMessages(jsValue => sqsMessageReader.validate(sqsMessageReader.getRecords(jsValue)))

    val commitClassConstructor = Class.forName(commitClassPath).getDeclaredConstructor(classOf[SqsClient], classOf[List[Message]])
    val commitClass = commitClassConstructor.newInstance(client, sqsMessages.messages).asInstanceOf[AbstractSqsDeletePolicy]
    commitClass.registerCommitMessages(sqlContext)

    val files = sqsMessages.results.map(path => prefixPath + path)
    logger.info(s"got ${files.length} messages to read.")

    new SQSRelation(sqlContext, innerFormat, files, innerParameters, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???
}
