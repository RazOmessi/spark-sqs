package com.startapp.data.spark.ds.sqs.client

object CommitPolicy extends Enumeration {
  val Never, QuerySuccess, Shutdown = Value
}
