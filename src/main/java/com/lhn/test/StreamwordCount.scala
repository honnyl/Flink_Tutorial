package com.lhn.test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamwordCount {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = tool.get("host")
    val port: String = tool.get("port")

    val dataResource: DataStream[String] = env.socketTextStream(host, port.toInt)

    val value: DataStream[(String, Int)] = dataResource.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
    
    print("aaaa")

    value.print()

    env.execute()
  }

}
