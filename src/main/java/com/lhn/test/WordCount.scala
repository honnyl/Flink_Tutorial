package com.lhn.test

import org.apache.flink.api.scala._


object WordCount {

  def main(args: Array[String]): Unit = {

    //批处理，有界集，不需要关闭流
    var environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataSet : DataSet[String] = environment.readTextFile("C:\\Users\\honnyl\\IdeaProjects\\Flink_Tutorial\\src\\main\\resources\\hello.txt")

    val resultSet: DataSet[(String, Int)] = dataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    resultSet.print()
  }

}
