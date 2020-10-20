import producer.playerDataProducerFactory

def main(args: Array[String]): Unit = {
  val input_file = "config.json"
  val json_content = scala.io.Source.fromFile(input_file).mkString
  val json_data = JSON.parseFull(json_content)
  print(json_data)
}