package dataframe

import org.apache.spark.sql.SparkSession

object SparkSqlDataFrame extends App {

  // Definimos la estructura del fichero
  case class Person(ID:Int, name:String, age:Int, numFriend:Int)

  // Funcion para convertir la linea del fichero leida en una estructura de tipo Persona
  def mapper(line:String): Person ={
    val fields = line.split(",")

    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

    return person
  }

  // Creamos una sesion de Spark
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[3]")
    .getOrCreate()

  println("\n Reading fakefriends files...")

  // Leemos el contenido del fichero usando el sparkContext
  val lines = spark.sparkContext.textFile("fakefriends.csv")

  println("\nGetting persons structure from file...")

  import spark.implicits._

  // Convertimos en personas RDD y lo pasamos a datasets cacheado
  val people = lines.map(mapper).toDS().cache()

  // Obtenemos los nombres de los usuarios
  println("Seleccionamos todos por nombre")
  people.select("name").show()

  // Obtenemos los usuarios mayores de 20 años
  println("Filtramos por edad mayor a 20 años")
  people.filter(people("age") < 20).show()

  // Obtemos los usuarios por nombre con edad mayores de 20 años
  println("Usuarios por nombre con edad mayores de 20 años")
  people.select("name", "age").filter(people("age") < 20).show()

  // Agrupamos por edad
  println("Agrupamos por edad")
  people.groupBy("age").count().show()

  // Añadimos 10 años mas a todos los registros
  println("Añadimos 10 años mas a todos los registros")
  people.select(people("name"), people("age") + 10).show()

  spark.stop()

}
