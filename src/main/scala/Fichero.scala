
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex.Match

@SerialVersionUID(100L)
case class Fichero(path :String,sala: String,terminal: String, ciudad: String,socio: String, tipo: String,fecha: String)
extends Serializable{
  override def toString = s"$path;$sala;$terminal;$ciudad;$socio;$tipo;$fecha"
}


object Fichero {

  val regEx = "(?:([\\w().-]+-)?(sst\\d|till\\d|sst|till)-)?([\\w]+-)?([\\w.()]+)?#(?:[0-9a-f-]{36})?([\\w]+)\\.?(\\d{4}-\\d{2}-\\d{2})?(\\.1)?\\.log(?:[\\da-f-]{36}|\\.(\\d{4}-\\d{2}-\\d{2}))?(?:[\r\n]|$)".r
  val regEx1= "(?:([\\w().-]+-)?(sst\\d|till\\d|sst|till)-)?([\\w]+-)?([\\w.()]+)?#(?:[0-9a-f-]{36})?([\\w]+)\\.?(\\d{4}-\\d{2}-\\d{2})?(\\.1)?\\.log(?:[\\da-f-]{36}|\\.(\\d{4}-\\d{2}-\\d{2}))?(\n|$)".r
  def apply(m : Match): Fichero = {
    new Fichero(
      path = "data/"+m.group(0).trim(),
      sala = if(m.group(1) != null) m.group(1).init else null,
      terminal = m.group(2),
      ciudad = if(m.group(3) != null) m.group(3).init else null,
      socio = m.group(4),
      tipo = m.group(5),
      fecha = if(m.group(6) != null) m.group(6) else if(m.group(7) != null) m.group(7) else null
    )
  }

  def readFromFile(file :String) : List[Fichero] = {

      val source : String = scala.io.Source.fromFile(file).mkString
      val m1 = regEx.findAllMatchIn(source)
      val res = m1.map( Fichero(_) )
      return res.toList

  }

  /**
    * Ejemplo de cómo sería el un iterado de expresiones regulares devolviendo
    * un RDD con los campos encapsulados en objetos.
    * @param file
    * @return
    */
  def readFromFileRDD(file :String ) : List[Fichero] = {

    val conf = new SparkConf().setAppName("Codere1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(file)
    val rdd1 = rdd.flatMap( regEx.findAllMatchIn(_) )
    val rdd2 = rdd1.map( Fichero(_) )
    return  rdd2.toLocalIterator.toList

  }

}


