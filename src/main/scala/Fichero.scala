import java.io.{FileOutputStream, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by dpro on 30/05/16.
  */
@SerialVersionUID(100L)
case class Fichero(path :String,sala: String,terminal: String, ciudad: String,socio: String, tipo: String,fecha: String)
extends Serializable{
  override def toString = s"$path;$sala;$terminal;$ciudad;$socio;$tipo;$fecha"
}

  //   data/canoesgaming-sst10-arcade-madrid.madrid.ssts.codere#assetfail.2015-12-27.log




object Ficheros {

  val regEx = "(?:([\\w().-]+-)?(sst\\d|till\\d|sst|till)-)?([\\w]+-)?([\\w.()]+)?#(?:[0-9a-f-]{36})?([\\w]+)\\.?(\\d{4}-\\d{2}-\\d{2})?(\\.1)?\\.log(?:[\\da-f-]{36}|\\.(\\d{4}-\\d{2}-\\d{2}))?(?:[\r\n$])"r

  def readFromFile(file :String) : List[Fichero] = {
      var ts  = System.currentTimeMillis()
      val source : String = scala.io.Source.fromFile(file).mkString
      val m1 = regEx.findAllMatchIn(source)
      var  i = 0;
      val res = m1.map(gr =>
        new Fichero(
          path = "data/"+gr.group(0),
          sala = gr.group(1),
          terminal = gr.group(2),
          ciudad = gr.group(3),
          socio = gr.group(4),
          tipo = gr.group(5),
          fecha = if(gr.group(6) != null) gr.group(6) else if(gr.group(7) != null) gr.group(7) else "null"
        ))


      return res.toList
  }
/*
  def readFromFileRDD(file :String ) : List[Fichero] = {
    var ts  = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Codere1").setMaster("local[1]")
    val sc = new SparkContext(conf)
    println("Creo el contecto spark " +      (System.currentTimeMillis() - ts) )
    ts  = System.currentTimeMillis()
    val rdd = sc.textFile(file)
    println("Creo el rdd " +      (System.currentTimeMillis() - ts) )
    ts  = System.currentTimeMillis()
  //  val rdd1 = rdd.map(getFichero(_))
    println("Creo el rdd1 " +      (System.currentTimeMillis() - ts) )
   // val listaFichero = rdd.map( getFichero(_)).toLocalIterator.toList
    ts  = System.currentTimeMillis()
    println("Líneas leidas " + rdd1.count() + " (" +  (System.currentTimeMillis() - ts) + " )")
   // println("Realizao un map sobre la lista de ficheros con " + listaFichero.length + " líneas ("+ (System.currentTimeMillis()-ts) +")")

    return  List[Fichero]()

  }*/

}


