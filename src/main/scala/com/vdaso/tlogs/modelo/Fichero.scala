package com.vdaso.tlogs.modelo

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

@SerialVersionUID(100L)
case class Fichero(path :String, nombre_sala: String, tipoid_terminal: String, tipo_sala: String,
                   poblacion_sala: String, comunidad_autonoma: String, tipo_terminal: String,
                   nombre_tipo_log: String, fecha: String)
extends Serializable{
    override def toString = this.productIterator.mkString(";")
}


object Fichero {

//  nombre_sala + "-" + tipoid_terminal + "-" +  tipo_sala + "-" + poblacion_sala + “." + comunidad_autonoma + “.”+  tipo_terminal +  “.codere#" + nombre_tipo_log + "." + fecha + ".1.log"
 // val regEx =  "(?:([\\w().-]+-)?(sst\\d|till\\d|sst|till)-)?([\\w]+-)?([\\w.()]+)?#(?:[0-9a-f-]{36})?([\\w]+)\\.?(\\d{4}-\\d{2}-\\d{2})?(\\.1)?\\.log(?:[\\da-f-]{36}|\\.(\\d{4}-\\d{2}-\\d{2}))?(?:[\r\n$])".r

  val regEx = new Regex (
          "data/(?:" +
                "([\\w().-]+-)?" +            //nombre de la sala.
                "(sst\\d{1,2}|till\\d{1,2}|sst|till)(?:-|\\.)" + // tipoid_terminal
            ")?" +
            "([\\w]+(?:-|\\.))?" + //tipo de sala
            "(?:" +
                "([\\w()-]+)" + //poblacion
                "(\\.[\\w()-]+)?" + //comunidad autonoma
                "(?:(\\.root|\\.ssts|\\.tills|\\.sst\\.test)" +  //tipo terminal
                "(?:\\.codere|\\.unnamed)))?" +
            "#" +
                "(?:[0-9a-f-]{36})?"+
                "([\\w]+)\\.?" + // Nombre tipo log
                "(\\d{4}-\\d{2}-\\d{2})?(\\.1)?\\.log" + //Fecha
                "(?:[\\da-f-]{36}|\\.(\\d{4}-\\d{2}-\\d{2}))?" + //Fecha 2
            "(?:[\r\n]+|$)" //Fin de lines o de string
    )

    def apply(m : Match): Fichero = {
        new Fichero(
            path =   if( m.groupCount > 0 ) m.group(0).trim() else "",
            nombre_sala = if(m.groupCount>1 &&  m.group(1) != null) m.group(1) else null,
            tipoid_terminal = if(m.groupCount >2 )  m.group(2) else null,
            tipo_sala = if(m.groupCount>3) m.group(3) else null,
            poblacion_sala = if(m.groupCount>4 )m.group(4) else null,
            comunidad_autonoma = if(m.groupCount>5) m.group(5) else null,
            tipo_terminal = if(m.groupCount>6) m.group(6) else null,
            nombre_tipo_log = if(m.groupCount>7) m.group(7) else null,
            fecha = if(m.groupCount>8 &&  m.group(8) != null) m.group(8) else if(m.group(9) != null) m.group(9) else null
        )
    }

    def toCSV(file : String, ficheros : Iterator[Fichero]) ={
        val wt = new PrintWriter(new File(file ))
        wt.write(Fichero.getClass.getDeclaredFields.tail.map(_.getName).mkString(";"))
        for( f <- ficheros) wt.write( f.toString)
        wt.close()
    }

    def apply(text : String) : Fichero = {
       Fichero(regEx.findFirstMatchIn(text).get)
    }

    def readFromFile(file :String) : Iterator[Fichero] = {

        val source : String = scala.io.Source.fromFile(file).mkString
        val m = regEx.findAllMatchIn(source)
         m.map( Fichero(_) )

    }

    def readRDD(rdd : RDD[String] ) : RDD[Fichero] = {

      val rdd1 = rdd.flatMap( regEx.findAllMatchIn(_) )
      val rdd2 = rdd1.map( Fichero(_) )
      return  rdd2
  }


}


