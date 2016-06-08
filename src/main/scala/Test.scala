import java.io._
import java.util.Properties

import com.vdaso.tlogs.modelo._
import com.vdaso.tlogs.servicio._

/**
  * Created by dpro on 29/05/16.
  */




object Test {

  def dir = Prop.filesystem_input
  def main(args: Array[String]): Unit = {

   println( dir)




    //  val f = Fichero.readFromFile(dir + "dir.txt");

    //  val l = f.toList



   ////  Test2_LeerDirectorioAzure
    //  Test1_ReadFiles

   // println("Se ha leido el fichero de directorios")


   // println(l.length)
  //  val temp = l.filter(_.fecha == "")
  //  for(k <- l.groupBy( f => f.fecha ).toList.sortBy( _._1)) {
  //      az.SaveZipLog(dir +"data/" + k._1 + ".zip", k._2.map( _.path ).toList)
  //  }

    //  for( path <- temp.map(_.path)) println(path)
    //  for(path  <- l.filter(_.fecha ==null).map(_.path)) {
    //  println()
    //}

   // for(f : String <- l.map( _.fecha).distinct){
    //  println("Fecha: " + f)
       //
     //var temp = l.filter(f => f.fecha==fecha).map( _.path  )
     //  println("Se ha comenzado a leer los erchivos con la fecha: " + fecha  + " y tiene " + temp.length + " ficheros")
        //saz.SaveZipLog(dir + fecha + ".zip",temp )
    // }

    /*


    l.foreach { f =>    println("Creo el contecto spark y leo el fichero"
         ZipFiles.zip(dir + f.sala + f.fecha + ".zip", z.ReadLog(f.path))

    }*/
    /*
    var s1 = "data/#BootstrapLog.2015-10-13.log"
    var s2 = "data/#BootstrapLog.2015-10-14.log"

   // val f = new Fichero(s)

 //   println(
    var content = az.ReadLog(s1)

    ZipFiles.zip(dir + "fichero"+ ".zip",s1,content)
    ZipFiles.zip(dir + "fichero"+ ".zip",s2,az.ReadLog(s2))

    println("Fin del test")
    */

  }

  /**
    * El test 1 leer una lista de txt y los parsea.
    * El tes compara la velocidad realizando en memoria y con RDD
    * Las pruebas son
    *   3.5 sg lectura directa de archivo.
    *   8.5 sg lecuta a través de rdd.
    * Los resultados los almacena en un CSV
    */
  def Test1_ReadFiles {

    println("Inicio del test 1")

    val t1 = System.currentTimeMillis()
    val l1 = Fichero.readFromFile(dir + "dir.txt")


    println(l1.length + " - "  + (System.currentTimeMillis() - t1))

    val pw1 = new PrintWriter(new File(dir + "dir1.csv" ))
    l1.foreach( x => pw1.write(x + "\n") )
     pw1.close


  }

  /**
    * Lee le directorio de Azure y lo almacena en un archivo txt
    */
  def Test2_LeerDirectorioAzure(): Unit ={

    println("Inicio del test 2. Lectura de directorio de Azure")
    val ts = System.currentTimeMillis()

    val az = new AzureBlobLog("TughY1KHJ+NmAYbo+Zeqonb4sBWO29+38zGcz9h4cRkMGOwpCoO+NQAetOtsv8X8lCLQMWVAYF7J+Wh3F1T8eg==")
    val list = az.DirLogs()
    val wt = new PrintWriter(dir + "dir.txt")
    for(l <- list) wt.write(l + "\n")
    wt.close()
    println(s"La lectura ha durado ${System.currentTimeMillis-ts}. Se han leído ${list.length} archivos." )

  }

}


