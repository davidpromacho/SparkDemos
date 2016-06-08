package com.vdaso.tlogs.modelo

/**
  * Created by dpro on 8/06/16.
  */
@SerialVersionUID(200L)
case class Patron(nombre_patron : String, expresion : String, nombre_tipo_log : String, categoria : String ) extends Serializable {
      override def toString = s"$nombre_patron;$expresion;$nombre_tipo_log;$categoria"
}

object Patron{
  def apply(line : List[String]) : Patron = {
    new Patron(
      nombre_patron = if (line.length > 0) line(0) else "",
      expresion = if(line.length > 1) line(1) else "",
      nombre_tipo_log = if(line.length > 2) line(2) else "",
      categoria  = if(line.length > 3) line(3) else ""
    )
  }

  def readFromFile(file :String) : Iterator[Patron] = {
    val source  = scala.io.Source.fromFile(file).getLines()
    for(line <- source ) yield Patron(line.split(";").toList)
  }

}