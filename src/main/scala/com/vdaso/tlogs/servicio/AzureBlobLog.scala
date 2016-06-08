package com.vdaso.tlogs.servicio

import java.io._
import java.util.Date
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob._



/**
  * Created by dpro on 28/05/16.
  */
class AzureBlobLog(key : String) {

  def storageConnectionString : String  =  "DefaultEndpointsProtocol=https;AccountName=bgtlog;AccountKey="  + key;
  def storageAccount  = CloudStorageAccount.parse(storageConnectionString);
  def blobClient   = storageAccount.createCloudBlobClient();
  def container = blobClient.getContainerReference("bgtlog")

  /**
    * DEvuelve la lita de todos los archivos.
    *
    * @return
    */
  def DirLogs() : List[String] = {

    val v = container.listBlobs("data/",true).iterator()
    var list : List[String] = List()
    while(v.hasNext) {
      val item = v.next()
     if(item.isInstanceOf[CloudBlockBlob]) {
            list :+= item.asInstanceOf[CloudBlob].getName()
      } else if(item.isInstanceOf[CloudBlobDirectory]){
            list = list ::: DirLogs(item.asInstanceOf[CloudBlob].getName())
      }
    }

    return list;
  }

  /**o
    * Devuelve la lsita de archvios que comienza por un texto
    *
    * @param comienza
    * @return
    */
  def DirLogs(comienza : String) :List[String] = {

    val v = container.listBlobs("data/" + comienza).iterator()
    var list : List[String] = List()
    while(v.hasNext) {
      val item = v.next();
      if(item.isInstanceOf[CloudBlob]) {
        list :+= item.asInstanceOf[CloudBlob].getName()
      }
    }

    return list

  }

  /**
    * Devuelve la lista de los archvos que se han modificado entre las fechas del argumento
    *
    * @param dateIni
    * @param dateFin
    * @return
    */
  def DirLogs(dateIni : Date, dateFin : Date ) : List[String] = {

    val v = container.listBlobs("data/",true).iterator()

    var list : List[String] = List()

    while(v.hasNext){
      val item = v.next();
      if(item.isInstanceOf[CloudBlob]) {
          var cloudBlob = item.asInstanceOf[CloudBlob]
          var date : Date = cloudBlob.getProperties.getLastModified
          if(date.compareTo( dateIni ) >= 0 && date.compareTo( dateFin) <= 0) {
            list :+= item.asInstanceOf[CloudBlob].getName()
          }
      }
    }

    return list;

  }

  def DirLogs(empieza:String,  dateIni : Date, dateFin : Date ) : List[String] = {

    val v = container.listBlobs("data/" + empieza,true).iterator()

    var list : List[String] = List()

    while(v.hasNext){

      val item = v.next();

      if(item.isInstanceOf[CloudBlob]) {

          var cloudBlob = item.asInstanceOf[CloudBlob]
          var date : Date = cloudBlob.getProperties.getLastModified

          if(date.compareTo( dateIni ) >= 0 && date.compareTo( dateFin) <= 0) {
              list :+= item.asInstanceOf[CloudBlob].getName()
          }

      }

    }

    return list;

  }

  /**
    * Lee en formato texto el contenido del archovo que se le pasa por argumento.
    *
    * @param name
    * @return
    */
  def ReadLog(name : String) : String = {

    var fileName : String = ""
    if(name.startsWith(("data/")))
        fileName = name
    else
        fileName = "data/" + name

    val v = container.listBlobs( fileName,true).iterator()
    val item = v.next()
    if(item.isInstanceOf[CloudBlob]){
      val cloudBlob = item.asInstanceOf[CloudBlob]
      var n = cloudBlob.getStreamMinimumReadSizeInBytes
      var buffer  = new Array[Byte](n)
      var str = new StringBuilder()
      var i = 0
      do {
        i = cloudBlob.downloadToByteArray(buffer, 0)
        str.append(new String(buffer,0,i))
      } while(i==n)

      return str.toString()

    }else{
      return ""
    }

  }

  def ReadLogAsArray(name : String) : Array[Byte] = {

    val fileName = if (name.startsWith("data/")) name.trim() else "data/" + name.trim()

    println("Descargando " + fileName + " ....")

    try {
      val v = container.listBlobs(fileName, true).iterator()

    //  for(item  <- v  if item.isInstanceOf[CloudBlob] ){

      /*  val cloudBlob = item.asInstanceOf[CloudBlob]
        val n = cloudBlob.getStreamMinimumReadSizeInBytes
        val buffer: Array[Byte] = new Array[Byte](n)
        val res = new ByteArrayOutputStream()
        var i = 0
        do {
          i = cloudBlob.downloadToByteArray(buffer, 0)
          res.write(buffer, 0, i)
        } while (i == n)
        */

 //     }



      return null //res.toByteArray
    } catch {
        case e: Any =>
          println("Error al descargar " + fileName + " " + e.toString() )
          return null
    }


  }

  def SaveZipLog(name : String, files : List[String]): Unit = {
      val zip = new ZipOutputStream(new FileOutputStream(name))
      files.foreach { file =>
             val content = ReadLogAsArray(file)
             if(content != null && content.length > 0) {
                zip.putNextEntry(new ZipEntry(file))
                zip.write(content)
                zip.closeEntry()
             }
      }
      zip.close()
  }


  def SaveLog(name : String, file : String) : Unit = {
    val v = container.listBlobs("data/" + name,true).iterator()
    val item = v.next()
    if( item.isInstanceOf[CloudBlob]) {
      val cloudBlob = item.asInstanceOf[CloudBlob]
      cloudBlob.downloadToFile(file)
    }
  }




}
