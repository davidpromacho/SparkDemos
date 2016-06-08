package com.vdaso.tlogs.servicio

/**
  * Created by dpro on 30/05/16.
  */
import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}



object ZipFiles {

  def zip(out: String, file:String, content:String): Unit ={
      val zip = new ZipOutputStream(new FileOutputStream(out,true))
      zip.putNextEntry( new ZipEntry(file) )
      zip.write( scala.io.Codec.toUTF8( content ) )
      zip.closeEntry()
      zip.close()
  }


  def zip(out: String, files: Iterable[String]) = {
    val zip = new ZipOutputStream(new FileOutputStream(out))
    files.foreach {
          name => zip.putNextEntry(new ZipEntry(name))
          val in = new BufferedInputStream(new FileInputStream(name))
          var b = in.read()
          while (b > -1) {
              zip.write(b)
              b = in.read()
          }
        in.close()
        zip.closeEntry()
    }
    zip.close()
  }



}
