import java.util.Date

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlob
import com.microsoft.azure.storage.blob.CloudBlobContainer

/**
  * Created by dpro on 28/05/16.
  */
class AzureBlobLog(key : String) {

  def storageConnectionString : String  =  "DefaultEndpointsProtocol=https;AccountName=bgtlog;AccountKey="  + key;
  def storageAccount  = CloudStorageAccount.parse(storageConnectionString);
  def blobClient   = storageAccount.createCloudBlobClient();
  def container = blobClient.getContainerReference("bgtlog")


  def DirLogs() : List[String] = {

    val v = container.listBlobs("data/",true).iterator()

    var list : List[String] = List()

    while(v.hasNext){
      val item = v.next();
      if(item.isInstanceOf[CloudBlob])
        list = list :+ item.asInstanceOf[CloudBlob].getName()
    }

    return list;

  }

  def DirLogs(contiene : String) :List[String] = {

    val v = container.listBlobs("data/" + contiene).iterator()

    var list : List[String] = List()

    while(v.hasNext) {
      val item = v.next();

      if(item.isInstanceOf[CloudBlob]) {
        list = list :+ item.asInstanceOf[CloudBlob].getName()
      }
    }

    return list

  }

  def DirLogs(dateIni : Date, dateFin : Date ) : List[String] = {
    val v = container.listBlobs("data/",true).iterator()
    var list : List[String] = List()

    while(v.hasNext){
      val item = v.next();

      if(item.isInstanceOf[CloudBlob]) {
        var cloudBlob = item.asInstanceOf[CloudBlob]
        var date : Date = cloudBlob.getProperties.getLastModified
        if(date.compareTo( dateIni ) >= 0 && date.compareTo( dateFin) <= 0) {
          list = list :+ item.asInstanceOf[CloudBlob].getName()
        }
      }
    }
    return list;

  }

  def ReadLog(name : String) : String = {
    val v = container.listBlobs("data/" + name,true).iterator()
    val item = v.next()
    if(item.isInstanceOf[CloudBlob]){
      val cloudBlob = item.asInstanceOf[CloudBlob]
      var n = cloudBlob.getStreamMinimumReadSizeInBytes
      var buffer  = new Array[Byte](n)
      var str = new StringBuilder()
      var i = 0
      do {
        i = cloudBlob.downloadToByteArray(buffer, 0)
        str.append(new String(buffer, 0, i, "UTF-8"))

      } while(i==n)

      return str.toString()

    }else{
      return ""
    }

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
