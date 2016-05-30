package com.example

import au.com.bytecode.opencsv.CSVWriter
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import java.io.StringWriter
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


object Codere1
{
    val conf = new SparkConf().setAppName("Codere1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //Función main prncipal/////////////////////////////////////////////////////////////////
    def main(args: Array[String])
    {
        /*Leemos los ficheros del directorio*/
        def getListOfFiles(dir: String):List[File] =
        {
            val d = new File(dir)
            if (d.exists && d.isDirectory)
            {
                d.listFiles.filter(_.isFile).toList
            }
            else
            {
                List[File]()
            }
        }

        val rutafichero = "/home/spark/Escritorio/mbit/codere/Logs";
        val files = getListOfFiles(rutafichero);

        //println("Estamos antes de imprimir");
        //files.count()
        val out = new PrintWriter( "/home/spark/Escritorio/mbit/codere/nombres_ficheros.txt" , "UTF-8")
        try{
            files.foreach
            {
                x => out.println(x);
                    var nombrefichero:String = x.getName();
                    var tipofichero = tipoFichero(nombrefichero);

                    var salaterminalca = nombreSalaTerminalCA2(nombrefichero);

                    println(nombrefichero)
                    println ("tipo fichero : "+tipofichero);
                    println ("Sala : "+salaterminalca(0));
                    println ("terminal: "+salaterminalca(1));
                    println ("Tipo de sala: "+salaterminalca(2));
                    println ("población: "+salaterminalca(3));
                    println ("CA: "+salaterminalca(4));
                    println ("TIPO: "+salaterminalca(5));

                    var rutaficherocompleta = rutafichero + "/" + nombrefichero;
                //COC 27-05-2016 comentamos para el cambio detectado por ADO sobre los tipos de nombre de fichero
                // var casostipofichero = casoFichero(tipofichero, rutaficherocompleta,salaterminalca);


            }
        }
        finally{ out.close }
    }
    //Bloque de funciones auxiliares/////////////////////////////////////////////////////////
    /*
    COC 3-5-2016
    En esta función sacamos del tipo del fichero los siguientes datos:
    COC 27-05-2016 cambiamos el algritmo aparecen nombres sin codere solo tomamos #

    * */
    def tipoFichero(nombrefichero: String):(String) =
    {
        var inicio = nombrefichero.indexOf("#") + 1
        var tipofichero = nombrefichero.substring(inicio,nombrefichero.length)
        tipofichero = tipofichero.substring(0, tipofichero.indexOf("."))
        return tipofichero
    }

    /*
    COC 6-5-2016
    V2 COC 30-05-2016 cambiamos el parseo por nuevos casos introducidos
    Versión 2 de la función nombreSalaTerminalCA con Split
    En esta función sacamos del titulo del fichero los siguientes datos:
    Sala : 0
    terminal: 1
    tiposala: 2
    poblacion: 3
    CA: 4
    TIPOterminal: 5
    en el Array completamos los datos en ese orden de la posición 0 a la 5.
    * */
    def nombreSalaTerminalCA2(nombrefichero: String):(Array[String]) =
    {
        var salaterminalca = new Array[String](6);
        var primeraparte = nombrefichero.split('.')(0)
        var segundaparte = nombrefichero.substring(nombrefichero.indexOf(".")+1,nombrefichero.length);
        println("NOMBRE DEL FICHERO: "+nombrefichero);
        println("primera parte del nombre = " +primeraparte)
        println("segunda parte del nombre = " +segundaparte)
       //Parseamos la primera parte del nombre
       //Miramos si contiene guión puede tener varias partes
        if(primeraparte.indexOf('-') > 0)
        {
            var primeraparte2 = "";
            if (primeraparte.indexOf("sst") > 0 || primeraparte.indexOf("till") > 0) {
                if (primeraparte.indexOf("sst") > 0) {
                    salaterminalca(5) = "sst" //TIPO DE TERMINAL
                    salaterminalca(0) = primeraparte.substring(0, primeraparte.indexOf("sst") - 1) //Sala
                    //Comprobamos cuantos - tiene a la derecha de sst
                    primeraparte2 = primeraparte.substring(primeraparte.indexOf("sst"), primeraparte.length) //Recogemos la parte a la derecha de sst (incluido)
                }
                else {
                    salaterminalca(5) = "till" //TIPO DE TERMINAL
                    salaterminalca(0) = primeraparte.substring(0, primeraparte.indexOf("till") - 1) //Sala
                    //Comprobamos cuantos - tiene a la derecha de till
                    primeraparte2 = primeraparte.substring(primeraparte.indexOf("till"), primeraparte.length) //Recogemos la parte a la derecha de sst (incluido)
                }

                //Solo tiene el terminal
                if (primeraparte2.indexOf("-") < 0) {
                    salaterminalca(1) = primeraparte2;
                    salaterminalca(2) = "SIN TIPO DE SALA"
                    salaterminalca(3) = "SIN POBLACIÓN"

                }
                //Tiene más campos
                else {
                    salaterminalca(1) = primeraparte2.substring(0, primeraparte2.indexOf("-") - 1)
                    var primeraparte3 = primeraparte2.substring(primeraparte2.indexOf("-") + 1, primeraparte2.length)
                    //miramos a ver cuantos campos tiene tras sst/till
                    //Solo tiene un campo más
                    if (primeraparte3.indexOf("-") < 0) {
                        salaterminalca(2) = primeraparte3
                        salaterminalca(3) = "SIN POBLACIÓN"
                    }
                    //Tiene almenos dos
                    else {
                        salaterminalca(2) = primeraparte3.substring(0, primeraparte3.indexOf("-"))
                        salaterminalca(3) = primeraparte3.substring(primeraparte3.indexOf("-")+1, primeraparte3.length)
                    }
                }
            }
            //Solo tiene un campo y no detectamos sst ni till
            else
            {
                salaterminalca(0) = primeraparte;

                salaterminalca(1) = "SIN TERMINAL"
                salaterminalca(2) = "SIN TIPO DE SALA"
                salaterminalca(3) = "SIN POBLACION"
            }
        }
        //Solamente tiene un campo y se lo asignamos al nombre de sala
        else
        {
           salaterminalca(0) = primeraparte;

            salaterminalca(1) = "SIN TERMINAL"
            salaterminalca(2) = "SIN TIPO DE SALA"
            salaterminalca(3) = "SIN POBLACION"
        }
        //Parseamos la segunda parte ojo el tipo de terminal ya lo hemos completado según sst o till
       if(segundaparte.indexOf(".") > 0) // tiene informado más de un campo
        {
            salaterminalca(4) = segundaparte.substring(0,segundaparte.indexOf("."))
        }
        else //Tiene almenos dos tomamos el primero como CA
        {
            salaterminalca(4) = segundaparte
        }
        if (segundaparte.indexOf("sst") > 0 )
        {
            salaterminalca(5) = "sst"
        }
        else if(segundaparte.indexOf("till") > 0)
        {
            salaterminalca(5) = "till"
        }


        return salaterminalca;

    }

    /*
    COC 06-05-2016
    En esta función vamos a clasificar las acciones a realizar según el tipo de fichero:
    BootstrapLog
    BrowserOverlay
    CashLog
    Printerlog
    Localization
    LiveLog
    Assetfail
    Sbsfail

    * */
    def casoFichero (tipofichero : String, ruta : String, salaterminalca: Array[String]) : String =
    {
        var resultado  = "RESULTADO"

        tipofichero match {
            case "LiveLog" =>
            {
                println("ESTAMOS EN LIVELOG")
                var LiveLog = analisisLiveLog(ruta,salaterminalca);
            }
            case "BootstrapLog" =>
            {
                //println("ESTAMOS EN BootstrapLog")
            }
        }
        return resultado
    }

    /*
    COC 6-5-2016
    En esta funcion procesamos el contenido del fichero
    LiveLog
    * */

    def analisisLiveLog(ruta :String,salaterminalca: Array[String] ) : String =
    {
        var resultado = "RESULTADO"
        //COC 07-05-2016 generamos un RDD partiendo del fichero
        val rddlineas = sc.textFile(ruta)
        //COC 07-05-2016 llamo a las tres funciones para segmentar el fichero
        val rddcompleto =completarRdd(rddlineas)
        val rddlimpio =limpiarRdd(rddlineas)

        val rrdParseado=parsearRdd(rddlimpio,salaterminalca)

        // guardarCsv(rddcompleto, salaterminalca)

        val rddnavegacion = navegacionRdd(rddlimpio)
        val rddmonedas = monedasRdd(rddlimpio)

        impresion(rddcompleto)
        //impresion2(rddmonedas)

        return resultado;
    }

    /*
    COC 13-05-2016 Función para parsear en campos los ficheros añadiendole los datos de información contenidos
    en el nombre del fichero
    * */

    def parsearRdd (rdd : RDD[String],salaterminalca: Array[String]):RDD[String] =
    {
        /*
         Sala : 0
         terminal: 2
         CA: 1
         TIPO: 3
         */
        val rddmensaje = rdd.map(x =>x.split(" - ")).map(x=>(x(0).replace("][","!!!").replace("[","!!!").replace("]","!!!").replace(" ","!!!"),x(1)))
        val rddmensajecomp = rddmensaje.map(x=>salaterminalca(0)+"!!!"+salaterminalca(2)+"!!!"+salaterminalca(1)+"!!!"+salaterminalca(3)+"!!!"+x._1+x._2)

        // impresion(rddmensajecomp)
        // impresion(rddmensaje)

        return rddmensajecomp
    }


    def guardarCsv (rdd : RDD[String],salaterminalca: Array[String]):String =
    {
        println ("ESTOY en guardarCsv2")
        val fichero = "/home/spark/mbit/codere/logsProcesados/" + salaterminalca(0) +"_"+ salaterminalca(2) +"_"+ salaterminalca(1) +"_"+ salaterminalca(3) +".CtV"//depurar fecha fichero
    //impresion(rdd)
    val bloques = rdd.map(x => x.split("!!!")).mapPartitions
        {
            x=>
                val stringWriter = new StringWriter();
                val csvWriter = new CSVWriter(stringWriter,'¿','\'');

                csvWriter.writeAll(x.toList)
                Iterator(stringWriter.toString)
        }.saveAsTextFile(fichero)

        return "resultado"

    }

    /*
   COC 20-05-2016 Función que competa las lineas que no cumplen con lo establecido añadiendolas al mensaje anterior.
   * */
    def completarRdd (rdd : RDD[String]):RDD[String] =
    {
        //COC 07-05-2016 expresión regular que indica que el primer caracter es de 0 a 3
        //realizamos el segundo RDD con filter para limpiar las filas que no tienen información a procesar
        //Con el filtro y la expresión findFirstIn vemos si devuelve cadena, si es correcto lo guardamos en rddfiltro
        //val rddfiltro = rddlineas.filter(x => regprimercaracterlinea.findFirstIn(x).nonEmpty )
        println("ESTAMOS EN completarRdd")
        var lineaerror = "";
        var regprimercaracterlinea = "^(0[1-9]|[12][0-9]|3[01])\\.(0[1-9]|1[012])\\.(19|20)[0-9]{2}[\\s](0[0-9]|1\\d|2[0-3]):([0-5]\\d):([0-5]\\d),\\d{3}\\[".r;
        //val rrdresultado = rdd.map(x => regprimercaracterlinea.findFirstIn(x).nonEmpty )
        /*
        En la variable mensajetotal vamos a componer el mensaje de forma completa
         recorremos el rdd y por cada linea (cada X) analizamos si cumple o no la experesión regular
         si no la cumple añadimos esa linea al mensaje de la linea anterior.
         Para lograrlo paso todo a cadena y separo cada mensaje por "?"
         finalmente genero un rdd con el array de string con el metodo sc.makeRDD(mensajeparseado)
        */
        var mensajetotal = rdd.collect().toString
        println (mensajetotal.toString)

        rdd.foreach
        {
            x => val mensaje = x.toString
                if(regprimercaracterlinea.findFirstIn(mensaje).isEmpty)//Caso de linea erronea
                {
                    if (mensajetotal.length == 0)
                    {
                        mensajetotal = mensaje + "Ñ"

                    }
                    else
                    {

                        mensajetotal = mensajetotal + " " + mensaje + "Ñ"

                    }
                }
                else
                {
                    mensajetotal = mensajetotal + mensaje + "Ñ"
                }
                mensajetotal = mensajetotal + mensaje + "Ñ"
                println ("tamaño de mensajetotal " + mensajetotal.length)
                println ("mensaje total = " + mensajetotal)

        }


        /*
        mensajetotal=mensajetotal + mensaje

        println("Tamaño de mensaje inicio: " + mensajetotal.length)
        if(regprimercaracterlinea.findFirstIn(x).isEmpty)
        {
          println("una linea mala que es " + mensaje)
            println("tamaño es: " + mensajetotal.length)
            if(mensajetotal.lastIndexOf("ñ") == mensajetotal.length)
            {
                println("pasa por mensajetotal.substring(mensajetotal.lastIndexOf == mensajetotal.length")
                mensajetotal = mensajetotal.substring(0, mensajetotal.length -2) + " " + mensaje+ "ñ"
            }
            else
            {
                mensajetotal = mensajetotal + " " + mensaje + "ñ"
            }
        }
        else
        {
          //  println("una linea buena")
            mensajetotal = mensajetotal + mensaje.toString + "ñ"
        }
        println("DENTRO DEL BUCLE" + mensajetotal)
            println("tamaño: " + mensajetotal.length)

    }
    println("FUERA DEL BUCLE" + mensajetotal)
    println ("tamaño de mensajetotal" + mensajetotal.length)
    println ("mensaje total = " + mensajetotal)
    val mensajeparseado = mensajetotal.split("¿")

    println ("tamaño de mensajeparseado" + mensajeparseado.length)

    mensajeparseado.foreach(println)

    val rdd2 = sc.makeRDD(mensajeparseado.toList)

    impresion(rdd2)
    */
        return rdd

    }
    /*
   COC 07-05-2016 Función que limpia las lineas que no cumplen con lo establecido.
   * */
    def limpiarRdd (rdd : RDD[String]):RDD[String] =
    {
        //COC 07-05-2016 expresión regular que indica que el primer caracter es de 0 a 3
        //realizamos el segundo RDD con filter para limpiar las filas que no tienen información a procesar
        //Con el filtro y la expresión findFirstIn vemos si devuelve cadena, si es correcto lo guardamos en rddfiltro
        //val rddfiltro = rddlineas.filter(x => regprimercaracterlinea.findFirstIn(x).nonEmpty )
        var regprimercaracterlinea = "^(0[1-9]|[12][0-9]|3[01])\\.(0[1-9]|1[012])\\.(19|20)[0-9]{2}[\\s](0[0-9]|1\\d|2[0-3]):([0-5]\\d):([0-5]\\d),\\d{3}\\[".r;
        val rrdresultado = rdd.filter(x => regprimercaracterlinea.findFirstIn(x).nonEmpty )
        return rrdresultado
    }

    /*
    COC 07-05-2016 Función que lista el proceso de navegación de un fichero
    * */
    def navegacionRdd (rdd : RDD[String]):RDD[String] =
    {
        //COC 07.05.2016 variable para buscar la navegación
        var navegacion = "Navigating to destination"
        val rrdresultado = rdd.filter(x => x.contains(navegacion))
        return rrdresultado
    }

    /*
    COC 07-05-2016 Función que cuneta el numero de veces que aparece la inserción de un tipo de moneda
    en un fichero
    * */
    def monedasRdd (rdd : RDD[String]):RDD[(String, Int)]=
    {
        var coinstotal = "cash token Coin("
        //RDD para todos las monedas y posterior reducción por valor
        val rrdmonedas = rdd.filter(x => x.contains(coinstotal))
        //COC 07-05-2016 realizamos un par key-value:
        //dividimos el mensaje por " - " y forzamos a que la la vlave sea la segunda parte (1) y adicionamos 1
        //para que contabilice cada ocurrencia
        val rrdresultado = rrdmonedas.map(x =>(x.split(" - ") (1),1))

        return  rrdresultado.reduceByKey((x, y) => x + y)
    }

    /*
    COC 07-05-2016 Función para la impresión de los RDDs generados
    * */

    def impresion(rdd : RDD[String]) =
    {
        val output = rdd.collect()
        output.foreach(println)
    }
    /*
   COC 07-05-2016 Función para la impresión de los RDDs del tipo RDD[(String,Int)] generados
   * */

    def impresion2(rdd : RDD[(String,Int)]) =
    {
        val output = rdd.collect()
        output.foreach(println)
    }
    /*
    COC 13-05-2016 Función para imprimir RDD RDD[(String,String)]
    * */
    def impresion3(rdd : RDD[(String,String)]) =
    {
        val output = rdd.collect()
        output.foreach(println)
    }
    ///////////////////////////////////////////////////////////////////////////////////////


}
