package com.vdaso.tlogs.modelo

import java.io._
import java.util.Properties


/**
  * Created by dpro on 8/06/16.
  */
object Prop {

    def propesties : Properties = {
       val p =new Properties()
       p.load(getClass.getResource("/app.properties").openStream())
       return p
    }

    val azure_key = propesties.getProperty("azure.key")
    val azure_account = propesties.getProperty("azure.account")
    val persistence_method = propesties.getProperty("persistence.method")
    val filesystem_output = propesties.getProperty("filesystem.output")
    val filesystem_input = propesties.getProperty("filesystem.input")

}
