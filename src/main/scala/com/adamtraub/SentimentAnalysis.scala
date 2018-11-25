/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adamtraub

import com.google.cloud.language.v1._
import com.google.cloud.language.v1.Document.Type
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList

object SentimentAnalysis {
  def main(args: Array[String]) {

      //val url = "localhost"
      val url = "73.251.32.82"
      val port = 9001


      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val dataStream = env.socketTextStream(url, port)

      val chatText = dataStream
        .flatMap { w =>
          val text = w.split(",")
            .drop(2)
            .mkString(",")

          var outList = new MutableList[TextData]

          val language = LanguageServiceClient.create()

          val doc = Document.newBuilder()
            .setContent(text)
            .setType(Type.PLAIN_TEXT)
            .build()

          val syntax = language.analyzeSyntax(AnalyzeSyntaxRequest.newBuilder()
            .setDocument(doc)
            .setEncodingType(EncodingType.UTF16)
            .build())

          val sentiment = language.analyzeSentiment(doc).getDocumentSentiment

          if(text.split(" ").length >= 25){
            val classification = language.classifyText(ClassifyTextRequest.newBuilder()
              .setDocument(doc)
              .build())
            for (cat <- classification.getCategoriesList){
              outList += contentData(cat.getName, cat.getConfidence)
            }
          }

          outList += sentimentData(text, sentiment.getScore, sentiment.getMagnitude)
        }
      chatText.print()
      env.execute("Text Analysis")
    }

    abstract class TextData {}
    case class sentimentData(text: String, score: Float, magnitude: Float) extends TextData
    case class chatData(room: String, user: String, message: String) extends TextData
    case class contentData(category: String, confidence: Float) extends TextData
}



//      val chatText = text
//        .map { w =>
//          val split = w.split(",")
//          if(split.length >= 3) chatData(split(0), split(1), split.drop(2).mkString(","))
//          else chatData("Unknown", "Unknown", split.mkString(","))
//        }
//        .keyBy("room")
//

//  val  chatMessage = text
//    .map { w =>
//      val split = w.split(",")
//      chatData(split(0), split(1), split.drop(2).mkString(","))
//    }
//    .keyBy("room")


//    val text = "I fucking hate sharks"
//    val doc = Document.newBuilder()
//              .setContent(text)
//              .setType(Type.PLAIN_TEXT)
//              .build()
//    val sentiment = language.analyzeSentiment(doc).getDocumentSentiment()
//    val x =sentimentData(text, sentiment.getScore(), sentiment.getMagnitude())
//    println("Text:"+x.text)
//    println("Score:"+x.score)
//    println("Magnitude:"+x.magnitude)
