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
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.mutable


object SentimentAnalysis {
  def main(args: Array[String]) {

    //val url = "localhost"
    val url = "73.251.32.82"
    val port = 9001


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream(url, port)

    val parsedStream = dataStream.map { w =>
      val msg = w.split(",")
      Message(msg(0), msg(1), msg.drop(2).mkString(","))
    }

    val aggregateStream = parsedStream
      .keyBy("user", "channel")
      .timeWindow(Time.seconds(90),Time.seconds(60))
      .reduce{(Message1, Message2) =>

        if (!Message1.user.equals(Message2.user)) {
          sys.error("MAJOR PROBLEMS")
        }
        Message(
          channel = Message1.channel,
          user = Message1.user,
          text = Message1.text +" "+ Message2.text
        )
      }

    val sentimentStream = parsedStream
      .map { mData =>

        val language = LanguageServiceClient.create()
        val sentiment = language.analyzeSentiment(Document.newBuilder()
          .setContent(mData.text)
          .setType(Type.PLAIN_TEXT)
          .build()).getDocumentSentiment

        language.close

        MessageSentiment(mData, Sentiment(sentiment.getScore, sentiment.getMagnitude))
      }

    val sentimentSentenceStream = parsedStream
      .map { mData =>

        val language = LanguageServiceClient.create()
        val document = language.analyzeSentiment(Document.newBuilder()
          .setContent(mData.text)
          .setType(Type.PLAIN_TEXT)
          .build())
        language.close

        document.getSentencesList.map{ sent =>
          MessageSentiment(
            message = Message(
              channel = mData.channel,
              user = mData.user,
              text = sent.getText.getContent
            ),
            sentiment = Sentiment(sent.getSentiment.getScore, sent.getSentiment.getMagnitude)
          )
        }
      }

    val entityStream = sentimentStream
      .map { mData =>

        var entities = new MutableList[Entity]
        val language = LanguageServiceClient.create()
        val doc = Document.newBuilder()
          .setContent(mData.message.text)
          .setType(Type.PLAIN_TEXT)
          .build()

        language.analyzeEntities(AnalyzeEntitiesRequest
            .newBuilder()
            .setDocument(doc)
            .setEncodingType(EncodingType.UTF16)
            .build())
          .getEntitiesList
          .map { ent =>
            entities += Entity(
              ent.getName,
              ent.getSalience,
              Sentiment(
                ent.getSentiment.getScore,
                ent.getSentiment.getMagnitude
              )
            )
          }

        language.close
        MessageSentimentEntities(
          message = mData.message,
          sentiment = mData.sentiment,
          entities = entities.toList
        )
      }

    val categoryStream = aggregateStream
      .map { mData =>

        var outList = new mutable.MutableList[Category]

        if(mData.text.split(" ").length >= 25) {
          val language = LanguageServiceClient.create()

          val doc = Document.newBuilder()
            .setContent(mData.text)
            .setType(Type.PLAIN_TEXT)
            .build()

          language
            .classifyText(ClassifyTextRequest.newBuilder()
              .setDocument(doc)
              .build())
            .getCategoriesList
            .map { cat =>
              outList += Category(cat.getName, cat.getConfidence)
            }
          language.close
        } else {
          outList += Category("N/A Not enough data", 0)
        }
        MessageCategories(mData, outList.toList)
      }

      sentimentSentenceStream.print()
      entityStream.print()
      categoryStream.print()

      env.execute("Slack Analysis")
    }


    case class Message(channel: String, user: String, text: String)
    case class Sentiment(score: Float, magnitude: Float)
    case class MessageSentiment(message: Message, sentiment: Sentiment)
    case class MessageSentimentEntities(message: Message, sentiment: Sentiment, entities: List[Entity])
    case class Entity(entity: String, salience: Float, sentiment: Sentiment)
    case class Annotation()

    case class Category(category: String, confidence: Float)
    case class MessageCategories(message: Message, categories: List[Category])
}