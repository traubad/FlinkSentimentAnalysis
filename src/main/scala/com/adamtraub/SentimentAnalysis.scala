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

    val url = "localhost"
    val port = 9001


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream(url, port)

    //blocks a given user's messages together every 5 seconds
    val parsedStream: DataStream[Message] =
      processMessageStream(dataStream.map { w =>
        val msg = w.split(",")
        Message(msg(0), msg(1), msg.drop(2).mkString(","))
      },5)

    //blocks a given user's messages together every 100 seconds
    val aggregateStream: DataStream[Message] =
      processMessageStream(parsedStream,100)

    val sentimentStream: DataStream[MessageSentiment] = parsedStream
      .map { mData =>
        //var sentiments = new mutable.MutableList[MessageSentiment]
        val docSentiment = getSentimentFromString(mData.text)
        MessageSentiment(mData, Sentiment(docSentiment.getScore, docSentiment.getMagnitude))

//        sentiments += MessageSentiment(mData, Sentiment(docSentiment.getScore, docSentiment.getMagnitude))
//        if(document.getSentencesCount > 1) {
//          document.getSentencesList.map { sent =>
//            sentiments += MessageSentiment(
//              message = Message(
//                channel = mData.channel,
//                user = mData.user,
//                text = sent.getText.getContent
//              ),
//              sentiment = Sentiment(sent.getSentiment.getScore, sent.getSentiment.getMagnitude)
//            )
//          }
//        }
//        sentiments.toList
      }

    val entityStream: DataStream[MessageEntities] = parsedStream
      .map { mData =>

        var entities = new mutable.MutableList[Entity]
        val language = LanguageServiceClient.create()
        val doc = Document.newBuilder()
          .setContent(mData.text)
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

        language.close()
        MessageEntities(
          message = mData,
          entities = entities.toList
        )
      }

    val categoryStream: DataStream[MessageCategories] = aggregateStream
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
          language.close()
        }
        MessageCategories(mData, outList.toList)
      }

    val categorySentimentStream: DataStream[CategorySentiment] = categoryStream
      .flatMap { mCat =>
        val docSentiment = getSentimentFromString(mCat.message.text)
        val sentiment = Sentiment(docSentiment.getScore, docSentiment.getMagnitude)

        mCat.categories
          .map{ cat =>
            CategorySentiment(mCat.message,cat,sentiment)
          }
      }

    val userMoodStream: DataStream[Mood] =
      buildMoodStream(
        stream = sentimentStream,
        entityExtractor = sData => sData.message.user,
        moodType = "User",
        timings = (90,60))

    val channelMoodStream: DataStream[Mood] =
      buildMoodStream(
        stream = sentimentStream,
        entityExtractor = sData => sData.message.channel,
        moodType ="Channel",
        timings = (90,60))

    val categoryOpinionStream: DataStream[Mood] =
      buildMoodStream(
        stream = categorySentimentStream,
        entityExtractor = sData => sData.category.category,
        timings = (0,0))

    val toxicUserStream: DataStream[Mood] =
      buildToxicityStream(
        stream = userMoodStream,
        sampleSize = 10,
        threshold = -8)

    val toxicChannelStream: DataStream[Mood] =
      buildToxicityStream(
        stream = userMoodStream,
        sampleSize = 50,
        threshold = -30)


    //sentimentStream.print()
    //entityStream.print()
    userMoodStream.print()
    channelMoodStream.print()
    toxicUserStream.print()
    toxicChannelStream.print()
    categoryOpinionStream.print()

    env.execute("Slack Analysis")
  }

  def getSentimentFromString(text: String): com.google.cloud.language.v1.Sentiment = {
    val language = LanguageServiceClient.create()
    val document = language.analyzeSentiment(Document.newBuilder()
      .setContent(text)
      .setType(Type.PLAIN_TEXT)
      .build())

    language.close()
    document.getDocumentSentiment
  }


  def buildMoodStream(stream: DataStream[MessageSentiment],
                  entityExtractor: MessageSentiment => String,
                  moodType: String, timings: (Int,Int)): DataStream[Mood] =
    processMoodStream(stream.map{ sData =>
        Mood(
          entity=entityExtractor(sData),
          value=sData.sentiment.magnitude*sData.sentiment.score,
          moodType=moodType
        )
      },timings)

  def buildMoodStream(stream: DataStream[CategorySentiment],
                  entityExtractor: CategorySentiment => String,
                  timings: (Int,Int)): DataStream[Mood] =
    processMoodStream(stream.map{ sData =>
      Mood(
        entity=entityExtractor(sData),
        value=sData.sentiment.magnitude*sData.sentiment.score,
        moodType="Category"
      )
    },timings)

  def processMoodStream(stream: DataStream[Mood], timings: (Int,Int)): DataStream[Mood] =
    stream
      .keyBy("entity")
      .timeWindow(Time.seconds(timings._1),Time.seconds(timings._2))
      .reduce { (Mood1, Mood2) => moodReduce(Mood1, Mood2) }

  def moodReduce(Mood1: Mood, Mood2: Mood): Mood = {
    if (!Mood1.entity.equals(Mood2.entity)) {
      sys.error("MOOD PROBLEM!")
    }

    Mood(
      entity = Mood1.entity,
      value = Mood1.value + Mood2.value,
      moodType = Mood1.moodType
    )
  }

  def processMessageStream(stream: DataStream[Message], timing: Int): DataStream[Message] =
    stream
      .keyBy("channel", "user")
      .timeWindow(Time.seconds(timing))
      .reduce{(message1, message2) => messageReduce(message1, message2)}

  def processMessageStream(stream: DataStream[Message], timings: (Int, Int)): DataStream[Message] =
    stream
      .keyBy("channel", "user")
      .timeWindow(Time.seconds(timings._1),Time.seconds(timings._2))
      .reduce{(message1, message2) => messageReduce(message1, message2)}

  def messageReduce(message1: Message, message2: Message) : Message = {
    if (!message1.user.equals(message2.user)) {
      sys.error("AGGREGATION PROBLEMS")
    }
    Message(
      channel = message1.channel,
      user = message1.user,
      text = message1.text + "\n" + message2.text
    )
  }

  def buildToxicityStream(stream: DataStream[Mood],
                          sampleSize: Int,
                          threshold: Int): DataStream[Mood] =
    stream
      .keyBy("entity")
      .countWindow(sampleSize)
      .sum("value")
      .filter( _.value <= threshold )

    abstract class TextData {}

    case class Message(channel: String, user: String, text: String) extends TextData

    case class Sentiment(score: Float, magnitude: Float)
    case class Entity(entity: String, salience: Float, sentiment: Sentiment)

    case class MessageSentiment(message: Message, sentiment: Sentiment)
    case class MessageEntities(message: Message, entities: List[Entity])

    case class Mood(entity: String, value: Float, moodType: String)

    case class Category(category: String, confidence: Float)
    case class MessageCategories(message: Message, categories: List[Category])
    case class CategorySentiment(message: Message, category: Category, sentiment: Sentiment) extends TextData
}