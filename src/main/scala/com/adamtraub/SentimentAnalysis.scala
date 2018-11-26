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
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time.{seconds, minutes}

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable


object SentimentAnalysis {
  def main(args: Array[String]) {

    val url = ParameterTool.fromArgs(args).get("url","localhost")
    val port = ParameterTool.fromArgs(args).getInt("port", 9001)
    println("port: "+port+"\nurl:"+url)


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream(url, port)

    //val languageSet = env.fromCollection( List[LanguageServiceClient](LanguageServiceClient.create()))

    //blocks a given user's messages together every 5 seconds
    val parsedStream: DataStream[Message] =
      processMessageStream(dataStream.map { w =>
        val msg = w.split(",")
        Message(msg(0), msg(1), msg.drop(2).mkString(","))
      },(5,0))

    //blocks a given user's messages together every 100 seconds for larger, categorical analysis
    val aggregateStream: DataStream[Message] =
      processMessageStream(parsedStream,(100,0))

    //A stream to get Sentiment based on the user's input
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

    //The entity stream creates a list of entities and their corresponding sentiments
    val entityStream: DataStream[Entity] = parsedStream
      .flatMap { mData =>

        var entities = new mutable.MutableList[Entity]
        val language = LanguageServiceClient.create()
        val doc = Document.newBuilder()
          .setContent(mData.text)
          .setType(Type.PLAIN_TEXT)
          .build()

        val entitiesList = language.analyzeEntitySentiment(AnalyzeEntitySentimentRequest
            .newBuilder()
            .setDocument(doc)
            .setEncodingType(EncodingType.UTF16)
            .build())
          .getEntitiesList
        language.close()
        entitiesList
            .map { ent =>
            entities += Entity(
              key = ent.getName,
              salience = ent.getSalience,
              sentiment = Sentiment(
                ent.getSentiment.getScore,
                ent.getSentiment.getMagnitude
              )
            )
          }
        entities
      }

    val topicStream: DataStream[EntityCount] = entityStream
      .map { eData =>
        EntityCount(
          key = eData.key,
          count = 1
        )
      }
      .keyBy("key")
      .timeWindow(seconds(10))
      .sum("count")


//      val trendingStream: DataStream[EntityCount] = topicStream
//      .flatMap{ eData =>
//        var entityCounts = new mutable.MutableList[EntityCount]
//        topicStream
//          .keyBy("_")
//          .sum("count")
//          .map{ tData =>
//            entityCounts += EntityCount(
//              key = eData.key,
//              count = eData.count,
//              percentage = eData.percentage/tData.count
//            )
//          }
//        entityCounts.toList
//      }

    //This attempts to take a block of text and determine the topics/categories
    val categoryStream: DataStream[MessageCategories] = aggregateStream
      .map { mData =>
        var outList = new mutable.MutableList[Category]
        if(mData.text.split(" ").length >= 25) { //minimum 20 words for api, 25 for good measure
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
        keyExtractor = (sData: MessageSentiment) => sData.message.user:String,
        moodType = "User",
        timings = (90,60)
      )

    val channelMoodStream: DataStream[Mood] =
      buildMoodStream(
        stream = sentimentStream,
        keyExtractor = (sData: MessageSentiment) => sData.message.channel:String,
        moodType ="Channel",
        timings = (90,60)
      )

    val categoryOpinionStream: DataStream[Mood] =
      buildMoodStream(
        stream = categorySentimentStream,
        keyExtractor = (sData: CategorySentiment) => sData.category.category:String,
        moodType="Category",
        timings = (0,0)//this one runs immediately because categorical data is already slow
      )

    val entityOpinionStream: DataStream[Mood] =
      buildMoodStream(
        stream = entityStream,
        keyExtractor = (sData: Entity) => sData.key:String,
        moodType="Entity",
        timings = (30,0)
      )

    val toxicTopicStream: DataStream[Mood] =
      buildToxicityStream(
        stream = entityOpinionStream,
        sampleSize = 25,
        threshold = -20)

    val toxicUserStream: DataStream[Mood] =
      buildToxicityStream(
        stream = userMoodStream,
        sampleSize = 15,
        threshold = -8)

    val toxicChannelStream: DataStream[Mood] =
      buildToxicityStream(
        stream = userMoodStream,
        sampleSize = 50,
        threshold = -30)


    sentimentStream.print()
    entityStream.print()
    entityOpinionStream.print()
    topicStream.print()
    userMoodStream.print()
    channelMoodStream.print()
    categoryOpinionStream.print()
    toxicTopicStream.print()
    toxicUserStream.print()
    toxicChannelStream.print()

    env.execute("Slack Analysis")
  }

  //This helper method reduces some repeated code and prevents
  def getSentimentFromString(text: String): com.google.cloud.language.v1.Sentiment = {
    val language: LanguageServiceClient = LanguageServiceClient.create()
    val document = language.analyzeSentiment(Document.newBuilder()
      .setContent(text)
      .setType(Type.PLAIN_TEXT)
      .build())

    language.close()
    document.getDocumentSentiment
  }

  // This notation took a while to make work.  A <: HoldsSentiment means that A extends HoldsSentiment
  def buildMoodStream[A <: HoldsSentiment](
                          stream: DataStream[A],
                          keyExtractor: A => String,
                          moodType: String,
                          timings: (Int,Int)
                        ): DataStream[Mood] =
    processChatStream(stream.map{ sData =>
        Mood(
          key=keyExtractor(sData),
          value=sData.sentiment.magnitude*sData.sentiment.score,
          moodType=moodType
        )
      },("key",""),timings, moodReduce)

  def moodReduce(Mood1: Mood, Mood2: Mood): Mood = {
    if (!Mood1.key.equals(Mood2.key)) {
      sys.error("MOOD PROBLEM!")
    }

    Mood(
      key = Mood1.key,
      value = Mood1.value + Mood2.value,
      moodType = Mood1.moodType
    )
  }

  def processChatStream[A](
                            stream: DataStream[A],
                            keyField: (String,String),
                            timings: (Int,Int),
                            reducer: (A, A) =>  A
                          ): DataStream[A] = {

    val localStream =
      keyField match {
        case (k1, "") => stream.keyBy(k1)
        case (k1, k2) => stream.keyBy(k1, k2)
      }

      timings match {
        case (t1, 0)  if t1 > 0           => localStream.timeWindow(seconds(t1)).reduce{ reducer }
        case (t1, t2) if t1 > 0 && t2 > 0 => localStream.timeWindow(seconds(t1), seconds(t2)).reduce{ reducer }
        case _                            => localStream.reduce{ reducer }
      }
  }

  def processMessageStream(stream: DataStream[Message], timings: (Int, Int)): DataStream[Message] =
    processChatStream(stream, ("channel","user"),timings, messageReduce)


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
      .keyBy("key")
      .countWindow(sampleSize)
      .sum("value")
      .filter( _.value <= threshold )

  trait HoldsSentiment{ def sentiment: Sentiment}

  case class Message(channel: String, user: String, text: String)

  case class Sentiment(score: Float, magnitude: Float)
  case class MessageSentiment(message: Message, sentiment: Sentiment) extends HoldsSentiment

  case class Entity(key: String, salience: Float, sentiment: Sentiment) extends HoldsSentiment
  case class EntityCount(key: String, count: Int)
  case class MessageEntities(message: Message, entities: List[Entity])

  case class Mood(key: String, value: Float, moodType: String)

  case class Category(category: String, confidence: Float)
  case class MessageCategories(message: Message, categories: List[Category])
  case class CategorySentiment(message: Message, category: Category, sentiment: Sentiment) extends HoldsSentiment


}