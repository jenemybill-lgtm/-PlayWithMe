import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import io.ktor.server.application.*

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates

import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList

import org.bson.Document

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.regex.Pattern


// ================= CONFIG =================

const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11

val UPDATE_URL =
    "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"


val MONGODB_URI =
    System.getenv("MONGODB_URI")
        ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"


// ================= MODELS =================


enum class MessageType {

    CREATE_ROOM,
    JOIN,
    JOIN_RESPONSE,

    START_GAME,
    QUESTION,
    ANSWER,
    RESULT,
    LEADERBOARD,
    GAME_OVER,

    ERROR,
    RESTART,

    PLAYER_COUNT,
    VERSION_CHECK,

    REGISTER,
    REGISTER_RESPONSE,

    LOGIN,
    LOGIN_RESPONSE,

    ADD_FRIEND,
    FRIEND_LIST,

    INVITE,
    INVITE_RECEIVED,

    ACCEPT_REQUEST,
    REJECT_REQUEST,
    REQUEST_LIST,

    CHALLENGE_FRIEND,
    CHALLENGE_RECEIVED,
    CHALLENGE_RESULT
}



data class GameMessage(
    val type: MessageType,
    val sender: String,
    val content: String? = null
)



data class Player(
    val name:String,
    val session: DefaultWebSocketServerSession,

    var score:Int = 0,
    var correctCount:Int = 0,
    var wrongCount:Int = 0,

    var hasAnswered:Boolean = false,
    var lastAnswerIndex:Int = -1,

    var isEliminated:Boolean = false,

    var totalTime:Long = 0
)



data class FriendInfo(
    val name:String,
    var isOnline:Boolean
)



val onlineUsers =
    ConcurrentHashMap<String, DefaultWebSocketServerSession>()


val rooms =
    ConcurrentHashMap<String, GameRoom>()


val gson = Gson()


lateinit var database:MongoDatabase




class GameRoom(
    val code:String,
    val hostSession:DefaultWebSocketServerSession
){

    val players =
        CopyOnWriteArrayList<Player>()


    var questions:List<Map<String,Any>> =
        emptyList()


    var currentQuestionIndex = 0


    var timerSeconds = 20


    var waitingForAnswers=false


    var isGameRunning=false


    var gameJob:Job? = null



    suspend fun broadcast(message:GameMessage){

        val text =
            gson.toJson(message)


        val sessions =
            players.map{it.session}
                .toMutableSet()


        sessions.add(hostSession)



        for(session in sessions){

            try{

                session.send(
                    Frame.Text(text)
                )

            }catch(_:Exception){}

        }
    }




    suspend fun updateHostPlayerCount(){

        val msg =
            GameMessage(
                MessageType.PLAYER_COUNT,
                "Server",
                players.size.toString()
            )


        try{

            hostSession.send(
                Frame.Text(
                    gson.toJson(msg)
                )
            )

        }catch(_:Exception){}

    }

}



// ================= DATABASE =================


suspend fun initDatabase(){

    try{

        val client =
            MongoClient.create(MONGODB_URI)


        database =
            client.getDatabase("playwithme")


        println(
            "SERVER: DATABASE CONNECTED"
        )


    }catch(e:Exception){

        println(
            "DATABASE ERROR ${e.message}"
        )

    }

}



// ================= MAIN =================



fun main(){


    GlobalScope.launch{

        initDatabase()

    }



    val port =
        System.getenv("PORT")
            ?.toInt()
            ?:8080



    embeddedServer(
        Netty,
        port=port,
        host="0.0.0.0"

    ){


        install(WebSockets)



        routing{


            webSocket("/ws"){


                send(
                    Frame.Text(
                        gson.toJson(
                            GameMessage(
                                MessageType.VERSION_CHECK,
                                "Server",
                                "$LATEST_VERSION_CODE|$UPDATE_URL|$LATEST_VERSION_NAME"
                            )
                        )
                    )
                )



                try{


                    for(frame in incoming){


                        if(frame is Frame.Text){


                            val text =
                                frame.readText()



                            val msg =
                                gson.fromJson(
                                    text,
                                    GameMessage::class.java
                                )



                            handleMessage(
                                this,
                                msg
                            )

                        }

                    }


                }catch(_:Exception){}



                // cleanup

                var disconnected:String?=null


                for(entry in onlineUsers){

                    if(entry.value==this){

                        disconnected=entry.key
                        break

                    }

                }



                if(disconnected!=null){

                    onlineUsers.remove(disconnected)


                    notifyFriendsStatus(
                        disconnected,
                        false
                    )

                }


            }

        }


    }.start(wait=true)

}



// ================= HELPERS =================


suspend fun canonicalName(
    users:MongoCollection<Document>,
    raw:String
):String{


    val doc =
        users.find(
            Filters.regex(
                "name",
                "^${Pattern.quote(raw)}$",
                "i"
            )
        )
        .toList()
        .firstOrNull()



    return doc?.getString("name")
        ?:raw

}
suspend fun handleMessage(
    session: DefaultWebSocketServerSession,
    msg: GameMessage
){

    if(!::database.isInitialized)
        return


    val usersColl =
        database.getCollection<Document>("users")


    val friendsColl =
        database.getCollection<Document>("friends")


    val requestsColl =
        database.getCollection<Document>("requests")



    when(msg.type){


        MessageType.LOGIN -> {


            val name =
                msg.content?.trim()
                    ?:return


            val official =
                canonicalName(
                    usersColl,
                    name
                )


            onlineUsers[official]=session



            session.send(
                Frame.Text(
                    gson.toJson(
                        GameMessage(
                            MessageType.LOGIN_RESPONSE,
                            "Server",
                            "OK"
                        )
                    )
                )
            )


            sendFriendList(
                official,
                session
            )


            sendRequestList(
                official,
                session
            )


            notifyFriendsStatus(
                official,
                true
            )

        }




        // ================= FIX FRIEND REQUEST =================


        MessageType.ADD_FRIEND -> {


            val user =
                canonicalName(
                    usersColl,
                    msg.sender.trim()
                )


            val target =
                msg.content?.trim()
                    ?:return



            val targetDoc =
                usersColl.find(
                    Filters.regex(
                        "name",
                        "^${Pattern.quote(target)}$",
                        "i"
                    )
                )
                .toList()
                .firstOrNull()



            if(targetDoc==null){


                session.send(
                    Frame.Text(
                        gson.toJson(
                            GameMessage(
                                MessageType.ERROR,
                                "Server",
                                "Ο παίκτης δεν βρέθηκε!"
                            )
                        )
                    )
                )


                return
            }



            val officialTarget =
                targetDoc.getString("name")




            requestsColl.updateOne(

                Filters.eq(
                    "target",
                    officialTarget
                ),

                Updates.combine(

                    Updates.addToSet(
                        "requesters",
                        user
                    ),

                    Updates.setOnInsert(
                        "target",
                        officialTarget
                    )
                ),


                UpdateOptions()
                    .upsert(true)
            )



            println(
                "REQUEST SAVED $user -> $officialTarget"
            )




            val receiver =
                onlineUsers.entries
                    .firstOrNull{
                        it.key.equals(
                            officialTarget,
                            true
                        )
                    }



            if(receiver!=null){


                sendRequestList(
                    officialTarget,
                    receiver.value
                )



                receiver.value.send(
                    Frame.Text(
                        gson.toJson(
                            GameMessage(
                                MessageType.ERROR,
                                "Server",
                                "Νέο αίτημα από $user!"
                            )
                        )
                    )
                )


            }



            session.send(
                Frame.Text(
                    gson.toJson(
                        GameMessage(
                            MessageType.ERROR,
                            "Server",
                            "Το αίτημα στάλθηκε!"
                        )
                    )
                )
            )

        }






        MessageType.ACCEPT_REQUEST -> {


            val user =
                canonicalName(
                    usersColl,
                    msg.sender
                )


            val requester =
                canonicalName(
                    usersColl,
                    msg.content ?: return
                )



            requestsColl.updateOne(
                Filters.eq(
                    "target",
                    user
                ),

                Updates.pull(
                    "requesters",
                    requester
                )
            )



            friendsColl.insertOne(
                Document()
                    .append("user",user)
                    .append("friend",requester)
            )



            friendsColl.insertOne(
                Document()
                    .append("user",requester)
                    .append("friend",user)
            )



            sendFriendList(
                user,
                session
            )


            sendRequestList(
                user,
                session
            )

        }






        MessageType.CREATE_ROOM -> {


            val code =
                (10000..99999)
                    .random()
                    .toString()



            rooms[code] =
                GameRoom(
                    code,
                    session
                )
                .apply{

                    players.add(
                        Player(
                            msg.sender,
                            session
                        )
                    )

                }



            session.send(
                Frame.Text(
                    gson.toJson(
                        GameMessage(
                            MessageType.JOIN_RESPONSE,
                            "Server",
                            code
                        )
                    )
                )
            )

        }





        MessageType.JOIN -> {


            val room =
                rooms[msg.content]



            if(room!=null){


                if(
                    room.players.none{
                        it.session==session
                    }
                ){

                    room.players.add(
                        Player(
                            msg.sender,
                            session
                        )
                    )

                }


                room.broadcast(
                    GameMessage(
                        MessageType.JOIN_RESPONSE,
                        "Server",
                        room.code
                    )
                )


            }

        }



        else -> {}

    }

}







suspend fun sendFriendList(
    user:String,
    session:DefaultWebSocketServerSession
){

    val friends =
        database
            .getCollection<Document>("friends")
            .find(
                Filters.eq(
                    "user",
                    user
                )
            )
            .toList()
            .map{

                FriendInfo(
                    it.getString("friend"),
                    onlineUsers.containsKey(
                        it.getString("friend")
                    )
                )

            }



    session.send(
        Frame.Text(
            gson.toJson(
                GameMessage(
                    MessageType.FRIEND_LIST,
                    "Server",
                    gson.toJson(friends)
                )
            )
        )
    )

}







suspend fun sendRequestList(
    user:String,
    session:DefaultWebSocketServerSession
){


    val requests =
        database
            .getCollection<Document>("requests")
            .find(
                Filters.eq(
                    "target",
                    user
                )
            )
            .firstOrNull()



    val list =
        requests
            ?.get(
                "requesters",
                List::class.java
            )
            ?.map{
                it.toString()
            }
            ?: emptyList()



    session.send(
        Frame.Text(
            gson.toJson(
                GameMessage(
                    MessageType.REQUEST_LIST,
                    "Server",
                    gson.toJson(list)
                )
            )
        )
    )


}






suspend fun notifyFriendsStatus(
    user:String,
    online:Boolean
){

    val friends =
        database
            .getCollection<Document>("friends")
            .find(
                Filters.eq(
                    "friend",
                    user
                )
            )
            .toList()



    for(doc in friends){


        val name =
            doc.getString("user")
                ?:continue



        onlineUsers[name]?.let{

            sendFriendList(
                name,
                it
            )

        }

    }

}
