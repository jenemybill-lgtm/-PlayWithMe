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
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// --- CONFIGURATION ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
const val MONGODB_URI = "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"
// ---------------------

enum class MessageType {
    CREATE_ROOM, JOIN, JOIN_RESPONSE, START_GAME, QUESTION, ANSWER, RESULT, LEADERBOARD, GAME_OVER, ERROR, RESTART, PLAYER_COUNT, VERSION_CHECK,
    REGISTER, REGISTER_RESPONSE, LOGIN, LOGIN_RESPONSE, ADD_FRIEND, FRIEND_LIST, INVITE, INVITE_RECEIVED,
    ACCEPT_REQUEST, REJECT_REQUEST, REQUEST_LIST,
    CHALLENGE_FRIEND, CHALLENGE_RECEIVED, CHALLENGE_RESULT
}

data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(val name: String, val session: DefaultWebSocketServerSession, var score: Int = 0, var correctCount: Int = 0, var wrongCount: Int = 0, var hasAnswered: Boolean = false, var lastAnswerIndex: Int = -1, var isEliminated: Boolean = false, var totalTime: Long = 0)
data class FriendInfo(val name: String, var isOnline: Boolean)

val onlineUsers = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val rooms = ConcurrentHashMap<String, GameRoom>()
val gson = Gson()

lateinit var database: MongoDatabase

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = mutableListOf<Player>()
    var questions: List<Map<String, Any>> = emptyList()
    var extraQuestions: List<Map<String, Any>> = emptyList()
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isSuddenDeathEnabled = true
    var isSpeedMode = false
    var isSuddenDeathActive = false
    var isGameRunning = false

    suspend fun broadcast(message: GameMessage) {
        val text = gson.toJson(message)
        val uniqueSessions = players.map { it.session }.toMutableSet()
        uniqueSessions.add(hostSession)
        uniqueSessions.forEach { try { it.send(Frame.Text(text)) } catch(e: Exception) {} }
    }

    suspend fun updateHostPlayerCount() {
        val msg = GameMessage(MessageType.PLAYER_COUNT, "Server", "${players.size}")
        try { hostSession.send(Frame.Text(gson.toJson(msg))) } catch(e: Exception) {}
    }
}

suspend fun initDatabase() {
    try {
        println("Connecting to MongoDB...")
        val client = MongoClient.create(MONGODB_URI)
        database = client.getDatabase("playwithme")
        println("Connected to MongoDB Atlas successfully!")
    } catch (e: Exception) {
        println("CRITICAL: MongoDB Connection Error: ${e.message}")
        e.printStackTrace()
    }
}

fun main() {
    GlobalScope.launch { initDatabase() }
    val port = System.getenv("PORT")?.toInt() ?: 8080
    println("Starting server on port $port...")
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                val versionMsg = GameMessage(MessageType.VERSION_CHECK, "Server", "$LATEST_VERSION_CODE|$UPDATE_URL|$LATEST_VERSION_NAME")
                try { send(Frame.Text(gson.toJson(versionMsg))) } catch(e: Exception) {}

                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val text = frame.readText()
                            val msg = gson.fromJson(text, GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) { e.printStackTrace() }

                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) {
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Database connecting..."))))
        return
    }

    val usersColl = database.getCollection<Map<String, Any>>("users")
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    val requestsColl = database.getCollection<Map<String, Any>>("requests")
    val pendingColl = database.getCollection<Map<String, Any>>("pending_messages")

    when (msg.type) {
        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            val count = usersColl.countDocuments(Filters.eq("name", name))
            if (count > 0) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN"))))
            } else {
                usersColl.insertOne(mapOf("name" to name))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            }
        }
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            onlineUsers[name] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(name, session)
            sendRequestList(name, session)
            notifyFriendsStatus(name, true)
            pendingColl.find(Filters.eq("target", name)).toList().forEach { doc ->
                val typeStr = doc["type"] as String
                val type = MessageType.valueOf(typeStr)
                session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc["content"] as String))))
            }
            pendingColl.deleteMany(Filters.eq("target", name))
        }
        MessageType.ADD_FRIEND -> {
            val user = msg.sender.trim()
            val target = msg.content?.trim() ?: return
            val targetExists = usersColl.countDocuments(Filters.eq("name", target))
            if (targetExists > 0) {
                val isAlreadyFriend = friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", target)))
                if (isAlreadyFriend > 0) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }
                requestsColl.updateOne(Filters.eq("target", target), Updates.addToSet("requesters", user), UpdateOptions().upsert(true))
                onlineUsers[target]?.let { targetSession ->
                    sendRequestList(target, targetSession)
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
                }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
            }
        }
        MessageType.ACCEPT_REQUEST -> {
            val user = msg.sender.trim()
            val requester = msg.content?.trim() ?: return
            requestsColl.updateOne(Filters.eq("target", user), Updates.pull("requesters", requester))
            friendsColl.insertOne(mapOf("user" to user, "friend" to requester))
            friendsColl.insertOne(mapOf("user" to requester, "friend" to user))
            sendFriendList(user, session)
            sendRequestList(user, session)
            onlineUsers[requester]?.let { sendFriendList(requester, it); sendRequestList(requester, it) }
        }
        MessageType.REJECT_REQUEST -> {
            val user = msg.sender.trim()
            val requester = msg.content?.trim() ?: return
            requestsColl.updateOne(Filters.eq("target", user), Updates.pull("requesters", requester))
            sendRequestList(user, session)
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    val friends = friendsColl.find(Filters.eq("user", user)).toList().map { doc ->
        val fName = doc["friend"] as String
        FriendInfo(fName, onlineUsers.containsKey(fName))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val requestsColl = database.getCollection<Map<String, Any>>("requests")
    val doc = requestsColl.find(Filters.eq("target", user)).toList().firstOrNull()
    val requesters = (doc?.get("requesters") as? List<*>)?.map { it.toString() } ?: emptyList()
    val msg = GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters))
    session.send(Frame.Text(gson.toJson(msg)))
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    friendsColl.find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc["user"] as String
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}
