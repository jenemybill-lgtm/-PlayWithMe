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
        // Check connectivity by listing collections
        database.listCollectionNames().firstOrNull()
        println("Connected to MongoDB Atlas successfully!")
    } catch (e: Exception) {
        println("CRITICAL: MongoDB Connection Error: ${e.message}")
        e.printStackTrace()
    }
}

fun main() {
    // Start database in background to avoid blocking server start
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

                // Cleanup on disconnect
                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                rooms.values.forEach { room ->
                    if (room.players.any { it.session == this } || room.hostSession == this) {
                        if (room.isGameRunning) {
                            runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε.")) }
                        }
                        room.players.removeIf { it.session == this }
                        runBlocking { room.updateHostPlayerCount() }
                    }
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
            val name = msg.content ?: return
            val existing = usersColl.find(Filters.eq("name", name)).firstOrNull()
            if (existing != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN"))))
            } else {
                usersColl.insertOne(mapOf("name" to name))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            }
        }
        MessageType.LOGIN -> {
            val name = msg.content ?: return
            onlineUsers[name] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))

            // Sync database state: Send friend list & requests
            sendFriendList(name, session)
            sendRequestList(name, session)
            notifyFriendsStatus(name, true)

            // Send stored messages (invites/challenges) while user was offline
            pendingColl.find(Filters.eq("target", name)).toList().forEach { doc ->
                val type = MessageType.valueOf(doc["type"] as String)
                session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc["content"] as String))))
            }
            pendingColl.deleteMany(Filters.eq("target", name))
        }
        MessageType.ADD_FRIEND -> {
            val user = msg.sender
            val target = msg.content ?: return
            val targetExists = usersColl.find(Filters.eq("name", target)).firstOrNull()
            if (targetExists != null) {
                val alreadyFriends = friendsColl.find(Filters.and(Filters.eq("user", user), Filters.eq("friend", target))).firstOrNull()
                if (alreadyFriends != null) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }
                requestsColl.updateOne(Filters.eq("target", target), Updates.addToSet("requesters", user), UpdateOptions().upsert(true))
                onlineUsers[target]?.let { sendRequestList(target, it) }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
            }
        }
        MessageType.ACCEPT_REQUEST -> {
            val user = msg.sender
            val requester = msg.content ?: return
            requestsColl.updateOne(Filters.eq("target", user), Updates.pull("requesters", requester))
            friendsColl.insertOne(mapOf("user" to user, "friend" to requester))
            friendsColl.insertOne(mapOf("user" to requester, "friend" to user))
            sendFriendList(user, session)
            sendRequestList(user, session)
            onlineUsers[requester]?.let { sendFriendList(requester, it) }
        }
        MessageType.REJECT_REQUEST -> {
            val user = msg.sender
            val requester = msg.content ?: return
            requestsColl.updateOne(Filters.eq("target", user), Updates.pull("requesters", requester))
            sendRequestList(user, session)
        }
        MessageType.INVITE -> {
            val host = msg.sender
            val target = msg.content ?: return
            val inviteStr = "$host|PRIVATE_ROOM"
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.INVITE_RECEIVED, "Server", inviteStr))))
            } else {
                pendingColl.insertOne(mapOf("target" to target, "type" to "INVITE_RECEIVED", "content" to inviteStr))
            }
        }
        MessageType.CHALLENGE_FRIEND -> {
            val host = msg.sender
            val target = msg.content ?: return
            val seed = (1..999999).random().toString()
            val challengeStr = "$host|$seed"
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
            } else {
                pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RECEIVED", "content" to challengeStr))
            }
        }
        MessageType.CHALLENGE_RESULT -> {
            val parts = msg.content?.split("|") ?: return
            val target = parts[0]
            val resStr = "${msg.sender}|${parts[1]}|${parts[2]}"
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RESULT, "Server", resStr))))
            } else {
                pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RESULT", "content" to resStr))
            }
        }
        // ... Game logic (CREATE_ROOM, JOIN, START_GAME, ANSWER) follows the same pattern ...
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
    val doc = requestsColl.find(Filters.eq("target", user)).firstOrNull()
    val requesters = (doc?.get("requesters") as? List<*>)?.map { it.toString() } ?: emptyList()
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters)))))
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    friendsColl.find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc["user"] as String
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}
