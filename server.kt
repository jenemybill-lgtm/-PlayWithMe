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
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.regex.Pattern

// --- CONFIGURATION v2.0 (FINAL ULTRA STABLE) ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

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
    val players = CopyOnWriteArrayList<Player>()
    var questions: List<Map<String, Any>> = emptyList()
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isGameRunning = false
    var gameJob: Job? = null

    suspend fun broadcast(message: GameMessage) {
        val text = gson.toJson(message)
        val sessions = players.map { it.session }.toMutableSet()
        sessions.add(hostSession)
        for (session in sessions) {
            try { session.send(Frame.Text(text)) } catch(e: Exception) {}
        }
    }

    suspend fun updateHostPlayerCount() {
        val msg = GameMessage(MessageType.PLAYER_COUNT, "Server", "${players.size}")
        try { hostSession.send(Frame.Text(gson.toJson(msg))) } catch(e: Exception) {}
    }
}

suspend fun initDatabase() {
    try {
        val client = MongoClient.create(MONGODB_URI)
        database = client.getDatabase("playwithme")
        println("SERVER: !!! DATABASE CONNECTED !!!")
    } catch (e: Exception) { println("DATABASE ERROR: ${e.message}") }
}

fun main() {
    GlobalScope.launch { initDatabase() }
    val port = System.getenv("PORT")?.toInt() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                send(Frame.Text(gson.toJson(GameMessage(MessageType.VERSION_CHECK, "Server", "$LATEST_VERSION_CODE|$UPDATE_URL|$LATEST_VERSION_NAME"))))
                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val text = frame.readText()
                            val msg = gson.fromJson(text, GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) { }

                // --- CLEANUP (THREAD-SAFE FOR LOOPS) ---
                var disconnectedUser: String? = null
                for (entry in onlineUsers.entries) {
                    if (entry.value == this) {
                        disconnectedUser = entry.key
                        break
                    }
                }
                
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                val iterator = rooms.entries.iterator()
                while (iterator.hasNext()) {
                    val entry = iterator.next()
                    val room = entry.value
                    if (room.hostSession == this || room.players.any { it.session == this }) {
                        room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε."))
                        iterator.remove()
                    }
                }
            }
        }
    }.start(wait = true)
}

suspend fun canonicalName(usersColl: MongoCollection<Document>, raw: String): String {
    val doc = usersColl.find(Filters.regex("name", "^${Pattern.quote(raw)}$", "i")).toList().firstOrNull()
    return if (doc != null) doc.getString("name") else raw
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) return
    val usersColl = database.getCollection<Document>("users")
    val friendsColl = database.getCollection<Document>("friends")
    val requestsColl = database.getCollection<Document>("requests")
    val pendingColl = database.getCollection<Document>("pending_messages")

    when (msg.type) {
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            val officialName = canonicalName(usersColl, name)
            println("SERVER: Login for $officialName")
            onlineUsers[officialName] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            
            // CRITICAL SYNC
            sendFriendList(officialName, session)
            sendRequestList(officialName, session)
            notifyFriendsStatus(officialName, true)

            for (doc in pendingColl.find(Filters.regex("target", "^${Pattern.quote(officialName)}$", "i")).toList()) {
                try {
                    val type = MessageType.valueOf(doc.getString("type"))
                    session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc.getString("content")))))
                } catch (e: Exception) {}
            }
            pendingColl.deleteMany(Filters.regex("target", "^${Pattern.quote(officialName)}$", "i"))
        }

        MessageType.ADD_FRIEND -> {
            val user = msg.sender.trim(); val target = msg.content?.trim() ?: return
            val targetDoc = usersColl.find(Filters.regex("name", "^${Pattern.quote(target)}$", "i")).toList().firstOrNull()
            if (targetDoc != null) {
                val officialTarget = targetDoc.getString("name")
                if (friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", officialTarget))) == 0L) {
                    requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"), 
                        Updates.combine(Updates.addToSet("requesters", user), Updates.setOnInsert("target", officialTarget)), UpdateOptions().upsert(true))
                    
                    for (entry in onlineUsers.entries) {
                        if (entry.key.equals(officialTarget, ignoreCase = true)) {
                            sendRequestList(entry.key, entry.value)
                            entry.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
                        }
                    }
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
                } else session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
            } else session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
        }

        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                room.gameJob?.cancel()
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                room.questions = setup["questions"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()
                room.isGameRunning = true; room.currentQuestionIndex = 0; room.waitingForAnswers = false
                for (p in room.players) { p.score = 0; p.totalTime = 0; p.hasAnswered = false; p.lastAnswerIndex = -1 }
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", room.timerSeconds.toString()))
                room.gameJob = CoroutineScope(Dispatchers.Default).launch { delay(2000); runGameLoop(room) }
            }
        }

        MessageType.ANSWER -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && room.waitingForAnswers && player != null && !player.hasAnswered) {
                val parts = msg.content?.split("|") ?: return
                player.hasAnswered = true
                player.lastAnswerIndex = parts[0].toIntOrNull() ?: -1
                player.totalTime += parts.getOrNull(1)?.toLongOrNull() ?: 0
                
                var allAnswered = true
                for (p in room.players) { if (!p.hasAnswered) allAnswered = false }
                if (allAnswered) room.waitingForAnswers = false
            }
        }
        
        MessageType.CREATE_ROOM -> {
            val code = (10000..99999).random().toString()
            rooms[code] = GameRoom(code, session).apply { players.add(Player(msg.sender, session)) }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
        }

        MessageType.JOIN -> {
            val room = rooms[msg.content]
            if (room != null) {
                if (room.players.none { it.session == session }) room.players.add(Player(msg.sender, session))
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", room.code))
                room.updateHostPlayerCount()
            } else session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το δωμάτιο δεν βρέθηκε!"))))
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Document>("friends").find(Filters.eq("user", user)).toList().map { doc ->
        FriendInfo(doc.getString("friend"), onlineUsers.containsKey(doc.getString("friend")))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        println("SERVER: Scanning database for $user")
        val requestsColl = database.getCollection<Document>("requests")
        val doc = requestsColl.find(Filters.regex("target", "^${Pattern.quote(user)}$", "i")).toList().firstOrNull()
        val rawRequesters = doc?.get("requesters", List::class.java)
        val requesters = rawRequesters?.map { it.toString() } ?: emptyList()
        
        println("SERVER: Found ${requesters.size} requests for $user")
        val msg = GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters))
        session.send(Frame.Text(gson.toJson(msg)))
    } catch (e: Exception) { println("SERVER ERROR: ${e.message}") }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    for (doc in database.getCollection<Document>("friends").find(Filters.eq("friend", user)).toList()) {
        val friendName = doc.getString("user")
        val sess = onlineUsers[friendName]
        if (sess != null) sendFriendList(friendName, sess)
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        val question = room.questions[room.currentQuestionIndex]
        val correctIdx = (question["correctAnswerIndex"] as Double).toInt()
        for (p in room.players) { p.hasAnswered = false; p.lastAnswerIndex = -1 }
        room.waitingForAnswers = true
        room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))

        var elapsedMs = 0
        while (elapsedMs < (room.timerSeconds * 1000) && room.waitingForAnswers && room.isGameRunning) {
            delay(200); elapsedMs += 200 }
        
        room.waitingForAnswers = false
        for (p in room.players) { if (p.lastAnswerIndex == correctIdx) p.score++ }
        val options = question["options"] as List<String>
        room.broadcast(GameMessage(MessageType.RESULT, "Server", "Σωστή: ${options[correctIdx]}"))
        delay(3000)
        room.currentQuestionIndex++
    }
    if (room.isGameRunning) {
        val finalRank = room.players.sortedWith(compareByDescending<Player> { it.score }.thenBy { it.totalTime })
        val rankingText = finalRank.withIndex().joinToString("\n") { "${it.index + 1}. ${it.value.name}: ${it.value.score}" }
        room.broadcast(GameMessage(MessageType.GAME_OVER, "Server", "ΤΕΛΙΚΗ ΚΑΤΑΤΑΞΗ:\n$rankingText"))
        room.isGameRunning = false
    }
}
