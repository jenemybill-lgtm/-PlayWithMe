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
import com.mongodb.client.model.Sorts
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.bson.Document
import org.bson.conversions.Bson
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.regex.Pattern

// --- CONFIGURATION v4.1 (FIXED) ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: error("MONGODB_URI environment variable is required")

enum class MessageType {
    CREATE_ROOM, JOIN, JOIN_RESPONSE, START_GAME, QUESTION, ANSWER, RESULT, LEADERBOARD, GAME_OVER, ERROR, RESTART, PLAYER_COUNT, VERSION_CHECK,
    REGISTER, REGISTER_RESPONSE, LOGIN, LOGIN_RESPONSE, ADD_FRIEND, FRIEND_LIST, INVITE, INVITE_RECEIVED,
    ACCEPT_REQUEST, REJECT_REQUEST, REQUEST_LIST,
    CHALLENGE_FRIEND, CHALLENGE_RECEIVED, CHALLENGE_RESULT,
    GET_LEADERBOARD, LEADERBOARD_DATA,
    UPLOAD_QUESTIONS, GET_SOLO_QUESTIONS, SOLO_QUESTIONS_DATA,
    SUGGEST_QUESTION, SUGGEST_QUESTION_RESPONSE,
    GET_PENDING_QUESTIONS, PENDING_QUESTIONS_DATA,
    APPROVE_QUESTION, REJECT_QUESTION, QUESTION_MODERATION_RESPONSE,
    SYNC_OFFLINE_SCORES, SYNC_OFFLINE_SCORES_RESPONSE,
    CHECK_NEW_QUESTIONS, NEW_QUESTIONS_DATA
}

data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(val name: String, val session: DefaultWebSocketServerSession, var score: Int = 0, var correctCount: Int = 0, var wrongCount: Int = 0, var hasAnswered: Boolean = false, var lastAnswerIndex: Int = -1, var isEliminated: Boolean = false, var totalTime: Long = 0)
data class FriendInfo(val name: String, var isOnline: Boolean)

val onlineUsers = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val rooms = ConcurrentHashMap<String, GameRoom>()
val gson = Gson()
lateinit var database: MongoDatabase

// Simple admin list (add your admin usernames here)
val ADMIN_USERS = setOf("jenemybill", "admin")

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

                var gone: String? = null
                for (entry in onlineUsers.entries) {
                    if (entry.value == this) {
                        gone = entry.key
                        break
                    }
                }
                
                if (gone != null) {
                    onlineUsers.remove(gone)
                    notifyFriendsStatus(gone, false)
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
    return if (doc != null) doc.getString("name") ?: raw else raw
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) return
    val usersColl = database.getCollection<Document>("users")
    val friendsColl = database.getCollection<Document>("friends")
    val requestsColl = database.getCollection<Document>("requests")
    val leaderboardColl = database.getCollection<Document>("solo_leaderboard")
    val questionsColl = database.getCollection<Document>("questions")
    val suggestionsColl = database.getCollection<Document>("suggested_questions")
    val offlineScoresColl = database.getCollection<Document>("offline_scores")

    when (msg.type) {

        // ==================== REGISTER (NEW) ====================
        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            val existing = usersColl.find(Filters.regex("name", "^${Pattern.quote(name)}$", "i")).firstOrNull()
            
            if (existing != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "EXISTS"))))
                return
            }
            
            usersColl.insertOne(
                Document()
                    .append("name", name)
                    .append("createdAt", Date())
                    .append("lastLogin", Date())
            )
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
        }

        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            val official = canonicalName(usersColl, name)
            onlineUsers[official] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(official, session)
            sendRequestList(official, session)
            notifyFriendsStatus(official, true)
            syncOfflineScoresForPlayer(official, offlineScoresColl, leaderboardColl, session)
        }

        // ==================== IMPROVED ADD_FRIEND ====================
        MessageType.ADD_FRIEND -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val targetRaw = msg.content?.trim() ?: return
            val targetDoc = usersColl.find(Filters.regex("name", "^${Pattern.quote(targetRaw)}$", "i")).toList().firstOrNull()

            if (targetDoc == null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
                return
            }

            val officialTarget = targetDoc.getString("name") ?: targetRaw

            if (user.equals(officialTarget, ignoreCase = true)) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Δεν μπορείς να στείλεις αίτημα στον εαυτό σου!"))))
                return
            }

            val alreadyFriends = friendsColl.countDocuments(
                Filters.and(Filters.eq("user", user), Filters.eq("friend", officialTarget))
            ) > 0

            if (alreadyFriends) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είστε ήδη φίλοι!"))))
                return
            }

            val existingRequest = requestsColl.find(
                Filters.and(
                    Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                    Filters.eq("requesters", user)
                )
            ).firstOrNull()

            if (existingRequest != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Έχεις ήδη στείλει αίτημα σε αυτόν τον παίκτη!"))))
                return
            }

            requestsColl.updateOne(
                Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                Updates.combine(Updates.addToSet("requesters", user), Updates.setOnInsert("target", officialTarget)),
                UpdateOptions().upsert(true)
            )

            onlineUsers.entries.firstOrNull { it.key.equals(officialTarget, ignoreCase = true) }?.let {
                sendRequestList(officialTarget, it.value)
                it.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
            }

            sendRequestList(user, session)
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε στον $officialTarget!"))))
        }

        // ==================== FIXED LEADERBOARD SYNC ====================
        MessageType.SYNC_OFFLINE_SCORES -> {
            try {
                val scores: List<Map<String, Any>> = gson.fromJson(msg.content, object : TypeToken<List<Map<String, Any>>>() {}.type)
                val maxScore = scores.maxOfOrNull { (it["score"] as? Number)?.toInt() ?: 0 } ?: 0

                leaderboardColl.updateOne(
                    Filters.eq("name", msg.sender),
                    Updates.combine(
                        Updates.max("maxScore", maxScore),
                        Updates.set("lastUpdate", Date())
                    ),
                    UpdateOptions().upsert(true)
                )
                offlineScoresColl.deleteMany(Filters.eq("playerName", msg.sender))

                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to true, "message" to "Scores synced", "count" to scores.size))))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to false, "error" to e.message))))))
            }
        }

        // ... (όλα τα υπόλοιπα handlers παραμένουν ακριβώς ίδια όπως στο αρχικό σου αρχείο)

        MessageType.UPLOAD_QUESTIONS -> { /* ... */ }
        MessageType.SUGGEST_QUESTION -> { /* ... */ }
        MessageType.GET_PENDING_QUESTIONS -> { /* ... */ }
        MessageType.APPROVE_QUESTION -> { /* ... */ }
        MessageType.REJECT_QUESTION -> { /* ... */ }
        MessageType.GET_SOLO_QUESTIONS -> { /* ... */ }
        MessageType.CHALLENGE_RESULT -> { /* ... */ }
        MessageType.CHALLENGE_FRIEND -> { /* ... */ }
        MessageType.GET_LEADERBOARD -> { /* ... */ }
        MessageType.ACCEPT_REQUEST -> { /* ... */ }
        MessageType.REJECT_REQUEST -> { /* ... */ }
        MessageType.START_GAME -> { /* ... */ }
        MessageType.ANSWER -> { /* ... */ }
        MessageType.CREATE_ROOM -> { /* ... */ }
        MessageType.JOIN -> { /* ... */ }
        else -> {}
    }
}

// ==================== ΒΟΗΘΗΤΙΚΕΣ ΣΥΝΑΡΤΗΣΕΙΣ (παραμένουν ίδιες) ====================

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) { /* ... */ }
suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) { /* ... */ }
suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) { /* ... */ }

suspend fun syncOfflineScoresForPlayer(
    playerName: String, 
    offlineScoresColl: MongoCollection<Document>, 
    leaderboardColl: MongoCollection<Document>,
    session: DefaultWebSocketServerSession
) {
    try {
        val scores = offlineScoresColl.find(Filters.eq("playerName", playerName)).toList()
        if (scores.isNotEmpty()) {
            val maxScore = scores.maxOfOrNull { it.getInteger("score") ?: 0 } ?: 0
            leaderboardColl.updateOne(
                Filters.eq("name", playerName),
                Updates.combine(Updates.max("maxScore", maxScore), Updates.set("lastUpdate", Date())),
                UpdateOptions().upsert(true)
            )
            offlineScoresColl.deleteMany(Filters.eq("playerName", playerName))
        }
    } catch (e: Exception) { e.printStackTrace() }
}

suspend fun broadcastToAll(message: GameMessage) { /* ... */ }
suspend fun runGameLoop(room: GameRoom) { /* ... */ }
