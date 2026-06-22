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

// ==================== CONFIG ====================
const val LATEST_VERSION_NAME = "2.1"
const val LATEST_VERSION_CODE = 12
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: error("MONGODB_URI environment variable is required")

val ADMIN_USERS = setOf("jenemybill", "admin")

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
        sessions.forEach { try { it.send(Frame.Text(text)) } catch (e: Exception) {} }
    }

    suspend fun updateHostPlayerCount() {
        try {
            hostSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.PLAYER_COUNT, "Server", players.size.toString()))))
        } catch (e: Exception) {}
    }
}

// ==================== DATABASE ====================
suspend fun initDatabase() {
    try {
        val client = MongoClient.create(MONGODB_URI)
        database = client.getDatabase("playwithme")
        println("SERVER: DATABASE CONNECTED SUCCESSFULLY")
    } catch (e: Exception) {
        println("DATABASE ERROR: ${e.message}")
    }
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
                            val msg = gson.fromJson(frame.readText(), GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) {}

                val gone = onlineUsers.entries.find { it.value == this }?.key
                gone?.let {
                    onlineUsers.remove(it)
                    notifyFriendsStatus(it, false)
                }

                rooms.entries.removeIf { entry ->
                    val room = entry.value
                    val shouldRemove = room.hostSession == this || room.players.any { it.session == this }
                    if (shouldRemove) {
                        room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε."))
                    }
                    shouldRemove
                }
            }
        }
    }.start(wait = true)
}

// ==================== HELPERS ====================
suspend fun canonicalName(usersColl: MongoCollection<Document>, raw: String): String {
    val doc = usersColl.find(Filters.regex("name", "^${Pattern.quote(raw)}$", "i")).toList().firstOrNull()
    return doc?.getString("name") ?: raw
}

fun isAdmin(name: String): Boolean = ADMIN_USERS.contains(name)

// ==================== MAIN HANDLER ====================
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

        // ==================== REGISTER ====================
        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            val existing = usersColl.find(Filters.regex("name", "^${Pattern.quote(name)}$", "i")).firstOrNull()
            if (existing != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "EXISTS"))))
                return
            }
            usersColl.insertOne(Document("name", name).append("createdAt", Date()).append("lastLogin", Date()))
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
        }

        // ==================== LOGIN ====================
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            val official = canonicalName(usersColl, name)
            onlineUsers[official] = session

            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(official, session)
            sendRequestList(official, session)           // ← Σημαντικό για requests
            notifyFriendsStatus(official, true)
            syncOfflineScoresForPlayer(official, offlineScoresColl, leaderboardColl)
        }

        // ==================== ADD_FRIEND (ΔΙΟΡΘΩΜΕΝΟ) ====================
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

            val hasPending = requestsColl.find(
                Filters.and(
                    Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                    Filters.eq("requesters", user)
                )
            ).firstOrNull() != null
            if (hasPending) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Έχεις ήδη στείλει αίτημα!"))))
                return
            }

            requestsColl.updateOne(
                Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                Updates.combine(Updates.addToSet("requesters", user), Updates.setOnInsert("target", officialTarget)),
                UpdateOptions().upsert(true)
            )

            // Ενημέρωση παραλήπτη (αν είναι online)
            val targetSession = onlineUsers[officialTarget]
            if (targetSession != null) {
                sendRequestList(officialTarget, targetSession)
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα φιλίας από $user!"))))
            }

            // Ενημέρωση αποστολέα
            sendRequestList(user, session)
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε επιτυχώς!"))))
        }

        // ==================== SYNC OFFLINE SCORES (FIXED) ====================
        MessageType.SYNC_OFFLINE_SCORES -> {
            try {
                val scores: List<Map<String, Any>> = gson.fromJson(msg.content, object : TypeToken<List<Map<String, Any>>>() {}.type)
                val maxScore = scores.maxOfOrNull { (it["score"] as? Number)?.toInt() ?: 0 } ?: 0

                leaderboardColl.updateOne(
                    Filters.eq("name", msg.sender),
                    Updates.combine(Updates.max("maxScore", maxScore), Updates.set("lastUpdate", Date())),
                    UpdateOptions().upsert(true)
                )
                offlineScoresColl.deleteMany(Filters.eq("playerName", msg.sender))

                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server",
                    gson.toJson(mapOf("success" to true, "count" to scores.size))))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server",
                    gson.toJson(mapOf("success" to false))))))
            }
        }

        // ==================== START_GAME / ANSWER / CREATE_ROOM / JOIN ====================
        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                room.gameJob?.cancel()
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)

                val count = (setup["count"] as? Double)?.toInt() ?: 10
                val categories = setup["categories"] as? List<String> ?: listOf("Όλες")
                val difficulties = setup["difficulties"] as? List<String> ?: listOf("Όλα")
                val timer = (setup["timer"] as? Double)?.toInt() ?: 20

                val questions = when {
                    categories.contains("Όλες") && difficulties.contains("Όλα") -> questionsColl.find().toList().shuffled().take(count)
                    categories.contains("Όλες") -> questionsColl.find(Filters.`in`("difficulty", difficulties)).toList().shuffled().take(count)
                    difficulties.contains("Όλα") -> questionsColl.find(Filters.`in`("category", categories)).toList().shuffled().take(count)
                    else -> questionsColl.find(Filters.and(Filters.`in`("category", categories), Filters.`in`("difficulty", difficulties))).toList().shuffled().take(count)
                }

                room.questions = questions
                room.timerSeconds = timer
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                room.waitingForAnswers = false

                room.players.forEach { it.score = 0; it.totalTime = 0; it.hasAnswered = false; it.lastAnswerIndex = -1 }

                room.broadcast(GameMessage(MessageType.START_GAME, "Server", timer.toString()))
                room.gameJob = CoroutineScope(Dispatchers.Default).launch {
                    delay(2000)
                    runGameLoop(room)
                }
            }
        }

        MessageType.ANSWER -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && room.waitingForAnswers && player != null && !player.hasAnswered) {
                val parts = msg.content?.split("|") ?: return
                player.hasAnswered = true
                player.lastAnswerIndex = parts.getOrNull(0)?.toIntOrNull() ?: -1
                player.totalTime += parts.getOrNull(1)?.toLongOrNull() ?: 0
                if (room.players.all { it.hasAnswered }) room.waitingForAnswers = false
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
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το δωμάτιο δεν βρέθηκε!"))))
            }
        }

        else -> {}
    }
}

// ==================== HELPER FUNCTIONS ====================
suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Document>("friends")
        .find(Filters.eq("user", user)).toList()
        .map { FriendInfo(it.getString("friend") ?: "", onlineUsers.containsKey(it.getString("friend"))) }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val doc = database.getCollection<Document>("requests")
            .find(Filters.regex("target", "^${Pattern.quote(user)}$", "i")).toList().firstOrNull()
        val requesters = doc?.get("requesters", List::class.java)?.map { it.toString() } ?: emptyList()
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters)))))
    } catch (e: Exception) {}
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    database.getCollection<Document>("friends").find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc.getString("user") ?: return@forEach
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}

suspend fun syncOfflineScoresForPlayer(playerName: String, offlineScoresColl: MongoCollection<Document>, leaderboardColl: MongoCollection<Document>) {
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
    } catch (e: Exception) {}
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        val question = room.questions[room.currentQuestionIndex]
        val correctIdx = (question["correctAnswerIndex"] as Double).toInt()

        room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
        room.waitingForAnswers = true

        room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))

        var elapsed = 0
        while (elapsed < room.timerSeconds * 1000 && room.waitingForAnswers && room.isGameRunning) {
            delay(200); elapsed += 200
        }

        room.waitingForAnswers = false
        room.players.forEach { if (it.lastAnswerIndex == correctIdx) it.score++ }

        val options = question["options"] as List<String>
        room.broadcast(GameMessage(MessageType.RESULT, "Server", "Σωστή απάντηση: ${options[correctIdx]}"))
        delay(3000)
        room.currentQuestionIndex++
    }

    if (room.isGameRunning) {
        val ranking = room.players.sortedWith(compareByDescending<Player> { it.score }.thenBy { it.totalTime })
        val text = ranking.withIndex().joinToString("\n") { "${it.index + 1}. ${it.value.name}: ${it.value.score}" }
        room.broadcast(GameMessage(MessageType.GAME_OVER, "Server", "ΤΕΛΙΚΗ ΚΑΤΑΤΑΞΗ:\n$text"))
        room.isGameRunning = false
    }
}
