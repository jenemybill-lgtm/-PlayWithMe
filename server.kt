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
val MONGODB_URI = System.getenv("MONGODB_URI") ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

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
data class RequestLists(val incoming: List<String>, val outgoing: List<String>)

val onlineUsers = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val rooms = ConcurrentHashMap<String, GameRoom>()
val gson = Gson()
lateinit var database: MongoDatabase

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = CopyOnWriteArrayList<Player>()
    var questions: List<Document> = emptyList()
    var extraQuestions: List<Document> = emptyList()
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isSuddenDeathEnabled = true
    var isSpeedMode = false
    var isSuddenDeathActive = false
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
                    val isParticipant = room.players.any { it.session == this }
                    val isHost = room.hostSession == this
                    
                    if (isParticipant || isHost) {
                        if (room.isGameRunning) {
                            runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε.")) }
                        }
                        room.players.removeIf { it.session == this }
                        runBlocking { room.updateHostPlayerCount() }
                    }
                    isHost
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

// ==================== MAIN HANDLER ====================
suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) return

    val usersColl = database.getCollection<Document>("users")
    val friendsColl = database.getCollection<Document>("friends")
    val requestsColl = database.getCollection<Document>("requests")
    val leaderboardColl = database.getCollection<Document>("solo_leaderboard")
    val questionsColl = database.getCollection<Document>("questions")
    val offlineScoresColl = database.getCollection<Document>("offline_scores")
    val pendingColl = database.getCollection<Document>("pending_messages")

    when (msg.type) {

        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            val existing = usersColl.find(Filters.regex("name", "^${Pattern.quote(name)}$", "i")).firstOrNull()
            if (existing != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "EXISTS"))))
                return
            }
            usersColl.insertOne(Document("name", name).append("createdAt", Date()))
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
        }

        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            val official = canonicalName(usersColl, name)
            onlineUsers[official] = session

            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            
            // Pending Messages
            pendingColl.find(Filters.eq("target", official)).toList().forEach { doc ->
                val type = MessageType.valueOf(doc.getString("type") ?: "ERROR")
                session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc.getString("content")))))
            }
            pendingColl.deleteMany(Filters.eq("target", official))

            sendFriendList(official, session)
            sendRequestList(official, session)
            notifyFriendsStatus(official, true)
        }

        MessageType.ADD_FRIEND -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val targetRaw = msg.content?.trim() ?: return
            val targetDoc = usersColl.find(Filters.regex("name", "^${Pattern.quote(targetRaw)}$", "i")).toList().firstOrNull()

            if (targetDoc == null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
                return
            }
            val officialTarget = targetDoc.getString("name") ?: targetRaw
            
            val alreadyFriends = friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", officialTarget))) > 0
            if (alreadyFriends) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είστε ήδη φίλοι!"))))
                return
            }

            requestsColl.updateOne(
                Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                Updates.combine(Updates.addToSet("requesters", user), Updates.setOnInsert("target", officialTarget)),
                UpdateOptions().upsert(true)
            )

            onlineUsers[officialTarget]?.let { sendRequestList(officialTarget, it) }
            sendRequestList(user, session)
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = msg.content ?: return
            // Χρήση regex για να βρούμε το έγγραφο ανεξάρτητα από κεφαλαία/πεζά στο target
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            friendsColl.insertOne(Document("user", user).append("friend", requester))
            friendsColl.insertOne(Document("user", requester).append("friend", user))
            
            sendFriendList(user, session)
            sendRequestList(user, session)
            onlineUsers[requester]?.let {
                sendFriendList(requester, it)
                sendRequestList(requester, it)
            }
        }

        MessageType.REJECT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = msg.content ?: return
            // Χρήση regex για σωστό ταίριασμα
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            sendRequestList(user, session)
            onlineUsers[requester]?.let { sendRequestList(requester, it) }
        }

        MessageType.INVITE -> {
            val host = msg.sender
            val target = msg.content ?: return
            val room = rooms.values.find { it.hostSession == session }
            if (room != null) {
                val inviteStr = "$host|${room.code}"
                val targetSession = onlineUsers[target]
                if (targetSession != null) {
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.INVITE_RECEIVED, "Server", inviteStr))))
                } else {
                    pendingColl.insertOne(Document("target", target).append("type", "INVITE_RECEIVED").append("content", inviteStr))
                }
            }
        }

        MessageType.CHALLENGE_FRIEND -> {
            val host = msg.sender
            val target = msg.content?.split("|")?.getOrNull(1) ?: return
            val seed = (1..99999).random().toString()
            val challengeStr = "SOLO|$host|$seed"
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
            } else {
                pendingColl.insertOne(Document("target", target).append("type", "CHALLENGE_RECEIVED").append("content", challengeStr))
            }
        }

        MessageType.CREATE_ROOM -> {
            val code = (10000..99999).random().toString()
            val room = GameRoom(code, session)
            room.players.add(Player(msg.sender, session))
            rooms[code] = room
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
        }

        MessageType.JOIN -> {
            val room = rooms[msg.content]
            if (room != null) {
                if (room.players.none { it.session == session }) room.players.add(Player(msg.sender, session))
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", room.code))
                room.updateHostPlayerCount()
            }
        }

        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                room.gameJob?.cancel()
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                
                val count = (setup["count"] as? Double)?.toInt() ?: 10
                val categories = setup["categories"] as? List<String> ?: listOf("Όλες")
                val difficulties = setup["difficulties"] as? List<String> ?: listOf("Όλα")
                
                room.questions = fetchQuestions(questionsColl, count, categories, difficulties)
                room.extraQuestions = fetchQuestions(questionsColl, 10, listOf("Όλες"), listOf("Όλα"))
                room.timerSeconds = (setup["timer"] as? Double)?.toInt() ?: 20
                room.isSuddenDeathEnabled = setup["isSuddenDeath"] as? Boolean ?: true
                room.isSpeedMode = setup["isSpeedMode"] as? Boolean ?: false
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                room.isSuddenDeathActive = false
                
                room.players.forEach { it.score = 0; it.correctCount = 0; it.wrongCount = 0; it.totalTime = 0; it.isEliminated = false }
                
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
                player.lastAnswerIndex = parts.getOrNull(0)?.toIntOrNull() ?: -1
                player.totalTime += parts.getOrNull(1)?.toLongOrNull() ?: 0
                if (room.players.filter { !it.isEliminated }.all { it.hasAnswered }) room.waitingForAnswers = false
            }
        }
        
        MessageType.SYNC_OFFLINE_SCORES -> {
            try {
                val scores: List<Map<String, Any>> = gson.fromJson(msg.content, object : TypeToken<List<Map<String, Any>>>() {}.type)
                val maxScore = scores.maxOfOrNull { (it["score"] as? Number)?.toInt() ?: 0 } ?: 0
                leaderboardColl.updateOne(Filters.eq("name", msg.sender), Updates.combine(Updates.max("maxScore", maxScore), Updates.set("lastUpdate", Date())), UpdateOptions().upsert(true))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server", "OK"))))
            } catch (e: Exception) {}
        }

        else -> {}
    }
}

suspend fun fetchQuestions(coll: MongoCollection<Document>, count: Int, cats: List<String>, diffs: List<String>): List<Document> {
    val filter = when {
        cats.contains("Όλες") && diffs.contains("Όλα") -> Filters.empty()
        cats.contains("Όλες") -> Filters.`in`("difficulty", diffs)
        diffs.contains("Όλα") -> Filters.`in`("category", cats)
        else -> Filters.and(Filters.`in`("category", cats), Filters.`in`("difficulty", diffs))
    }
    return coll.find(filter).toList().shuffled().take(count)
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Document>("friends").find(Filters.eq("user", user)).toList()
        .map { FriendInfo(it.getString("friend") ?: "", onlineUsers.containsKey(it.getString("friend"))) }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val requestsColl = database.getCollection<Document>("requests")
        
        // Incoming: Μαζεύουμε όλα τα αιτήματα από όλα τα πιθανά έγγραφα που αφορούν τον χρήστη (case-insensitive)
        val incoming = mutableSetOf<String>()
        requestsColl.find(Filters.regex("target", "^${Pattern.quote(user)}$", "i")).collect { doc ->
            (doc["requesters"] as? List<*>)?.forEach { incoming.add(it.toString()) }
        }
        
        // Outgoing: Ψάχνουμε σε όλα τα έγγραφα πού ο χρήστης έχει στείλει αίτημα (περιλαμβάνεται στους requesters)
        val outgoing = mutableSetOf<String>()
        requestsColl.find(Filters.regex("requesters", "^${Pattern.quote(user)}$", "i")).collect { doc ->
            val target = doc.getString("target")
            if (target != null) outgoing.add(target)
        }
        
        val response = RequestLists(incoming.toList(), outgoing.toList())
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(response)))))
        println("SERVER: Sent consolidated RequestList to $user (In: ${incoming.size}, Out: ${outgoing.size})")
    } catch (e: Exception) {
        println("SERVER ERROR in sendRequestList for $user: ${e.message}")
        e.printStackTrace()
    }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    database.getCollection<Document>("friends").find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc.getString("user") ?: return@forEach
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        sendQuestionAndWait(room, room.questions[room.currentQuestionIndex])
        room.currentQuestionIndex++
    }

    if (room.isSuddenDeathEnabled && room.isGameRunning) {
        val sorted = room.players.sortedByDescending { it.score }
        if (sorted.size >= 2 && sorted[0].score == sorted[1].score && sorted[0].score > 0) {
            room.isSuddenDeathActive = true
            room.broadcast(GameMessage(MessageType.RESULT, "Server", "ΙΣΟΠΑΛΙΑ! SUDDEN DEATH!"))
            delay(4000)
            val topScore = sorted[0].score
            room.players.forEach { if (it.score < topScore) it.isEliminated = true }

            var sdIdx = 0
            while (room.players.count { !it.isEliminated } > 1 && sdIdx < room.extraQuestions.size && room.isGameRunning) {
                val q = room.extraQuestions[sdIdx]
                sendQuestionAndWait(room, q)
                val correctIdx = (q.getInteger("correctAnswerIndex") ?: 0)
                room.players.filter { !it.isEliminated }.forEach { if (it.lastAnswerIndex != correctIdx) it.isEliminated = true }
                if (room.players.count { !it.isEliminated } == 0) room.players.forEach { if (it.score == topScore) it.isEliminated = false }
                sdIdx++
            }
        }
    }

    if (room.isGameRunning) {
        val finalRank = if (room.isSpeedMode) room.players.sortedWith(compareByDescending<Player> { it.score }.thenBy { it.totalTime }) else room.players.sortedByDescending { it.score }
        val rankingText = finalRank.withIndex().joinToString("\n") { "${it.index + 1}. ${it.value.name}: ${it.value.score}" }
        room.broadcast(GameMessage(MessageType.GAME_OVER, "Server", "ΤΕΛΙΚΗ ΚΑΤΑΤΑΞΗ:\n$rankingText"))
        room.isGameRunning = false
    }
}

suspend fun sendQuestionAndWait(room: GameRoom, question: Document) {
    val correctIdx = question.getInteger("correctAnswerIndex") ?: 0
    room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
    room.waitingForAnswers = true
    room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))

    var elapsed = 0
    while (elapsed < room.timerSeconds * 1000 && room.waitingForAnswers && room.isGameRunning) { delay(200); elapsed += 200 }
    
    room.waitingForAnswers = false
    room.players.forEach { p ->
        if (p.lastAnswerIndex == correctIdx) {
            if (!room.isSuddenDeathActive) p.score++
            p.correctCount++
        } else if (p.lastAnswerIndex != -1) p.wrongCount++
    }
    
    val options = question.get("options", List::class.java) as? List<String> ?: listOf()
    room.broadcast(GameMessage(MessageType.RESULT, "Server", "Σωστή απάντηση: ${options.getOrNull(correctIdx) ?: ""}"))
    delay(4000)
    room.broadcast(GameMessage(MessageType.LEADERBOARD, "Server", ""))
    delay(1000)
}
