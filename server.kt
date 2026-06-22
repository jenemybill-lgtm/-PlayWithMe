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

// --- CONFIGURATION v4.0 (CLOUD STORAGE & APPROVAL SYSTEM) ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

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
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            val official = canonicalName(usersColl, name)
            onlineUsers[official] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(official, session)
            sendRequestList(official, session)
            notifyFriendsStatus(official, true)
            
            // NEW: Sync offline scores when player logs in
            syncOfflineScoresForPlayer(official, offlineScoresColl, leaderboardColl, session)
        }

        MessageType.UPLOAD_QUESTIONS -> {
            // ADMIN ONLY: Bulk upload from assets to 'questions' (approved folder)
            try {
                val listType = object : TypeToken<List<Map<String, Any>>>() {}.type
                val questions: List<Map<String, Any>> = gson.fromJson(msg.content, listType)
                questions.forEach { q ->
                    val doc = Document()
                    q.forEach { (k, v) -> doc.append(k, v) }
                    // Prevent duplicates based on question text
                    if (questionsColl.countDocuments(Filters.eq("text", q["text"])) == 0L) {
                        questionsColl.insertOne(doc)
                    }
                }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Η μεταφόρτωση ολοκληρώθηκε στη MongoDB!"))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα Upload: ${e.message}"))))
            }
        }

        MessageType.SUGGEST_QUESTION -> {
            // PLAYER ACTION: Goes to 'suggested_questions' (for your review)
            try {
                val q: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                val doc = Document()
                q.forEach { (k, v) -> doc.append(k, v) }
                doc.append("suggested_by", msg.sender)
                doc.append("date", Date())
                doc.append("isApproved", false)
                doc.append("rejectionReason", null)
                suggestionsColl.insertOne(doc)
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SUGGEST_QUESTION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to true, "message" to "Η πρότασή σου στάλθηκε για έγκριση!"))))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SUGGEST_QUESTION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to false, "error" to e.message))))))
            }
        }

        // NEW: Get pending questions for admin
        MessageType.GET_PENDING_QUESTIONS -> {
            try {
                val pending = suggestionsColl.find(Filters.eq("isApproved", false)).toList()
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PENDING_QUESTIONS_DATA, "Server", gson.toJson(pending)))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα: ${e.message}"))))
            }
        }

        // NEW: Admin approves question
        MessageType.APPROVE_QUESTION -> {
            try {
                val q: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                val questionId = (q["_id"] as? String)?.let { org.bson.types.ObjectId(it) } ?: return
                
                val suggestion = suggestionsColl.find(Filters.eq("_id", questionId)).firstOrNull() ?: return
                
                // Move to main questions collection
                val approved = Document(suggestion).apply {
                    append("isApproved", true)
                    append("approvedDate", Date())
                }
                questionsColl.insertOne(approved)
                
                // Mark as approved in suggestions
                suggestionsColl.updateOne(Filters.eq("_id", questionId), 
                    Updates.combine(Updates.set("isApproved", true), Updates.set("approvedDate", Date())))
                
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to true, "message" to "Question approved!"))))))
                
                // Notify all clients
                broadcastToAll(GameMessage(MessageType.NEW_QUESTIONS_DATA, "Server", "New questions available!"))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to false, "error" to e.message))))))
            }
        }

        // NEW: Admin rejects question
        MessageType.REJECT_QUESTION -> {
            try {
                val q: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                val questionId = (q["_id"] as? String)?.let { org.bson.types.ObjectId(it) } ?: return
                val reason = q["reason"] as? String ?: "No reason provided"
                
                suggestionsColl.updateOne(Filters.eq("_id", questionId), 
                    Updates.combine(Updates.set("isApproved", false), Updates.set("rejectionReason", reason)))
                
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to true, "message" to "Question rejected"))))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to false, "error" to e.message))))))
            }
        }

        // NEW: Sync offline scores
        MessageType.SYNC_OFFLINE_SCORES -> {
            try {
                val scores: List<Map<String, Any>> = gson.fromJson(msg.content, object : TypeToken<List<Map<String, Any>>>() {}.type)
                val totalScore = scores.sumOf { (it["score"] as? Number)?.toInt() ?: 0 }
                
                leaderboardColl.updateOne(
                    Filters.eq("name", msg.sender),
                    Updates.combine(
                        Updates.max("maxScore", totalScore),
                        Updates.set("lastUpdate", Date())
                    ),
                    UpdateOptions().upsert(true)
                )
                
                // Mark scores as synced
                offlineScoresColl.deleteMany(Filters.eq("playerName", msg.sender))
                
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to true, "message" to "Scores synced", "count" to scores.size))))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SYNC_OFFLINE_SCORES_RESPONSE, "Server", 
                    gson.toJson(mapOf("success" to false, "error" to e.message))))))
            }
        }

        // NEW: Check for new questions
        MessageType.CHECK_NEW_QUESTIONS -> {
            try {
                val newCount = suggestionsColl.countDocuments(Filters.eq("isApproved", true))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.NEW_QUESTIONS_DATA, "Server", 
                    gson.toJson(mapOf("newQuestionsCount" to newCount))))))
            } catch (e: Exception) {}
        }

        MessageType.GET_SOLO_QUESTIONS -> {
            // Return in difficulty order: 15 easy, then 45 medium, then 40 hard
            // (shuffle only within each difficulty group)
            val easy = questionsColl.find(Filters.eq("difficulty", "Εύκολο")).toList().shuffled().take(15)
            val medium = questionsColl.find(Filters.eq("difficulty", "Μέτριο")).toList().shuffled().take(45)
            val hard = questionsColl.find(Filters.eq("difficulty", "Δύσκολο")).toList().shuffled().take(40)
            
            val allSolo = easy + medium + hard
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SOLO_QUESTIONS_DATA, "Server", gson.toJson(allSolo)))))
        }

        MessageType.CHALLENGE_RESULT -> {
            val content = msg.content ?: return
            if (content.startsWith("LEADERBOARD_SUBMIT")) {
                val score = content.split("|").getOrNull(1)?.toIntOrNull() ?: 0
                leaderboardColl.updateOne(
                    Filters.eq("name", msg.sender),
                    Updates.combine(
                        Updates.max("maxScore", score),
                        Updates.set("lastUpdate", Date())
                    ),
                    UpdateOptions().upsert(true)
                )
                return
            }
            
            val target = content.split("|").getOrNull(0) ?: return
            onlineUsers.entries.firstOrNull { it.key.equals(target, ignoreCase = true) }?.let {
                it.value.send(Frame.Text(gson.toJson(msg)))
            }
        }

        MessageType.GET_LEADERBOARD -> {
            val topScores = leaderboardColl.find()
                .sort(Sorts.descending("maxScore"))
                .limit(50)
                .toList()
                .map { doc -> mapOf("name" to doc.getString("name"), "score" to doc.getInteger("maxScore")) }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LEADERBOARD_DATA, "Server", gson.toJson(topScores)))))
        }

        MessageType.ADD_FRIEND -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val target = msg.content?.trim() ?: return
            val targetDoc = usersColl.find(Filters.regex("name", "^${Pattern.quote(target)}$", "i")).toList().firstOrNull()

            if (targetDoc == null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!"))))
                return
            }

            val officialTarget = targetDoc.getString("name") ?: target
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"), 
                Updates.combine(Updates.addToSet("requesters", user), Updates.setOnInsert("target", officialTarget)), UpdateOptions().upsert(true))
            
            onlineUsers.entries.firstOrNull { it.key.equals(officialTarget, ignoreCase = true) }?.let {
                sendRequestList(officialTarget, it.value)
                it.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
            }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε στον $officialTarget!"))))
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = canonicalName(usersColl, msg.content?.trim() ?: return)
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            friendsColl.insertOne(Document().append("user", user).append("friend", requester))
            friendsColl.insertOne(Document().append("user", requester).append("friend", user))
            sendFriendList(user, session); sendRequestList(user, session)
            onlineUsers[requester]?.let { sendFriendList(requester, it) }
        }

        MessageType.REJECT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = canonicalName(usersColl, msg.content?.trim() ?: return)
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            sendRequestList(user, session)
        }

        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                room.gameJob?.cancel()
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                
                val count = (setup["count"] as? Double)?.toInt() ?: 10
                val categories = setup["categories"] as? List<String> ?: listOf("Όλες")
                val difficulties = setup["difficulties"] as? List<String> ?: listOf("Όλα")
                
                val questions = if (categories.contains("Όλες") && difficulties.contains("Όλα")) {
                    questionsColl.find().toList().shuffled().take(count)
                } else if (categories.contains("Όλες")) {
                    questionsColl.find(Filters.`in`("difficulty", difficulties)).toList().shuffled().take(count)
                } else if (difficulties.contains("Όλα")) {
                    questionsColl.find(Filters.`in`("category", categories)).toList().shuffled().take(count)
                } else {
                    val categoryFilter = Filters.`in`("category", categories)
                    val difficultyFilter = Filters.`in`("difficulty", difficulties)
                    questionsColl.find(Filters.and(categoryFilter, difficultyFilter)).toList().shuffled().take(count)
                }
                
                room.questions = questions
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
            } else session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το δωμάτιο δεν βρέθηκε!"))))
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Document>("friends").find(Filters.eq("user", user)).toList().map { doc ->
        val fName = doc.getString("friend") ?: ""
        FriendInfo(fName, onlineUsers.containsKey(fName))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val requestsColl = database.getCollection<Document>("requests")
        val doc = requestsColl.find(Filters.regex("target", "^${Pattern.quote(user)}$", "i")).toList().firstOrNull()
        val rawRequesters = doc?.get("requesters", List::class.java)
        val requesters = rawRequesters?.map { it.toString() } ?: emptyList()
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters)))))
    } catch (e: Exception) { }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    for (doc in database.getCollection<Document>("friends").find(Filters.eq("friend", user)).toList()) {
        val friendName = doc.getString("user") ?: continue
        val sess = onlineUsers[friendName]
        if (sess != null) sendFriendList(friendName, sess)
    }
}

// NEW: Sync offline scores when player logs in
suspend fun syncOfflineScoresForPlayer(playerName: String, offlineScoresColl: MongoCollection<Document>, leaderboardColl: MongoCollection<Document>, session: DefaultWebSocketServerSession) {
    try {
        val scores = offlineScoresColl.find(Filters.eq("playerName", playerName)).toList()
        if (scores.isNotEmpty()) {
            val totalScore = scores.sumOf { (it.getInteger("score") ?: 0).toInt() }
            leaderboardColl.updateOne(
                Filters.eq("name", playerName),
                Updates.combine(
                    Updates.max("maxScore", totalScore),
                    Updates.set("lastUpdate", Date())
                ),
                UpdateOptions().upsert(true)
            )
            offlineScoresColl.deleteMany(Filters.eq("playerName", playerName))
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

// NEW: Broadcast to all online users
suspend fun broadcastToAll(message: GameMessage) {
    val text = gson.toJson(message)
    for (session in onlineUsers.values) {
        try { session.send(Frame.Text(text)) } catch(e: Exception) {}
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
