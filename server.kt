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
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

val ADMIN_USERS = setOf("jenemybill", "admin", "basil")
val lastSubmissionTime = ConcurrentHashMap<String, Long>()

fun containsProfanity(text: String): Boolean {
    val profanities = listOf(
        "μαλακα", "πουστη", "γαμω", "σκατα", "πουτανα", "ρχιδι", "μουνι", "κωλο",
        "malaka", "pousti", "gamo", "skata", "poutana", "arxidi", "mouni", "kolo"
    )
    val normalized = text.lowercase(Locale.ROOT)
        .replace("0", "o")
        .replace("1", "i")
        .replace("3", "e")
        .replace("4", "a")
        .replace("5", "s")
        .replace("7", "t")
        .replace("8", "b")

    return profanities.any { normalized.contains(it) }
}

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
    CHECK_NEW_QUESTIONS, NEW_QUESTIONS_DATA,
    GET_DISCOVER_PLAYERS, DISCOVER_PLAYERS_DATA,
    USE_POWERUP, POWERUP_EFFECT, PLAYER_STATS,
    DUEL_CHALLENGE, DUEL_CHALLENGE_RECEIVED, DUEL_ACCEPT, DUEL_SETUP, DUEL_START, DUEL_FINISH, DUEL_RESULT, DUEL_HISTORY_REQUEST, DUEL_HISTORY_DATA
}

data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(
    val name: String,
    val session: DefaultWebSocketServerSession,
    var score: Int = 0,
    var correctCount: Int = 0,
    var wrongCount: Int = 0,
    var hasAnswered: Boolean = false,
    var lastAnswerIndex: Int = -1,
    var isEliminated: Boolean = false,
    var totalTime: Long = 0,
    var lives: Int = 3,
    var doublePointsActive: Boolean = false,
    var shieldActive: Boolean = false,
    val powerups: MutableMap<String, Int> = mutableMapOf("50/50" to 1, "DoublePoints" to 1, "Shield" to 1)
)
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
    var isSurvivalMode = false
    var powerupsEnabled = false
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
    GlobalScope.launch { 
        initDatabase()
        watchDatabaseChanges()
    }
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

fun getQuestionsCollection(category: String): MongoCollection<Document> {
    val collectionName = if (category == "Όλες" || category == "Όλα") "questions" else "questions_$category"
    return database.getCollection<Document>(collectionName)
}

val ALL_CATEGORIES = listOf(
    "Γεωγραφία", "Ιστορία", "Αθλητικά", "Επιστήμη", "Μυθολογία",
    "Τέχνη", "Μαθηματικά", "Τεχνολογία", "Πολιτική", "Γενικές Γνώσεις", "Άλλα"
)

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
    val duelsColl = database.getCollection<Document>("duels")
    val duelStatsColl = database.getCollection<Document>("duel_stats")

    when (msg.type) {

        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            
            // Input Validation
            if (name.length < 3 || name.length > 20 || !name.all { it.isLetterOrDigit() || it == '_' }) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "INVALID_FORMAT"))))
                return
            }

            if (containsProfanity(name)) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "PROFANITY"))))
                return
            }

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
            if (user == officialTarget) return

            // 1. Check if already friends
            val myFriendsDoc = friendsColl.find(Filters.eq("_id", user)).firstOrNull()
            val alreadyFriends = (myFriendsDoc?.get("friends") as? List<*>)?.contains(officialTarget) == true
            if (alreadyFriends) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είστε ήδη φίλοι!"))))
                return
            }

            // 2. AUTO-ACCEPT LOGIC: Check if target has already sent a request to ME
            val targetRequestsDoc = requestsColl.find(Filters.eq("_id", officialTarget)).firstOrNull()
            val targetSentToMe = (targetRequestsDoc?.get("outgoing") as? List<*>)?.contains(user) == true

            if (targetSentToMe) {
                // They already sent a request to me, so just make us friends!
                // Remove the incoming/outgoing requests
                requestsColl.updateOne(Filters.eq("_id", officialTarget), Updates.pull("outgoing", user))
                requestsColl.updateOne(Filters.eq("_id", user), Updates.pull("incoming", officialTarget))

                // Add to both friends lists
                friendsColl.updateOne(Filters.eq("_id", user), Updates.combine(Updates.addToSet("friends", officialTarget), Updates.setOnInsert("_id", user)), UpdateOptions().upsert(true))
                friendsColl.updateOne(Filters.eq("_id", officialTarget), Updates.combine(Updates.addToSet("friends", user), Updates.setOnInsert("_id", officialTarget)), UpdateOptions().upsert(true))

                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Τώρα είστε φίλοι με τον $officialTarget!"))))
                
                // Sync both players
                sendFriendList(user, session)
                sendRequestList(user, session)
                onlineUsers[officialTarget]?.let {
                    sendFriendList(officialTarget, it)
                    sendRequestList(officialTarget, it)
                }
            } else {
                // Standard request logic
                requestsColl.updateOne(
                    Filters.eq("_id", user),
                    Updates.combine(Updates.addToSet("outgoing", officialTarget), Updates.setOnInsert("_id", user)),
                    UpdateOptions().upsert(true)
                )
                requestsColl.updateOne(
                    Filters.eq("_id", officialTarget),
                    Updates.combine(Updates.addToSet("incoming", user), Updates.setOnInsert("_id", officialTarget)),
                    UpdateOptions().upsert(true)
                )

                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
                
                // Sync both players
                sendRequestList(user, session)
                onlineUsers[officialTarget]?.let { sendRequestList(officialTarget, it) }
            }
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = msg.content?.trim() ?: return

            // 1. Remove from folders
            requestsColl.updateOne(Filters.eq("_id", user), Updates.pull("incoming", requester))
            requestsColl.updateOne(Filters.eq("_id", requester), Updates.pull("outgoing", user))

            // 2. Add to friends folders (BIDIRECTIONAL)
            friendsColl.updateOne(
                Filters.eq("_id", user),
                Updates.combine(Updates.addToSet("friends", requester), Updates.setOnInsert("_id", user)),
                UpdateOptions().upsert(true)
            )
            friendsColl.updateOne(
                Filters.eq("_id", requester),
                Updates.combine(Updates.addToSet("friends", user), Updates.setOnInsert("_id", requester)),
                UpdateOptions().upsert(true)
            )

            // 3. Sync both
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

            // Remove from folders
            requestsColl.updateOne(Filters.eq("_id", user), Updates.pull("incoming", requester))
            requestsColl.updateOne(Filters.eq("_id", requester), Updates.pull("outgoing", user))

            // Sync both
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
                room.isSurvivalMode = setup["survival"] as? Boolean ?: false
                room.powerupsEnabled = setup["powerups"] as? Boolean ?: false
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                room.isSuddenDeathActive = false

                room.players.forEach { 
                    it.score = 0; it.correctCount = 0; it.wrongCount = 0; it.totalTime = 0; it.isEliminated = false
                    it.lives = 3; it.doublePointsActive = false; it.shieldActive = false
                    it.powerups["50/50"] = 1; it.powerups["DoublePoints"] = 1; it.powerups["Shield"] = 1
                }

                room.broadcast(GameMessage(MessageType.START_GAME, "Server", room.timerSeconds.toString()))
                room.gameJob = CoroutineScope(Dispatchers.Default).launch { delay(2000); runGameLoop(room) }
            }
        }

        MessageType.ANSWER -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && room.waitingForAnswers && player != null && !player.hasAnswered && !player.isEliminated) {
                val parts = msg.content?.split("|") ?: return
                player.hasAnswered = true
                player.lastAnswerIndex = parts.getOrNull(0)?.toIntOrNull() ?: -1
                player.totalTime += parts.getOrNull(1)?.toLongOrNull() ?: 0
                if (room.players.filter { !it.isEliminated }.all { it.hasAnswered }) room.waitingForAnswers = false
            }
        }

        MessageType.USE_POWERUP -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && room.isGameRunning && room.powerupsEnabled && player != null && !player.isEliminated) {
                val type = msg.content ?: return
                val count = player.powerups[type] ?: 0
                if (count > 0) {
                    player.powerups[type] = count - 1
                    when (type) {
                        "50/50" -> {
                            // Effect is handled on client, server just broadcasts it
                            room.broadcast(GameMessage(MessageType.POWERUP_EFFECT, player.name, "50/50"))
                        }
                        "DoublePoints" -> {
                            player.doublePointsActive = true
                            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.POWERUP_EFFECT, "Server", "DoublePoints|ACTIVE"))))
                        }
                        "Shield" -> {
                            player.shieldActive = true
                            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.POWERUP_EFFECT, "Server", "Shield|ACTIVE"))))
                        }
                    }
                }
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

        // ==================== BULK UPLOAD QUESTIONS ====================
        MessageType.UPLOAD_QUESTIONS -> {
            try {
                val listType = object : TypeToken<List<Map<String, Any>>>() {}.type
                val newQuestions: List<Map<String, Any>> = gson.fromJson(msg.content, listType)
                var added = 0
                
                // Group questions by category for batch insertion
                val groupedByCat = newQuestions.groupBy { it["category"]?.toString() ?: "Άλλα" }
                
                groupedByCat.forEach { (category, questions) ->
                    val coll = getQuestionsCollection(category)
                    val existingTexts = coll.find().projection(Document("text", 1)).toList().map { it.getString("text") }.toSet()
                    
                    val toInsert = mutableListOf<Document>()
                    questions.forEach { q ->
                        val text = q["text"] as? String ?: return@forEach
                        if (!existingTexts.contains(text)) {
                            toInsert.add(Document(q).apply { append("isApproved", true) })
                            added++
                        }
                    }
                    
                    if (toInsert.isNotEmpty()) {
                        coll.insertMany(toInsert)
                    }
                }
                
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Συγχρονίστηκαν $added νέες ερωτήσεις σε κατηγορίες!"))))
                println("SERVER: Bulk Upload from ${msg.sender} -> Added: $added")
            } catch (e: Exception) {
                println("SERVER ERROR (bulk upload): ${e.message}")
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Αποτυχία συγχρονισμού ερωτήσεων: ${e.message}"))))
            }
        }

        // ==================== SUGGEST QUESTION ====================
        MessageType.SUGGEST_QUESTION -> {
            try {
                val now = System.currentTimeMillis()
                val lastTime = lastSubmissionTime[msg.sender] ?: 0L
                if (now - lastTime < 60000) { // Increased to 60 seconds rate limit
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Περίμενε 1 λεπτό πριν την επόμενη υποβολή!"))))
                    return
                }

                val suggestion = Document.parse(msg.content)
                val questionText = suggestion.getString("text") ?: ""
                val options = suggestion.get("options", List::class.java) as? List<*> ?: emptyList<Any>()
                
                if (questionText.length < 10 || questionText.length > 200) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Η ερώτηση πρέπει να είναι 10-200 χαρακτήρες."))))
                    return
                }

                if (containsProfanity(questionText) || options.any { containsProfanity(it.toString()) }) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Η ερώτηση ή οι απαντήσεις περιέχουν απαγορευμένες λέξεις."))))
                    return
                }

                suggestion.append("suggestedBy", msg.sender)
                suggestion.append("createdAt", Date())
                suggestion.append("isApproved", false)
                database.getCollection<Document>("suggested_questions").insertOne(suggestion)
                lastSubmissionTime[msg.sender] = now
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SUGGEST_QUESTION_RESPONSE, "Server", "OK"))))
            } catch (e: Exception) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα κατά την αποθήκευση."))))
            }
        }

        // ==================== SOLO QUESTIONS ====================
        MessageType.GET_SOLO_QUESTIONS -> {
            val allSoloQuestions = mutableListOf<Document>()
            
            // Fetch questions across ALL categories to ensure we have enough
            val diffCounts = mapOf("Εύκολο" to 15, "Μέτριο" to 45, "Δύσκολο" to 40)
            
            diffCounts.forEach { (diff, targetCount) ->
                val collected = mutableListOf<Document>()
                // First try across all actual categories
                for (cat in ALL_CATEGORIES.shuffled()) {
                    if (collected.size >= targetCount) break
                    val fromCat = getQuestionsCollection(cat).find(Filters.eq("difficulty", diff)).toList()
                    collected.addAll(fromCat)
                }
                
                // If still not enough, try the catch-all "questions" collection
                if (collected.size < targetCount) {
                    val fromMain = database.getCollection<Document>("questions").find(Filters.eq("difficulty", diff)).toList()
                    collected.addAll(fromMain)
                }
                
                allSoloQuestions.addAll(collected.distinctBy { it.getString("text") }.shuffled().take(targetCount))
            }
            
            // Final fallback: if no questions found at all, just take anything
            if (allSoloQuestions.isEmpty()) {
                val fallback = database.getCollection<Document>("questions").find().limit(100).toList()
                allSoloQuestions.addAll(fallback)
            }
            
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SOLO_QUESTIONS_DATA, "Server", gson.toJson(allSoloQuestions)))))
        }

        // ==================== LEADERBOARD ====================
        MessageType.GET_LEADERBOARD -> {
            val top = leaderboardColl.find().sort(Document("maxScore", -1)).limit(50).toList()
                .map { mapOf("name" to it.getString("name"), "score" to it.getInteger("maxScore")) }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LEADERBOARD_DATA, "Server", gson.toJson(top)))))
        }

        // ==================== MODERATION (ADMIN) ====================
        MessageType.GET_PENDING_QUESTIONS -> {
            if (ADMIN_USERS.contains(msg.sender)) {
                val pending = database.getCollection<Document>("suggested_questions")
                    .find(Filters.eq("isApproved", false)).toList()
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PENDING_QUESTIONS_DATA, "Server", gson.toJson(pending)))))
            }
        }

        MessageType.APPROVE_QUESTION -> {
            if (ADMIN_USERS.contains(msg.sender)) {
                try {
                    val contentDoc = Document.parse(msg.content)
                    val idString = contentDoc.getString("_id") ?: return
                    val id = org.bson.types.ObjectId(idString)
                    val fullDoc = database.getCollection<Document>("suggested_questions").find(Filters.eq("_id", id)).firstOrNull()
                    if (fullDoc != null) {
                        val category = fullDoc.getString("category") ?: "Άλλα"
                        val approved = Document(fullDoc).apply {
                            remove("_id")
                            append("isApproved", true)
                        }
                        getQuestionsCollection(category).insertOne(approved)
                        database.getCollection<Document>("suggested_questions").deleteOne(Filters.eq("_id", id))
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", "APPROVED"))))
                    }
                } catch (e: Exception) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα έγκρισης: ${e.message}"))))
                }
            }
        }

        MessageType.REJECT_QUESTION -> {
            if (ADMIN_USERS.contains(msg.sender)) {
                try {
                    val contentDoc = Document.parse(msg.content)
                    val idString = contentDoc.getString("_id") ?: return
                    val id = org.bson.types.ObjectId(idString)
                    val reason = contentDoc.getString("reason") ?: ""
                    
                    val result = database.getCollection<Document>("suggested_questions").deleteOne(Filters.eq("_id", id))
                    if (result.deletedCount > 0) {
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", "REJECTED"))))
                    } else {
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Η ερώτηση δεν βρέθηκε."))))
                    }
                } catch (e: Exception) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα απόρριψης: ${e.message}"))))
                }
            }
        }

        // ==================== DISCOVER PLAYERS ====================
        MessageType.GET_DISCOVER_PLAYERS -> {
            val user = msg.sender
            // Get all users
            val allUsers = usersColl.find(Filters.ne("name", user)).limit(30).toList()
            
            // Get my friends from the new folder structure
            val myFriendsDoc = friendsColl.find(Filters.eq("_id", user)).firstOrNull()
            val myFriends = (myFriendsDoc?.get("friends") as? List<*>)?.map { it.toString() }?.toSet() ?: emptySet()

            // Filter out those who ARE already friends
            val discoverable = allUsers.mapNotNull { it.getString("name") }
                .filter { !myFriends.contains(it) }
                .take(15)

            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DISCOVER_PLAYERS_DATA, "Server", gson.toJson(discoverable)))))
        }

        MessageType.DUEL_CHALLENGE -> {
            val host = msg.sender
            val content = msg.content ?: return
            val parts = content.split("|")
            val target = parts.getOrNull(0) ?: return
            val isLive = parts.getOrNull(1) == "LIVE"
            
            val challengeStr = "$host|$isLive"
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_CHALLENGE_RECEIVED, "Server", challengeStr))))
            } else {
                pendingColl.insertOne(Document("target", target).append("type", "DUEL_CHALLENGE_RECEIVED").append("content", challengeStr))
            }
        }

        MessageType.DUEL_ACCEPT -> {
            val acceptor = msg.sender
            val host = msg.content?.split("|")?.getOrNull(0) ?: return
            
            // Generate unique duel ID
            val duelId = "DUEL_${System.currentTimeMillis()}_${(1000..9999).random()}"
            
            val duelDoc = Document("_id", duelId)
                .append("player1", host)
                .append("player2", acceptor)
                .append("status", "SETUP")
                .append("p1_ready", false)
                .append("p2_ready", false)
                .append("createdAt", Date())
            
            duelsColl.insertOne(duelDoc)
            
            val responseStr = "$duelId|$host|$acceptor"
            onlineUsers[host]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", responseStr))))
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", responseStr))))
        }

        MessageType.DUEL_START -> {
            val sender = msg.sender
            val content = msg.content ?: return
            val parts = content.split("|")
            val duelId = parts.getOrNull(0) ?: return
            val categories = parts.getOrNull(1)?.split(",") ?: emptyList()
            
            val duel = duelsColl.find(Filters.eq("_id", duelId)).firstOrNull() ?: return
            
            if (sender == duel.getString("player1")) {
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(Updates.set("p1_categories", categories), Updates.set("p1_ready", true)))
            } else {
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(Updates.set("p2_categories", categories), Updates.set("p2_ready", true)))
            }
            
            val updatedDuel = duelsColl.find(Filters.eq("_id", duelId)).firstOrNull() ?: return
            if (updatedDuel.getBoolean("p1_ready") == true && updatedDuel.getBoolean("p2_ready") == true) {
                // Both ready! Start game logic
                val p1 = updatedDuel.getString("player1") ?: ""
                val p2 = updatedDuel.getString("player2") ?: ""

                val allCats = (updatedDuel.get("p1_categories") as List<String>) + (updatedDuel.get("p2_categories") as List<String>)
                
                // Create a transient room for the duel
                val code = duelId.takeLast(5)
                val room = GameRoom(code, onlineUsers[p1]!!) 
                room.players.add(Player(p1, onlineUsers[p1]!!))
                room.players.add(Player(p2, onlineUsers[p2]!!))
                
                val qCount = 5 
                val questionsList = mutableListOf<Document>()
                allCats.distinct().forEach { cat ->
                    questionsList.addAll(fetchQuestions(questionsColl, qCount, listOf(cat), listOf("Όλα")))
                }
                
                room.questions = questionsList.shuffled()
                room.isSpeedMode = true 
                room.timerSeconds = 15
                room.isGameRunning = true
                rooms[code] = room
                
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", "15"))
                CoroutineScope(Dispatchers.Default).launch { delay(2000); runGameLoop(room) }
            }
        }

        MessageType.DUEL_FINISH -> {
            val sender = msg.sender
            val stats = msg.content?.split("|") ?: return
            val score = stats.getOrNull(0)?.toIntOrNull() ?: 0
            val time = stats.getOrNull(1)?.toLongOrNull() ?: 0
            
            // Find active duel for this player
            val duel = duelsColl.find(Filters.and(
                Filters.or(Filters.eq("player1", sender), Filters.eq("player2", sender)),
                Filters.eq("status", "SETUP") // Still in setup phase technically until finished
            )).firstOrNull() ?: return
            
            val duelId = duel.getString("_id")
            if (sender == duel.getString("player1")) {
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(Updates.set("p1_score", score), Updates.set("p1_time", time), Updates.set("p1_finished", true)))
            } else {
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(Updates.set("p2_score", score), Updates.set("p2_time", time), Updates.set("p2_finished", true)))
            }
            
            val updated = duelsColl.find(Filters.eq("_id", duelId)).firstOrNull() ?: return
            if (updated.getBoolean("p1_finished") == true && updated.getBoolean("p2_finished") == true) {
                // Determine winner
                val s1 = updated.getInteger("p1_score") ?: 0
                val s2 = updated.getInteger("p2_score") ?: 0
                val t1 = updated.getLong("p1_time") ?: 0L
                val t2 = updated.getLong("p2_time") ?: 0L
                
                val p1 = updated.getString("player1") ?: ""
                val p2 = updated.getString("player2") ?: ""
                
                var winner = ""
                if (s1 > s2) winner = p1
                else if (s2 > s1) winner = p2
                else {
                    // Draw or check time
                    if (t1 < t2) winner = p1
                    else if (t2 < t1) winner = p2
                }
                
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(Updates.set("status", "COMPLETED"), Updates.set("winner", winner)))
                
                // Update Stats
                updateDuelStats(duelStatsColl, p1, p2, winner)
                
                val resultMsg = if (winner == "") "ΙΣΟΠΑΛΙΑ!" else "ΝΙΚΗΤΗΣ: $winner!"
                onlineUsers[p1]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_RESULT, "Server", resultMsg))))
                onlineUsers[p2]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_RESULT, "Server", resultMsg))))
            }
        }
        
        MessageType.DUEL_HISTORY_REQUEST -> {
            val stats = duelStatsColl.find(Filters.eq("_id", msg.sender)).firstOrNull()
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_HISTORY_DATA, "Server", gson.toJson(stats ?: Document("_id", msg.sender).append("wins", 0).append("losses", 0).append("draws", 0))))))
        }

        else -> {}
    }
}

suspend fun updateDuelStats(coll: MongoCollection<Document>, p1: String, p2: String, winner: String) {
    if (winner == "") {
        coll.updateOne(Filters.eq("_id", p1), Updates.combine(Updates.inc("draws", 1), Updates.setOnInsert("_id", p1)), UpdateOptions().upsert(true))
        coll.updateOne(Filters.eq("_id", p2), Updates.combine(Updates.inc("draws", 1), Updates.setOnInsert("_id", p2)), UpdateOptions().upsert(true))
    } else {
        val loser = if (winner == p1) p2 else p1
        coll.updateOne(Filters.eq("_id", winner), Updates.combine(Updates.inc("wins", 1), Updates.setOnInsert("_id", winner)), UpdateOptions().upsert(true))
        coll.updateOne(Filters.eq("_id", loser), Updates.combine(Updates.inc("losses", 1), Updates.setOnInsert("_id", loser)), UpdateOptions().upsert(true))
    }
}

suspend fun fetchQuestions(coll: MongoCollection<Document>, count: Int, cats: List<String>, diffs: List<String>): List<Document> {
    val allQuestions = mutableListOf<Document>()
    
    val actualCats = if (cats.contains("Όλες")) ALL_CATEGORIES else cats
    
    actualCats.forEach { cat ->
        val filter = if (diffs.contains("Όλα")) {
            Filters.empty()
        } else {
            Filters.`in`("difficulty", diffs)
        }
        allQuestions.addAll(getQuestionsCollection(cat).find(filter).toList())
    }
    
    return allQuestions.shuffled().take(count)
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friendsDoc = database.getCollection<Document>("friends").find(Filters.eq("_id", user)).firstOrNull()
    val friendNames = friendsDoc?.get("friends") as? List<*> ?: emptyList<String>()
    
    val friends = friendNames.map { name ->
        val n = name.toString()
        FriendInfo(n, onlineUsers.containsKey(n))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val requestsDoc = database.getCollection<Document>("requests").find(Filters.eq("_id", user)).firstOrNull()
        
        val incoming = (requestsDoc?.get("incoming") as? List<*>)?.map { it.toString() } ?: emptyList()
        val outgoing = (requestsDoc?.get("outgoing") as? List<*>)?.map { it.toString() } ?: emptyList()

        val response = RequestLists(incoming, outgoing)
        val jsonResponse = gson.toJson(response)
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", jsonResponse))))
        println("SERVER: Sync RequestList for $user -> In: ${incoming.size}, Out: ${outgoing.size}")
    } catch (e: Exception) {
        println("SERVER ERROR (sendRequestList): ${e.message}")
    }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    // Find all users who have this person as a friend
    database.getCollection<Document>("friends").find(Filters.eq("friends", user)).toList().forEach { doc ->
        val otherUser = doc.getString("_id") ?: return@forEach
        onlineUsers[otherUser]?.let { sendFriendList(otherUser, it) }
    }
}

suspend fun watchDatabaseChanges() {
    coroutineScope {
        // Watch Requests Collection
        launch {
            try {
                val requestsColl = database.getCollection<Document>("requests")
                requestsColl.watch().collect { event ->
                    val docId = event.documentKey?.get("_id")
                    val userId = when {
                        docId?.isString == true -> docId.asString().value
                        docId?.isObjectId == true -> docId.asObjectId().value.toString()
                        else -> null
                    }
                    println("SERVER: DB Change detected in Requests for player: $userId")
                    if (userId != null) {
                        onlineUsers[userId]?.let { sendRequestList(userId, it) }
                    }
                }
            } catch (e: Exception) {
                println("SERVER ERROR (watch requests): ${e.message}")
            }
        }

        // Watch Friends Collection
        launch {
            try {
                val friendsColl = database.getCollection<Document>("friends")
                friendsColl.watch().collect { event ->
                    val docId = event.documentKey?.get("_id")
                    val userId = when {
                        docId?.isString == true -> docId.asString().value
                        docId?.isObjectId == true -> docId.asObjectId().value.toString()
                        else -> null
                    }
                    println("SERVER: DB Change detected in Friends for player: $userId")
                    if (userId != null) {
                        onlineUsers[userId]?.let { sendFriendList(userId, it) }
                    }
                }
            } catch (e: Exception) {
                println("SERVER ERROR (watch friends): ${e.message}")
            }
        }
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        // In survival mode, check if we still have players
        if (room.isSurvivalMode && room.players.none { !it.isEliminated }) {
            break
        }
        
        sendQuestionAndWait(room, room.questions[room.currentQuestionIndex])
        room.currentQuestionIndex++
    }

    if (room.isSuddenDeathEnabled && room.isGameRunning && !room.isSurvivalMode) {
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
        if (p.isEliminated) return@forEach

        if (p.lastAnswerIndex == correctIdx) {
            val points = if (p.doublePointsActive) 2 else 1
            if (!room.isSuddenDeathActive) p.score += points
            p.correctCount++
            p.doublePointsActive = false // Reset for next round
        } else {
            p.wrongCount++
            if (room.isSurvivalMode) {
                if (p.shieldActive) {
                    p.shieldActive = false // Shield protects once
                } else {
                    p.lives--
                    if (p.lives <= 0) {
                        p.isEliminated = true
                        p.lives = 0
                    }
                }
            }
        }
    }

    val options = question.get("options", List::class.java) as? List<String> ?: listOf()
    val statusText = if (room.isSurvivalMode) {
        val stats = room.players.joinToString("\n") { "${it.name}: ❤️${it.lives}${if (it.isEliminated) " (OUT)" else ""}" }
        "Σωστή απάντηση: ${options.getOrNull(correctIdx) ?: ""}\n\n$stats"
    } else {
        "Σωστή απάντηση: ${options.getOrNull(correctIdx) ?: ""}"
    }
    room.broadcast(GameMessage(MessageType.RESULT, "Server", statusText))
    delay(4000)
    
    // Send updated stats (lives/powerups) to each player if enabled
    if (room.isSurvivalMode || room.powerupsEnabled) {
        room.players.forEach { p ->
            val pData = mapOf("lives" to p.lives, "powerups" to p.powerups)
            try { p.session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PLAYER_STATS, "Server", gson.toJson(pData))))) } catch (e: Exception) {}
        }
    }

    room.broadcast(GameMessage(MessageType.LEADERBOARD, "Server", ""))
    delay(1000)
}
