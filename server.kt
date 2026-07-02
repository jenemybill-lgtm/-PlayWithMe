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
const val LATEST_VERSION_NAME = "3.2.4"
const val LATEST_VERSION_CODE = 23
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
val MONGODB_URI = System.getenv("MONGODB_URI") ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

val ADMIN_USERS = setOf("jenemybill")
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
    CHALLENGE_FRIEND, CHALLENGE_RECEIVED, CHALLENGE_RESULT, CHALLENGE_SETUP,
    GET_LEADERBOARD, LEADERBOARD_DATA,
    UPLOAD_QUESTIONS, GET_SOLO_QUESTIONS, SOLO_QUESTIONS_DATA,
    // Offline Sync & Question Moderation
    SUGGEST_QUESTION, SUGGEST_QUESTION_RESPONSE,
    GET_PENDING_QUESTIONS, PENDING_QUESTIONS_DATA,
    APPROVE_QUESTION, QUESTION_MODERATION_RESPONSE,
    SYNC_OFFLINE_SCORES, SYNC_OFFLINE_SCORES_RESPONSE,
    CHECK_NEW_QUESTIONS, NEW_QUESTIONS_DATA,
    USE_POWERUP, POWERUP_EFFECT, PLAYER_STATS,
    DUEL_CHALLENGE, DUEL_CHALLENGE_RECEIVED, DUEL_ACCEPT, DUEL_SETUP, DUEL_START, DUEL_FINISH, DUEL_RESULT, DUEL_HISTORY_REQUEST, DUEL_HISTORY_DATA,
    REMOVE_FRIEND, ROOM_PLAYERS_UPDATE, CANCEL_MATCHMAKING
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
    var totalTime: Long = 0
)
data class FriendInfo(val name: String, var isOnline: Boolean)
data class RequestLists(val incoming: List<String>, val outgoing: List<String>)

val onlineUsers = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val duelMatchmakingQueue = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val userLanguages = ConcurrentHashMap<String, String>() // Track preferred language for each user
val rooms = ConcurrentHashMap<String, GameRoom>()
val gson = Gson()
lateinit var database: MongoDatabase

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = CopyOnWriteArrayList<Player>()
    var questions: List<Document> = emptyList()
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
            val names = players.map { it.name }.joinToString(", ")
            val count = players.size.toString()
            hostSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.PLAYER_COUNT, "Server", count))))
            broadcast(GameMessage(MessageType.ROOM_PLAYERS_UPDATE, "Server", names))
        } catch (e: Exception) {}
    }
}

// ==================== DATABASE ====================
suspend fun initDatabase() {
    try {
        val client = MongoClient.create(MONGODB_URI)
        database = client.getDatabase("playwithme")
        println("SERVER: DATABASE CONNECTED SUCCESSFULLY")
        
        // Final Migration to Categorized Plain Greek Schema
        migrateToFinalGreekSchema()
    } catch (e: Exception) {
        println("DATABASE ERROR: ${e.message}")
    }
}

suspend fun migrateToFinalGreekSchema() {
    ALL_CATEGORIES.forEach { cat ->
        val coll = getQuestionsCollection(cat)
        val questions = coll.find().toList()
        
        questions.forEach { doc ->
            val update = Document()
            var changed = false
            
            // 1. Simplify Text
            val textObj = doc.get("text")
            if (textObj is Document || textObj is Map<*, *>) {
                val textMap = if (textObj is Document) textObj else Document(textObj as Map<String, Any>)
                update.append("text", textMap.getString("el") ?: "")
                changed = true
            }

            // 2. Simplify Options
            val optionsObj = doc.get("options")
            if (optionsObj is Document || optionsObj is Map<*, *>) {
                val optionsMap = if (optionsObj is Document) optionsObj else Document(optionsObj as Map<String, Any>)
                update.append("options", optionsMap.getList("el", String::class.java) ?: emptyList<String>())
                changed = true
            }
            
            // 3. Remove qid
            if (doc.containsKey("qid")) {
                update.append("qid", Updates.unset("qid"))
                changed = true
            }
            
            if (changed) {
                coll.updateOne(Filters.eq("_id", doc.get("_id")), Document("\$set", update))
            }
        }
    }
    println("SERVER MIGRATION: Database simplified to Greek-only schema.")
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
                    duelMatchmakingQueue.remove(it)
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
                        if (!isHost) runBlocking { room.updateHostPlayerCount() }
                    }
                    isHost
                }
            }
        }
    }.start(wait = true)
}

// ==================== TRANSLATION (REMOVED - GREEK ONLY) ====================
fun translateQuestion(doc: Document): Document {
    return doc
}
suspend fun canonicalName(usersColl: MongoCollection<Document>, raw: String): String {
    val doc = usersColl.find(Filters.regex("name", "^${Pattern.quote(raw)}$", "i")).toList().firstOrNull()
    return doc?.getString("name") ?: raw
}

fun getQuestionsCollection(category: String): MongoCollection<Document> {
    val sanitized = category.trim()
    val base = if (sanitized == "Όλες" || sanitized == "Όλα") {
        "Γενικές Γνώσεις" 
    } else {
        sanitized
    }
    val collectionName = "questions_$base"
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
            val existing = usersColl.find(Filters.regex("name", "^${Pattern.quote(name)}$", "i")).firstOrNull()
            if (existing != null) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "EXISTS"))))
                return
            }
            usersColl.insertOne(Document("name", name).append("createdAt", Date()))
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
        }

        MessageType.LOGIN -> {
            val name = msg.sender.trim() // sender is username
            val official = canonicalName(usersColl, name)
            onlineUsers[official] = session
            
            // Preferred language removed - Greek only
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
            
            if (user.equals(officialTarget, ignoreCase = true)) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Δεν μπορείς να στείλεις αίτημα στον εαυτό σου!"))))
                return
            }

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

            // 2. Add to friends folders (BIDIRECTIONAL) using a clean list layout
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

            // 3. Sync both players immediately with fresh DB reads
            sendFriendList(user, session)
            sendRequestList(user, session)
            onlineUsers[requester]?.let {
                sendFriendList(requester, it)
                sendRequestList(requester, it)
            }
        }

        MessageType.REJECT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = msg.content?.trim() ?: return
            
            requestsColl.updateOne(Filters.eq("_id", user), Updates.pull("incoming", requester))
            requestsColl.updateOne(Filters.eq("_id", requester), Updates.pull("outgoing", user))

            // Cleanup empty documents
            val myReq = requestsColl.find(Filters.eq("_id", user)).firstOrNull()
            if (myReq != null && (myReq["incoming"] as? List<*>)?.isEmpty() == true && (myReq["outgoing"] as? List<*>)?.isEmpty() == true) {
                requestsColl.deleteOne(Filters.eq("_id", user))
            }
            val targetReq = requestsColl.find(Filters.eq("_id", requester)).firstOrNull()
            if (targetReq != null && (targetReq["incoming"] as? List<*>)?.isEmpty() == true && (targetReq["outgoing"] as? List<*>)?.isEmpty() == true) {
                requestsColl.deleteOne(Filters.eq("_id", requester))
            }

            // Sync both
            sendRequestList(user, session)
            onlineUsers[requester]?.let { sendRequestList(requester, it) }
        }

        MessageType.REMOVE_FRIEND -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val friendToRemove = msg.content?.trim() ?: return

            friendsColl.updateOne(Filters.eq("_id", user), Updates.pull("friends", friendToRemove))
            friendsColl.updateOne(Filters.eq("_id", friendToRemove), Updates.pull("friends", user))

            sendFriendList(user, session)
            onlineUsers[friendToRemove]?.let { sendFriendList(friendToRemove, it) }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης $friendToRemove αφαιρέθηκε από τους φίλους."))))
        }

        MessageType.INVITE -> {
            val host = canonicalName(usersColl, msg.sender.trim())
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
            val host = canonicalName(usersColl, msg.sender.trim())
            val rawContent = msg.content ?: return
            
            if (rawContent.startsWith("RANDOM|")) {
                // 1. Find random opponent from USERS collection
                val allUsersExceptMe = usersColl.find(Filters.ne("name", host)).toList()
                val target = allUsersExceptMe.shuffled().firstOrNull()?.getString("name")
                
                if (target != null) {
                    val catsStr = rawContent.substringAfter("RANDOM|")
                    val cats = catsStr.split(",")
                    
                    // 2. Fetch Questions (Fixed Signature)
                    val questions = fetchQuestions(10, cats, listOf("Όλα"))
                    
                    val now = System.currentTimeMillis()
                    val challengeId = "CHAL_${now}_${(1000..9999).random()}"
                    val chalDoc = Document("_id", challengeId)
                        .append("sender", host)
                        .append("receiver", target)
                        .append("categories", catsStr)
                        .append("questions", questions)
                        .append("timestamp", now)
                        .append("status", "SENDER_WAITING")
                    
                    database.getCollection<Document>("challenges").insertOne(chalDoc)
                    
                    // 3. Send setup info to SENDER to play first
                    val setupStr = "$challengeId|$target|${gson.toJson(questions)}"
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_SETUP, "Server", setupStr))))
                } else {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Δεν βρέθηκε διαθέσιμος αντίπαλος. Δοκίμασε ξανά!"))))
                }
            } else {
                // Standard direct challenge logic...
                val parts = rawContent.split("|")
                val target = parts.getOrNull(1) ?: return
                val seed = parts.getOrNull(2) ?: System.currentTimeMillis().toString()
                
                val challengeStr = "SOLO|$host|$seed"
                val targetSession = onlineUsers[target]
                if (targetSession != null) {
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
                } else {
                    pendingColl.insertOne(Document("target", target).append("type", "CHALLENGE_RECEIVED").append("content", challengeStr))
                }
            }
        }

        MessageType.CREATE_ROOM -> {
            val code = (10000..99999).random().toString()
            val room = GameRoom(code, session)
            room.players.add(Player(msg.sender, session))
            rooms[code] = room
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
            room.updateHostPlayerCount()
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
                
                // --- ALWAYS FETCH FROM DB (Greek Only) ---
                room.questions = fetchQuestions(count, categories, difficulties)
                
                room.timerSeconds = (setup["timer"] as? Double)?.toInt() ?: 20
                room.isSuddenDeathEnabled = setup["isSuddenDeath"] as? Boolean ?: true
                room.isSpeedMode = setup["isSpeedMode"] as? Boolean ?: false
                room.isSuddenDeathActive = false
                room.isGameRunning = true
                room.currentQuestionIndex = 0

                room.players.forEach { 
                    it.score = 0; it.correctCount = 0; it.wrongCount = 0; it.totalTime = 0; it.isEliminated = false
                }

                room.broadcast(GameMessage(MessageType.START_GAME, "Server", room.timerSeconds.toString()))
                room.gameJob = CoroutineScope(Dispatchers.Default).launch { delay(5000); runGameLoop(room) }
            }
        }

        MessageType.ANSWER -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && player != null) {
                if (msg.content == "CLIENT_READY") {
                    player.hasAnswered = true 
                    println("SERVER: Structural Handshake -> Player ${player.name} is READY.")
                    return
                }

                if (room.waitingForAnswers && !player.hasAnswered && !player.isEliminated) {
                    val parts = msg.content?.split("|") ?: return
                    player.hasAnswered = true
                    player.lastAnswerIndex = parts.getOrNull(0)?.toIntOrNull() ?: -1
                    player.totalTime += parts.getOrNull(1)?.toLongOrNull() ?: 0
                    if (room.players.filter { !it.isEliminated }.all { it.hasAnswered }) room.waitingForAnswers = false
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
                    
                    // Case-insensitive text check for robustness
                    val existingTexts = coll.find().projection(Document("text", 1)).toList()
                        .mapNotNull { it.getString("text")?.lowercase(Locale.ROOT)?.trim() }.toSet()
                    
                    val toInsert = mutableListOf<Document>()
                    questions.forEach { q ->
                        val text = q["text"] as? String ?: return@forEach
                        val normalized = text.lowercase(Locale.ROOT).trim()
                        
                        if (!existingTexts.contains(normalized)) {
                            // Save as plain Greek document
                            val options = q["options"] as? List<String> ?: emptyList()
                            val doc = Document("text", text)
                                .append("options", options)
                                .append("correctAnswerIndex", (q["correctAnswerIndex"] as? Double)?.toInt() ?: 0)
                                .append("category", category)
                                .append("difficulty", q["difficulty"]?.toString() ?: "Μέτριο")
                                .append("isApproved", true)
                            toInsert.add(doc)
                            added++
                        }
                    }
                    
                    if (toInsert.isNotEmpty()) {
                        coll.insertMany(toInsert)
                    }
                }
                
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο έξυπνος συγχρονισμός ολοκληρώθηκε! Προστέθηκαν $added νέες ερωτήσεις."))))
                println("SERVER: Smart Sync (Greek) from ${msg.sender} -> Added: $added")
            } catch (e: Exception) {
                println("SERVER ERROR (bulk sync): ${e.message}")
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Αποτυχία συγχρονισμού: ${e.message}"))))
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

        MessageType.GET_SOLO_QUESTIONS -> {
            val allSoloQuestions = mutableListOf<Document>()
            val diffCounts = mapOf("Εύκολο" to 15, "Μέτριο" to 45, "Δύσκολο" to 40)
            
            diffCounts.forEach { (diff, targetCount) ->
                val collected = mutableListOf<Document>()
                val shuffledCats = ALL_CATEGORIES.shuffled()
                
                for (cat in shuffledCats) {
                    if (collected.size >= targetCount) break
                    val fromCat = getQuestionsCollection(cat).find(Filters.eq("difficulty", diff)).toList()
                    collected.addAll(fromCat)
                }
                
                allSoloQuestions.addAll(collected.distinctBy { it.getString("text") }.shuffled().take(targetCount))
            }
            
            // Final check - if STILL empty (DB is empty), we must notify
            if (allSoloQuestions.isEmpty()) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Δεν βρέθηκαν ερωτήσεις στη βάση δεδομένων!"))))
                return
            }

            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.SOLO_QUESTIONS_DATA, "Server", gson.toJson(allSoloQuestions)))))
        }

        MessageType.CHALLENGE_RESULT -> {
            val sender = msg.sender
            val parts = msg.content?.split("|") ?: return
            val chalId = parts.getOrNull(0) ?: return
            
            // Format: CHAL_ID|score|time
            val score = parts.getOrNull(1)?.toIntOrNull() ?: 0
            val time = parts.getOrNull(2)?.toLongOrNull() ?: 0
            
            val challengesColl = database.getCollection<Document>("challenges")
            val chal = challengesColl.find(Filters.eq("_id", chalId)).firstOrNull() ?: return
            
            val isSender = sender == chal.getString("sender")
            val isReceiver = sender == chal.getString("receiver")
            
            if (isSender) {
                // Sender finished their turn
                challengesColl.updateOne(Filters.eq("_id", chalId), Updates.combine(
                    Updates.set("sender_score", score),
                    Updates.set("sender_time", time),
                    Updates.set("status", "RECEIVER_WAITING")
                ))
                
                // Notify Receiver
                val target = chal.getString("receiver") ?: return
                val cats = chal.getString("categories") ?: ""
                val questions = chal.get("questions", List::class.java) as? List<Document> ?: emptyList()
                val qJson = gson.toJson(questions)

                val challengeStr = "RANDOM|$sender|$cats|$chalId|$qJson"
                val targetSession = onlineUsers[target]
                if (targetSession != null) {
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
                } else {
                    pendingColl.insertOne(Document("target", target).append("type", "CHALLENGE_RECEIVED").append("content", challengeStr))
                }
            } else if (isReceiver) {
                // Receiver finished their turn
                val s1 = chal.getInteger("sender_score") ?: 0
                val t1 = chal.getLong("sender_time") ?: 0L
                val s2 = score
                val t2 = time
                
                val p1 = chal.getString("sender") ?: ""
                val p2 = sender
                
                var winner = ""
                if (s1 > s2) winner = p1
                else if (s2 > s1) winner = p2
                else if (t1 < t2) winner = p1
                else if (t2 < t1) winner = p2
                
                // 1. Update Global Stats
                updateDuelStats(duelStatsColl, p1, p2, winner)
                
                // 2. Notify Both
                val resultMsg = if (winner == "") "ΙΣΟΠΑΛΙΑ!" else "ΝΙΚΗΤΗΣ: $winner!"
                onlineUsers[p1]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Challenge Result: $resultMsg"))))
                onlineUsers[p2]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Challenge Result: $resultMsg"))))
                
                // 3. CLEANUP: Delete challenge
                challengesColl.deleteOne(Filters.eq("_id", chalId))
                println("SERVER: Challenge $chalId COMPLETED and DELETED.")
            }
        }

        // ==================== LEADERBOARD ====================
        MessageType.GET_LEADERBOARD -> {
            val mode = msg.content ?: "SOLO"
            println("SERVER: Leaderboard request for mode: $mode")
            
            val targetColl = when(mode) {
                "BLITZ" -> database.getCollection<Document>("blitz_leaderboard")
                "DUEL" -> database.getCollection<Document>("duel_stats")
                else -> database.getCollection<Document>("solo_leaderboard")
            }
            
            val sortField = if (mode == "DUEL") "wins" else "maxScore"
            
            val top = targetColl.find().sort(Document(sortField, -1)).limit(50).toList()
                .map { 
                    if (mode == "DUEL") {
                        // DuelStats uses _id as name
                        mapOf("name" to (it.getString("_id") ?: "Anon"), "score" to (it.getInteger("wins") ?: 0))
                    } else {
                        mapOf("name" to (it.getString("name") ?: "Anon"), "score" to (it.getInteger("maxScore") ?: 0)) 
                    }
                }
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LEADERBOARD_DATA, "Server", gson.toJson(top)))))
        }

        // ==================== MODERATION (ADMIN) ====================
        MessageType.GET_PENDING_QUESTIONS -> {
            if (ADMIN_USERS.contains(msg.sender)) {
                if (msg.content == "CHALLENGES") {
                    val challenges = database.getCollection<Document>("challenges")
                        .find(Filters.or(Filters.eq("sender", msg.sender), Filters.eq("receiver", msg.sender))).toList()
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PENDING_QUESTIONS_DATA, "Server", gson.toJson(challenges)))))
                } else {
                    val pending = database.getCollection<Document>("suggested_questions")
                        .find(Filters.eq("isApproved", false)).toList()
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PENDING_QUESTIONS_DATA, "Server", gson.toJson(pending)))))
                }
            } else if (msg.content == "CHALLENGES") {
                // Non-admin can also see their own challenges
                val challenges = database.getCollection<Document>("challenges")
                    .find(Filters.or(Filters.eq("sender", msg.sender), Filters.eq("receiver", msg.sender))).toList()
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.PENDING_QUESTIONS_DATA, "Server", gson.toJson(challenges)))))
            }
        }

        MessageType.APPROVE_QUESTION -> {
            if (ADMIN_USERS.contains(msg.sender)) {
                try {
                    val rawContent = msg.content ?: return
                    // Robust ID extraction: Look for a 24-character hex string anywhere in the content
                    val idMatch = Regex("[a-fA-F0-9]{24}").find(rawContent)
                    val cleanId = idMatch?.value
                    
                    if (cleanId == null || cleanId.length != 24) {
                        println("SERVER ERROR: Failed to extract valid 24-char hex ID from: $rawContent")
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Μη έγκυρο ID ερώτησης (Format Error)."))))
                        return
                    }
                    
                    val id = org.bson.types.ObjectId(cleanId)
                    val suggestionsColl = database.getCollection<Document>("suggested_questions")
                    
                    // 1. Find the question in suggestions
                    val questionDoc = suggestionsColl.find(Filters.eq("_id", id)).firstOrNull()
                    
                    if (questionDoc != null) {
                        val category = questionDoc.getString("category") ?: "Άλλα"
                        val targetColl = getQuestionsCollection(category)
                        
                        // 2. Check for duplicate text in target category
                        val text = questionDoc.getString("text") ?: ""
                        val existing = targetColl.find(Filters.eq("text", text)).firstOrNull()
                        
                        if (existing == null) {
                            // 3. Move to proper collection with approved flag
                            val approvedDoc = Document(questionDoc).apply {
                                append("isApproved", true)
                                append("approvedAt", Date())
                                // REMOVE ARABIC SUPPORT
                                remove("ar")
                            }
                            targetColl.insertOne(approvedDoc)
                            println("SERVER: Question $cleanId APPROVED and moved to $category")
                        } else {
                            println("SERVER: Question $cleanId already exists in $category, just deleting from suggestions.")
                        }

                        // 4. Always delete from suggestions after successful process
                        val result = suggestionsColl.deleteOne(Filters.eq("_id", id))
                        if (result.deletedCount > 0) {
                            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION_MODERATION_RESPONSE, "Server", "APPROVED"))))
                        }
                    } else {
                        println("SERVER ERROR: Question $cleanId NOT FOUND in suggestions during approval.")
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Η ερώτηση δεν βρέθηκε στις προτάσεις."))))
                    }
                } catch (e: Exception) {
                    println("APPROVE ERROR: ${e.message}")
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Σφάλμα έγκρισης: ${e.message}"))))
                }
            }
        }

        // MessageType.REJECT_QUESTION REMOVED

        // ==================== DISCOVER PLAYERS REMOVED ====================

        MessageType.DUEL_CHALLENGE -> {
            val host = canonicalName(usersColl, msg.sender.trim())
            val content = msg.content ?: return
            val parts = content.split("|")
            val targetRaw = parts.getOrNull(0) ?: return
            val isLive = parts.getOrNull(1) == "LIVE"

            if (targetRaw == "RANDOM") {
                if (isLive) {
                    // Find someone else in the queue for LIVE match
                    val opponentEntry = duelMatchmakingQueue.entries.find { it.key != host }
                    if (opponentEntry != null) {
                        val opponentName = opponentEntry.key
                        val opponentSession = opponentEntry.value
                        duelMatchmakingQueue.remove(opponentName)

                        // Pair them!
                        val duelId = "DUEL_RAND_${System.currentTimeMillis()}_${(1000..9999).random()}"
                        val duelDoc = Document("_id", duelId)
                            .append("player1", host)
                            .append("player2", opponentName)
                            .append("status", "SETUP")
                            .append("isLive", true)
                            .append("p1_ready", false)
                            .append("p2_ready", false)
                            .append("isRandom", true)
                            .append("createdAt", Date())
                        
                        duelsColl.insertOne(duelDoc)

                        // Notify both
                        val setupStr = "$duelId|$host|$opponentName|false" // false = NOT async (live)
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", setupStr))))
                        opponentSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", setupStr))))
                    } else {
                        // Add to queue
                        duelMatchmakingQueue[host] = session
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Αναμονή για αντίπαλο... (Matchmaking)"))))
                    }
                } else {
                    // ASYNC Random Matchmaking
                    // 1. Look for an existing "open" random duel created by someone else
                    val openDuel = duelsColl.find(Filters.and(
                        Filters.eq("player2", "RANDOM_SEARCH"),
                        Filters.ne("player1", host),
                        Filters.eq("isLive", false)
                    )).firstOrNull()

                    if (openDuel != null) {
                        val duelId = openDuel.getString("_id")
                        val p1 = openDuel.getString("player1")
                        duelsColl.updateOne(Filters.eq("_id", duelId), Updates.set("player2", host))
                        
                        val setupStr = "$duelId|$p1|$host|true" // true = async
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", setupStr))))
                    } else {
                        // 2. Create a new open duel and let player start playing NOW
                        val duelId = "DUEL_RAND_ASYNC_${System.currentTimeMillis()}"
                        val duelDoc = Document("_id", duelId)
                            .append("player1", host)
                            .append("player2", "RANDOM_SEARCH")
                            .append("status", "SETUP")
                            .append("isLive", false)
                            .append("p1_ready", false)
                            .append("p2_ready", false)
                            .append("createdAt", Date())
                        duelsColl.insertOne(duelDoc)

                        val setupStr = "$duelId|$host|Αναμονή...|true"
                        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", setupStr))))
                    }
                }
            } else {
                val target = canonicalName(usersColl, targetRaw)
                // Create Duel record IMMEDIATELY so sender can go to setup
                val duelId = "DUEL_${System.currentTimeMillis()}_${(1000..9999).random()}"
                val duelDoc = Document("_id", duelId)
                    .append("player1", host)
                    .append("player2", target)
                    .append("status", "SETUP")
                    .append("isLive", isLive)
                    .append("p1_ready", false)
                    .append("p2_ready", false)
                    .append("createdAt", Date())
                
                duelsColl.insertOne(duelDoc)

                // Send SETUP to sender (challenger) immediately
                val responseStr = "$duelId|$host|$target|${!isLive}"
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", responseStr))))

                // Notify Target
                val challengeStr = "$host|$isLive"
                val targetSession = onlineUsers[target]
                if (targetSession != null) {
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_CHALLENGE_RECEIVED, "Server", challengeStr))))
                } else {
                    pendingColl.insertOne(Document("target", target).append("type", "DUEL_CHALLENGE_RECEIVED").append("content", challengeStr))
                }
            }
        }

        MessageType.CANCEL_MATCHMAKING -> {
            duelMatchmakingQueue.remove(msg.sender)
            println("SERVER: Matchmaking CANCELLED by ${msg.sender}")
        }

        MessageType.DUEL_ACCEPT -> {
            val acceptor = msg.sender
            val host = msg.content?.split("|")?.getOrNull(0) ?: return
            
            // Find the pending duel
            val duel = duelsColl.find(Filters.and(
                Filters.eq("player1", host),
                Filters.eq("player2", acceptor),
                Filters.eq("status", "SETUP")
            )).firstOrNull() ?: return
            
            val duelId = duel.getString("_id")
            val isLive = duel.getBoolean("isLive") ?: false
            
            val responseStr = "$duelId|$host|$acceptor|$isLive"
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.DUEL_SETUP, "Server", responseStr))))
        }

        MessageType.DUEL_START -> {
            val content = msg.content ?: return
            val parts = content.split("|")
            val duelId = parts.getOrNull(0) ?: return
            val player = msg.sender
            
            val duel = duelsColl.find(Filters.eq("_id", duelId)).firstOrNull() ?: return
            
            val isP1 = player == duel.getString("player1")
            val updateField = if (isP1) "p1_ready" else "p2_ready"
            val catsField = if (isP1) "p1_cats" else "p2_cats"
            
            duelsColl.updateOne(Filters.eq("_id", duelId), Updates.combine(
                Updates.set(updateField, true),
                Updates.set(catsField, parts.getOrNull(1) ?: "")
            ))
            
            val updated = duelsColl.find(Filters.eq("_id", duelId)).firstOrNull() ?: return
            
            // Fetch or generate questions
            var questions = updated.get("questions", List::class.java) as? List<Document>
            if (questions == null) {
                // Generate questions based on FIRST player's ready signal (or both if we waited)
                val cats1 = updated.getString("p1_cats")?.split(",")?.filter { it.isNotBlank() } ?: emptyList()
                val cats2 = updated.getString("p2_cats")?.split(",")?.filter { it.isNotBlank() } ?: emptyList()
                val combinedCats = (cats1 + cats2).distinct()
                
                questions = fetchQuestions(10, if (combinedCats.isEmpty()) listOf("Όλες") else combinedCats, listOf("Όλα"))
                duelsColl.updateOne(Filters.eq("_id", duelId), Updates.set("questions", questions))
            }
            
            // Send START_GAME to the player who just signaled ready
            val gameSetup = mapOf(
                "timer" to 20,
                "questions" to gson.toJson(questions)
            )
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.START_GAME, "Server", gson.toJson(gameSetup)))))
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

                // SYNC TO SOLO LEADERBOARD as well
                val soloColl = database.getCollection<Document>("solo_leaderboard")
                soloColl.updateOne(Filters.eq("name", p1), Updates.combine(Updates.max("maxScore", s1), Updates.set("lastUpdate", Date())), UpdateOptions().upsert(true))
                soloColl.updateOne(Filters.eq("name", p2), Updates.combine(Updates.max("maxScore", s2), Updates.set("lastUpdate", Date())), UpdateOptions().upsert(true))
                
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

suspend fun fetchQuestions(count: Int, cats: List<String>, diffs: List<String>): List<Document> {
    val allQuestions = mutableListOf<Document>()
    val actualCats = if (cats.contains("Όλες") || cats.contains("Όλα")) ALL_CATEGORIES else cats
    
    actualCats.forEach { cat ->
        val filter = if (diffs.contains("Όλα") || diffs.isEmpty()) {
            Filters.empty()
        } else {
            Filters.`in`("difficulty", diffs)
        }
        
        try {
            val fromCat = getQuestionsCollection(cat).find(filter).toList()
            allQuestions.addAll(fromCat)
        } catch (e: Exception) {
            println("SERVER ERROR: Could not fetch from category $cat: ${e.message}")
        }
    }
    
    // --- ROBUST FALLBACK: If primary fetch yields nothing, grab from ANY Greek collection ---
    if (allQuestions.isEmpty()) {
        println("SERVER: fetchQuestions returned 0 results for cats: $cats. Falling back to General EL...")
        ALL_CATEGORIES.forEach { cat ->
            if (allQuestions.size >= count) return@forEach
            val fallback = getQuestionsCollection(cat).find().limit(count).toList()
            allQuestions.addAll(fallback)
        }
    }
    
    // Final check - if STILL empty, return empty list (DB is truly empty)
    if (allQuestions.isEmpty()) {
        println("SERVER CRITICAL: All collections are empty!")
        return emptyList()
    }
    
    return allQuestions.distinctBy { it.getString("text") ?: UUID.randomUUID().toString() }
        .shuffled().take(count)
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friendsDoc = database.getCollection<Document>("friends").find(Filters.eq("_id", user)).firstOrNull()
    val friendsList = (friendsDoc?.get("friends") as? List<*>)?.map { it.toString() } ?: emptyList()
    
    val friends = friendsList.map { FriendInfo(it, onlineUsers.containsKey(it)) }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val requestsColl = database.getCollection<Document>("requests")
        
        // Incoming: requests where target is current user
        val incomingDoc = requestsColl.find(Filters.eq("_id", user)).firstOrNull()
        val incoming = (incomingDoc?.get("incoming") as? List<*>)?.map { it.toString() } ?: emptyList()
        
        // Outgoing: requests where current user is in the requesters list
        val outgoingDoc = requestsColl.find(Filters.eq("_id", user)).firstOrNull()
        val outgoing = (outgoingDoc?.get("outgoing") as? List<*>)?.map { it.toString() } ?: emptyList()
        
        val response = RequestLists(incoming, outgoing)
        session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(response)))))
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
            // For Sudden Death, we now use the provided questions or a generic fallback if needed
            val sdQuestions = fetchQuestions(10, listOf("Όλες"), listOf("Όλα"))
            
            while (room.players.count { !it.isEliminated } > 1 && sdIdx < sdQuestions.size && room.isGameRunning) {
                val q = sdQuestions[sdIdx]
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

suspend fun sendQuestionAndWait(room: GameRoom, masterQuestion: Document) {
    val correctIdx = masterQuestion.getInteger("correctAnswerIndex") ?: 0
    room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
    room.waitingForAnswers = true
    
    // Structure: recipients (all in room)
    val recipients = room.players.map { it.session }.toMutableSet()
    recipients.add(room.hostSession)
    
    recipients.forEach { sess ->
        try { sess.send(Frame.Text(gson.toJson(GameMessage(MessageType.QUESTION, "Server", gson.toJson(masterQuestion))))) } catch (e: Exception) {}
    }

    var elapsed = 0
    while (elapsed < room.timerSeconds * 1000 && room.waitingForAnswers && room.isGameRunning) { delay(200); elapsed += 200 }

    room.waitingForAnswers = false
    room.players.forEach { p ->
        if (p.isEliminated) return@forEach

        if (p.lastAnswerIndex == correctIdx) {
            if (!room.isSuddenDeathActive) p.score += 1
            p.correctCount++
        } else {
            p.wrongCount++
        }
    }

    val options = masterQuestion.get("options", List::class.java) as? List<String> ?: listOf()
    val statusText = "Σωστή απάντηση: ${options.getOrNull(correctIdx) ?: ""}"
    room.broadcast(GameMessage(MessageType.RESULT, "Server", statusText))
    delay(4000)

    room.broadcast(GameMessage(MessageType.LEADERBOARD, "Server", ""))
    delay(1000)
}
