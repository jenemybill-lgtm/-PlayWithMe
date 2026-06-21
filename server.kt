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

// --- CONFIGURATION v2.0 (HARDENED) ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"

// SECURITY: MONGODB_URI via Environment Variables or fallback string
val MONGODB_URI = System.getenv("MONGODB_URI")
    ?: "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

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
    // Thread-safe player list
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
        sessions.forEach { try { it.send(Frame.Text(text)) } catch(e: Exception) {} }
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
        println("SERVER: CONNECTED TO MONGODB ATLAS")
    } catch (e: Exception) { println("SERVER DATABASE ERROR: ${e.message}") }
}

fun main() {
    GlobalScope.launch { initDatabase() }
    val port = System.getenv("PORT")?.toInt() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                // Initial Version Check
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

                // --- CLEANUP ON DISCONNECT ---
                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                val roomsToClose = mutableListOf<String>()
                rooms.forEach { (code, room) ->
                    if (room.hostSession == this || room.players.any { it.session == this }) {
                        runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποχώρησε.")) }
                        roomsToClose.add(code)
                    }
                }
                roomsToClose.forEach { rooms.remove(it) }
            }
        }
    }.start(wait = true)
}

// Case-Insensitive Canonical Name Lookup
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

            onlineUsers[officialName] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))

            // SYNC SOCIAL ON LOGIN (Deep Scan)
            sendFriendList(officialName, session)
            
            // DEEP SCAN FOR REQUESTS (Case-Insensitive)
            val reqDoc = requestsColl.find(Filters.regex("target", "^${Pattern.quote(officialName)}$", "i")).toList().firstOrNull()
            val rawReqs = reqDoc?.get("requesters", List::class.java)
            val requesters = rawReqs?.mapNotNull { it?.toString() } ?: emptyList()
            
            if (requesters.isNotEmpty()) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Έχεις ${requesters.size} εκκρεμή αιτήματα φιλίας!"))))
            }
            sendRequestList(officialName, session)
            
            notifyFriendsStatus(officialName, true)

            // Deliver Pending messages
            pendingColl.find(Filters.regex("target", "^${Pattern.quote(officialName)}$", "i")).toList().forEach { doc ->
                try {
                    val type = MessageType.valueOf(doc.getString("type"))
                    session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc.getString("content")))))
                } catch (e: Exception) {}
            }
            pendingColl.deleteMany(Filters.regex("target", "^${Pattern.quote(officialName)}$", "i"))
        }

        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            if (usersColl.countDocuments(Filters.regex("name", "^${Pattern.quote(name)}$", "i")) == 0L) {
                usersColl.insertOne(Document("name", name))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            } else { session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN")))) }
        }

        MessageType.ADD_FRIEND -> {
            val user = msg.sender.trim(); val target = msg.content?.trim() ?: return
            val user = canonicalName(usersColl, user)
            val targetDoc = usersColl.find(Filters.regex("name", "^${Pattern.quote(target)}$", "i")).toList().firstOrNull()

            if (targetDoc != null) {
                val officialTarget = targetDoc.getString("name")
                if (friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", officialTarget))) > 0L) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }
                
                // SAFE UPSERT: Set target field on insert
                requestsColl.updateOne(
                    Filters.regex("target", "^${Pattern.quote(officialTarget)}$", "i"),
                    Updates.combine(
                        Updates.addToSet("requesters", user),
                        Updates.setOnInsert("target", officialTarget)
                    ),
                    UpdateOptions().upsert(true)
                )

                // IMMEDIATE RELAY IF ONLINE
                onlineUsers.entries.forEach { (onlineName, sess) ->
                    if (onlineName.equals(officialTarget, ignoreCase = true)) {
                        sendRequestList(onlineName, sess)
                        sess.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
                    }
                }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε στον $officialTarget!"))))
            } else { session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης $target δεν βρέθηκε!")))) }
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = canonicalName(usersColl, msg.content?.trim() ?: return)

            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            friendsColl.insertOne(Document("user", user).append("friend", requester))
            friendsColl.insertOne(Document("user", requester).append("friend", user))
            sendFriendList(user, session)
            sendRequestList(user, session)
            onlineUsers.entries.find { it.key.equals(requester, ignoreCase = true) }?.let { sendFriendList(it.key, it.value); sendRequestList(it.key, it.value) }
        }

        MessageType.REJECT_REQUEST -> {
            val user = canonicalName(usersColl, msg.sender.trim())
            val requester = canonicalName(usersColl, msg.content?.trim() ?: return)
            requestsColl.updateOne(Filters.regex("target", "^${Pattern.quote(user)}$", "i"), Updates.pull("requesters", requester))
            sendRequestList(user, session)
        }

        MessageType.CREATE_ROOM -> {
            var code: String
            do { code = (10000..99999).random().toString() } while (rooms.containsKey(code))
            val room = GameRoom(code, session)
            room.players.add(Player(msg.sender, session))
            rooms[code] = room
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
            room.updateHostPlayerCount()
        }

        MessageType.JOIN -> {
            val room = rooms[msg.content]
            if (room != null) {
                if (room.players.none { it.name == msg.sender }) {
                    room.players.add(Player(msg.sender, session))
                }
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", room.code))
                room.updateHostPlayerCount()
            } else { session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το δωμάτιο δεν βρέθηκε!")))) }
        }

        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                // CANCEL PREVIOUS LOOP
                room.gameJob?.cancel()

                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                room.questions = setup["questions"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()

                // AGGRESSIVE RESET FOR ROOM REUSE
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                room.waitingForAnswers = false
                room.players.forEach {
                    it.score = 0
                    it.totalTime = 0
                    it.hasAnswered = false
                    it.lastAnswerIndex = -1
                }
                
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

        MessageType.INVITE -> {
            val target = msg.content ?: return; val room = rooms.values.find { it.hostSession == session }
            if (room != null) {
                val inviteStr = "${msg.sender}|${room.code}"
                val targetEntry = onlineUsers.entries.find { it.key.equals(target, ignoreCase = true) }
                if (targetEntry != null) targetEntry.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.INVITE_RECEIVED, "Server", inviteStr))))
                else pendingColl.insertOne(Document("target", target).append("type", "INVITE_RECEIVED").append("content", inviteStr))
            }
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Document>("friends").find(Filters.eq("user", user)).toList().map { doc ->
        val fName = doc.getString("friend")
        FriendInfo(fName, onlineUsers.containsKey(fName))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    try {
        val requestsColl = database.getCollection<Document>("requests")
        val doc = requestsColl.find(Filters.regex("target", "^${Pattern.quote(user)}$", "i")).toList().firstOrNull()
        val rawReqs = doc?.get("requesters", List::class.java)
        val requesters = rawReqs?.mapNotNull { it?.toString() } ?: emptyList()

        val msg = GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters))
        session.send(Frame.Text(gson.toJson(msg)))
    } catch (e: Exception) {
        println("SERVER SOCIAL ERROR: ${e.message}")
    }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    database.getCollection<Document>("friends").find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc.getString("user")
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        val question = room.questions[room.currentQuestionIndex]
        val correctIdx = (question["correctAnswerIndex"] as Double).toInt()
        room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
        room.waitingForAnswers = true
        room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))

        var elapsed = 0
        while (elapsed < room.timerSeconds && room.waitingForAnswers && room.isGameRunning) {
            delay(1000); elapsed++ }

        room.waitingForAnswers = false
        room.players.forEach { if (it.lastAnswerIndex == correctIdx) it.score++ }
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
        room.currentQuestionIndex = 0
    }
}
