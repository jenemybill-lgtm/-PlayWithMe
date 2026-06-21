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

// --- CONFIGURATION v2.0 ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
const val MONGODB_URI = "mongodb+srv://jenemybill:Bill1908@jenemybill.jchjibj.mongodb.net/playwithme?retryWrites=true&w=majority"

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
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isGameRunning = false

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
        println("CONNECTED TO MONGODB ATLAS")
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

                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                val roomsToClose = mutableListOf<String>()
                rooms.forEach { (code, room) ->
                    if (room.hostSession == this || room.players.any { it.session == this }) {
                        runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποχώρησε. Το δωμάτιο έκλεισε.")) }
                        roomsToClose.add(code)
                    }
                }
                roomsToClose.forEach { rooms.remove(it) }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) return
    val usersColl = database.getCollection<Map<String, Any>>("users")
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    val requestsColl = database.getCollection<Map<String, Any>>("requests")
    val pendingColl = database.getCollection<Map<String, Any>>("pending_messages")

    when (msg.type) {
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            onlineUsers[name] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(name, session)
            
            // SCAN DATABASE FOR REQUESTS ON LOGIN
            val reqDoc = requestsColl.find(Filters.eq("target", name)).toList().firstOrNull()
            val requesters = (reqDoc?.get("requesters") as? List<*>)?.map { it.toString() } ?: emptyList()
            if (requesters.isNotEmpty()) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Έχεις ${requesters.size} εκκρεμή αιτήματα φιλίας!"))))
            }
            sendRequestList(name, session)
            notifyFriendsStatus(name, true)
            
            pendingColl.find(Filters.eq("target", name)).toList().forEach { doc ->
                try {
                    val type = MessageType.valueOf(doc["type"] as String)
                    session.send(Frame.Text(gson.toJson(GameMessage(type, "Server", doc["content"] as String))))
                } catch (e: Exception) {}
            }
            pendingColl.deleteMany(Filters.eq("target", name))
        }

        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            if (usersColl.countDocuments(Filters.eq("name", name)) == 0L) {
                usersColl.insertOne(mapOf("name" to name))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            } else { session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN")))) }
        }

        MessageType.ADD_FRIEND -> {
            val user = msg.sender.trim(); val target = msg.content?.trim() ?: return
            if (usersColl.countDocuments(Filters.eq("name", target)) > 0L) {
                if (friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", target))) > 0L) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }
                requestsColl.updateOne(Filters.eq("target", target), Updates.addToSet("requesters", user), UpdateOptions().upsert(true))
                
                onlineUsers[target]?.let { targetSession ->
                    sendRequestList(target, targetSession)
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
                }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
            } else { session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης δεν βρέθηκε!")))) }
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = msg.sender.trim(); val requester = msg.content?.trim() ?: return
            requestsColl.updateOne(Filters.eq("target", user), Updates.pull("requesters", requester))
            friendsColl.insertOne(mapOf("user" to user, "friend" to requester))
            friendsColl.insertOne(mapOf("user" to requester, "friend" to user))
            sendFriendList(user, session); onlineUsers[requester]?.let { sendFriendList(requester, it); sendRequestList(requester, it) }
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
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                room.questions = setup["questions"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                room.players.forEach { 
                    it.score = 0
                    it.totalTime = 0
                    it.hasAnswered = false
                    it.lastAnswerIndex = -1
                }
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", room.timerSeconds.toString()))
                CoroutineScope(Dispatchers.Default).launch { delay(2000); runGameLoop(room) }
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
                onlineUsers[target]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.INVITE_RECEIVED, "Server", inviteStr))))
                    ?: pendingColl.insertOne(mapOf("target" to target, "type" to "INVITE_RECEIVED", "content" to inviteStr))
            }
        }

        MessageType.CHALLENGE_FRIEND -> {
            val target = msg.content ?: return; val challengeStr = "${msg.sender}|${(1..99999).random()}"
            onlineUsers[target]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
                ?: pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RECEIVED", "content" to challengeStr))
        }

        MessageType.CHALLENGE_RESULT -> {
            val target = msg.content?.split("|")?.get(0) ?: return
            onlineUsers[target]?.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RESULT, "Server", msg.content))))
                ?: pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RESULT", "content" to msg.content))
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val friends = database.getCollection<Map<String, Any>>("friends").find(Filters.eq("user", user)).toList().map { doc ->
        val fName = doc["friend"] as String
        FriendInfo(fName, onlineUsers.containsKey(fName))
    }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(friends)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    if (!::database.isInitialized) return
    val doc = database.getCollection<Map<String, Any>>("requests").find(Filters.eq("target", user)).toList().firstOrNull()
    val requesters = (doc?.get("requesters") as? List<*>)?.map { it.toString() } ?: emptyList()
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters)))))
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    database.getCollection<Map<String, Any>>("friends").find(Filters.eq("friend", user)).toList().forEach { doc ->
        val friendName = doc["user"] as String
        onlineUsers[friendName]?.let { sendFriendList(friendName, it) }
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size && room.isGameRunning) {
        val question = room.questions[room.currentQuestionIndex]
        val correctIdx = (question["correctAnswerIndex"] as Double).toInt()
        room.players.forEach { it.hasAnswered = false }
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
        val rankingText = finalRank.withIndex().joinToString("\n") { "${it.index + 1}. ${it.value.name}: ${it.value.score} (Χρόνος: ${it.value.totalTime/1000}s)" }
        room.broadcast(GameMessage(MessageType.GAME_OVER, "Server", "ΤΕΛΙΚΗ ΚΑΤΑΤΑΞΗ:\n$rankingText"))
        room.isGameRunning = false
        room.currentQuestionIndex = 0
    }
}
