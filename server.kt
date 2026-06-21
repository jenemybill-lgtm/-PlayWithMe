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
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isGameRunning = false

    suspend fun broadcast(message: GameMessage) {
        val text = gson.toJson(message)
        val sessions = players.map { it.session }.toMutableSet()
        sessions.add(hostSession)
        sessions.forEach { 
            try { 
                it.send(Frame.Text(text)) 
            } catch(e: Exception) {
                println("BROADCAST ERROR: Failed to reach a session")
            } 
        }
    }

    suspend fun updateHostPlayerCount() {
        val msg = GameMessage(MessageType.PLAYER_COUNT, "Server", "${players.size}")
        try { 
            hostSession.send(Frame.Text(gson.toJson(msg))) 
        } catch(e: Exception) {}
    }
}

suspend fun initDatabase() {
    try {
        println("Connecting to MongoDB...")
        val client = MongoClient.create(MONGODB_URI)
        database = client.getDatabase("playwithme")
        println("Connected to MongoDB Atlas successfully!")
    } catch (e: Exception) {
        println("CRITICAL: MongoDB Connection Error: ${e.message}")
    }
}

fun main() {
    GlobalScope.launch { initDatabase() }
    val port = System.getenv("PORT")?.toInt() ?: 8080
    println("Starting server on port $port...")
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                // Safety Lock: Push version info immediately
                try {
                    val versionMsg = GameMessage(MessageType.VERSION_CHECK, "Server", "$LATEST_VERSION_CODE|$UPDATE_URL|$LATEST_VERSION_NAME")
                    send(Frame.Text(gson.toJson(versionMsg)))
                } catch(e: Exception) {}

                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val text = frame.readText()
                            val msg = gson.fromJson(text, GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) {
                    println("WS ERROR: ${e.message}")
                }

                // --- CLEANUP SAFETY LOCK ---
                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    println("User $disconnectedUser disconnected")
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                val roomsToClose = mutableListOf<String>()
                rooms.forEach { (code, room) ->
                    if (room.hostSession == this || room.players.any { it.session == this }) {
                        runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε. Το δωμάτιο έκλεισε.")) }
                        roomsToClose.add(code)
                    }
                }
                roomsToClose.forEach { rooms.remove(it) }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    if (!::database.isInitialized) {
        try {
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Database connecting..."))))
        } catch(e: Exception) {}
        return
    }

    val usersColl = database.getCollection<Map<String, Any>>("users")
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    val requestsColl = database.getCollection<Map<String, Any>>("requests")
    val pendingColl = database.getCollection<Map<String, Any>>("pending_messages")

    when (msg.type) {
        MessageType.LOGIN -> {
            val name = msg.content?.trim() ?: return
            println("SOCIAL: Login attempt for $name")
            
            // SAFEGUARD: Find official name from DB to prevent case mismatches
            val userDoc = usersColl.find(Filters.regex("name", "^$name$", "i")).toList().firstOrNull()
            val officialName = if (userDoc != null) userDoc["name"] as String else name
            
            onlineUsers[officialName] = session
            
            // Safety Lock: Always push current state on login
            try {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
                sendFriendList(officialName, session)
                sendRequestList(officialName, session)
                notifyFriendsStatus(officialName, true)
                
                // Deliver all missed messages
                pendingColl.find(Filters.regex("target", "^$officialName$", "i")).toList().forEach { doc ->
                    val type = doc["type"] as String
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.valueOf(type), "Server", doc["content"] as String))))
                }
                pendingColl.deleteMany(Filters.regex("target", "^$officialName$", "i"))
            } catch(e: Exception) {
                println("LOGIN ERROR for $officialName: ${e.message}")
            }
        }

        MessageType.REGISTER -> {
            val name = msg.content?.trim() ?: return
            val count = usersColl.countDocuments(Filters.eq("name", name))
            if (count > 0) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN"))))
            } else {
                usersColl.insertOne(mapOf("name" to name))
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            }
        }

        MessageType.ADD_FRIEND -> {
            val user = msg.sender.trim()
            val target = msg.content?.trim() ?: return
            println("SOCIAL: Friend Request from $user to $target")
            
            // SAFEGUARD: Find target user case-insensitively in users collection
            val targetUserDoc = usersColl.find(Filters.regex("name", "^$target$", "i")).toList().firstOrNull()
            
            if (targetUserDoc != null) {
                val officialTargetName = targetUserDoc["name"] as String
                
                // Check if already friends
                val isAlreadyFriend = friendsColl.countDocuments(Filters.and(Filters.eq("user", user), Filters.eq("friend", officialTargetName)))
                if (isAlreadyFriend > 0) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }

                // Add to MongoDB requests using official name
                requestsColl.updateOne(
                    Filters.eq("target", officialTargetName), 
                    Updates.addToSet("requesters", user), 
                    UpdateOptions().upsert(true)
                )
                
                // Safety Lock: Immediate push if online
                var targetSession: DefaultWebSocketServerSession? = onlineUsers[officialTargetName]
                if (targetSession == null) {
                    // Try case-insensitive lookup in the concurrent map
                    val entry = onlineUsers.entries.find { it.key.equals(officialTargetName, ignoreCase = true) }
                    targetSession = entry?.value
                }
                
                if (targetSession != null) {
                    sendRequestList(officialTargetName, targetSession)
                    targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα από $user!"))))
                }
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε στον $officialTargetName!"))))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης $target δεν βρέθηκε!"))))
            }
        }

        MessageType.ACCEPT_REQUEST -> {
            val user = msg.sender.trim()
            val requester = msg.content?.trim() ?: return
            requestsColl.updateOne(Filters.regex("target", "^$user$", "i"), Updates.pull("requesters", requester))
            friendsColl.insertOne(mapOf("user" to user, "friend" to requester))
            friendsColl.insertOne(mapOf("user" to requester, "friend" to user))
            
            // Safety Lock: Sync both parties immediately
            sendFriendList(user, session)
            sendRequestList(user, session)
            
            // Find requester session case-insensitively
            val reqEntry = onlineUsers.entries.find { it.key.equals(requester, ignoreCase = true) }
            reqEntry?.value?.let { 
                sendFriendList(reqEntry.key, it) 
                sendRequestList(reqEntry.key, it)
            }
        }

        MessageType.REJECT_REQUEST -> {
            val user = msg.sender.trim()
            val requester = msg.content?.trim() ?: return
            requestsColl.updateOne(Filters.regex("target", "^$user$", "i"), Updates.pull("requesters", requester))
            sendRequestList(user, session)
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
                if (room.players.none { it.session == session }) {
                    room.players.add(Player(msg.sender, session))
                }
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", room.code))
                room.updateHostPlayerCount()
            }
        }

        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                room.questions = setup["questions"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()
                room.isGameRunning = true
                room.currentQuestionIndex = 0
                // RESET STATE FOR REUSE
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
            val host = msg.sender
            val target = msg.content ?: return
            val room = rooms.values.find { it.hostSession == session }
            if (room != null) {
                val inviteStr = "$host|${room.code}"
                
                val targetEntry = onlineUsers.entries.find { it.key.equals(target, ignoreCase = true) }
                if (targetEntry != null) {
                    targetEntry.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.INVITE_RECEIVED, "Server", inviteStr))))
                } else {
                    pendingColl.insertOne(mapOf("target" to target, "type" to "INVITE_RECEIVED", "content" to inviteStr))
                }
            }
        }

        MessageType.CHALLENGE_FRIEND -> {
            val host = msg.sender
            val target = msg.content ?: return
            val seed = (1..99999).random().toString()
            val challengeStr = "$host|$seed"
            
            val targetEntry = onlineUsers.entries.find { it.key.equals(target, ignoreCase = true) }
            if (targetEntry != null) {
                targetEntry.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", challengeStr))))
            } else {
                pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RECEIVED", "content" to challengeStr))
            }
        }

        MessageType.CHALLENGE_RESULT -> {
            val parts = msg.content?.split("|") ?: return
            val target = parts[0]
            val resStr = "${msg.sender}|${parts[1]}|${parts[2]}"
            
            val targetEntry = onlineUsers.entries.find { it.key.equals(target, ignoreCase = true) }
            if (targetEntry != null) {
                targetEntry.value.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RESULT, "Server", resStr))))
            } else {
                pendingColl.insertOne(mapOf("target" to target, "type" to "CHALLENGE_RESULT", "content" to resStr))
            }
        }
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
    try {
        println("SOCIAL: Scanning requests for user: $user")
        val requestsColl = database.getCollection<Map<String, Any>>("requests")
        // SAFEGUARD: Case-insensitive search using Regex
        val doc = requestsColl.find(Filters.regex("target", "^$user$", "i")).toList().firstOrNull()
        
        val rawRequesters = doc?.get("requesters") as? List<*>
        val requesters = rawRequesters?.mapNotNull { it?.toString() } ?: emptyList()
        
        println("SOCIAL: Found ${requesters.size} requests for $user: $requesters")
        
        val msg = GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters))
        session.send(Frame.Text(gson.toJson(msg)))
    } catch (e: Exception) {
        println("SOCIAL ERROR (sendRequestList): ${e.message}")
    }
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    if (!::database.isInitialized) return
    val friendsColl = database.getCollection<Map<String, Any>>("friends")
    friendsColl.find(Filters.eq("friend", user)).toList().forEach { doc ->
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
