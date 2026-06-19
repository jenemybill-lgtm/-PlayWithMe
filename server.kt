import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import io.ktor.server.application.*
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*

// --- CONFIGURATION ---
const val LATEST_VERSION_NAME = "2.0"
const val LATEST_VERSION_CODE = 11
val UPDATE_URL = "https://github.com/jenemybill-lgtm/-PlayWithMe/releases/download/v$LATEST_VERSION_NAME/app-debug.apk"
const val DATA_FILE = "server_data.json"
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

// Persistence Models
data class ServerData(
    val users: MutableSet<String> = mutableSetOf(),
    val friends: MutableMap<String, MutableSet<String>> = mutableMapOf(),
    val requests: MutableMap<String, MutableSet<String>> = mutableMapOf()
)

var globalData = ServerData()
val onlineUsers = ConcurrentHashMap<String, DefaultWebSocketServerSession>()
val rooms = ConcurrentHashMap<String, GameRoom>()
val gson = Gson()

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = mutableListOf<Player>()
    var questions: List<Map<String, Any>> = emptyList()
    var extraQuestions: List<Map<String, Any>> = emptyList()
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isSuddenDeathEnabled = true
    var isSpeedMode = false
    var isSuddenDeathActive = false
    var isGameRunning = false

    suspend fun broadcast(message: GameMessage) {
        val text = gson.toJson(message)
        val uniqueSessions = players.map { it.session }.toMutableSet()
        uniqueSessions.add(hostSession)
        uniqueSessions.forEach { try { it.send(Frame.Text(text)) } catch(e: Exception) {} }
    }

    suspend fun updateHostPlayerCount() {
        val msg = GameMessage(MessageType.PLAYER_COUNT, "Server", "${players.size}")
        try { hostSession.send(Frame.Text(gson.toJson(msg))) } catch(e: Exception) {}
    }
}

fun loadData() {
    val file = File(DATA_FILE)
    if (file.exists()) {
        try {
            val json = file.readText()
            globalData = gson.fromJson(json, ServerData::class.java)
        } catch (e: Exception) { e.printStackTrace() }
    }
}

fun saveData() {
    try {
        val json = gson.toJson(globalData)
        File(DATA_FILE).writeText(json)
    } catch (e: Exception) { e.printStackTrace() }
}

fun main() {
    loadData()
    val port = System.getenv("PORT")?.toInt() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                val versionMsg = GameMessage(MessageType.VERSION_CHECK, "Server", "$LATEST_VERSION_CODE|$UPDATE_URL|$LATEST_VERSION_NAME")
                try { send(Frame.Text(gson.toJson(versionMsg))) } catch(e: Exception) {}

                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val text = frame.readText()
                            val msg = gson.fromJson(text, GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) { e.printStackTrace() }

                // Cleanup
                var disconnectedUser: String? = null
                onlineUsers.forEach { (name, session) -> if (session == this) disconnectedUser = name }
                if (disconnectedUser != null) {
                    onlineUsers.remove(disconnectedUser)
                    notifyFriendsStatus(disconnectedUser!!, false)
                }

                rooms.values.forEach { room ->
                    if (room.players.any { it.session == this } || room.hostSession == this) {
                        if (room.isGameRunning) {
                            runBlocking { room.broadcast(GameMessage(MessageType.RESTART, "Server", "Ένας παίκτης αποσυνδέθηκε. Επιστροφή στο μενού.")) }
                        }
                        room.players.removeIf { it.session == this }
                        runBlocking { room.updateHostPlayerCount() }
                    }
                }
                rooms.values.removeIf { it.hostSession == this }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    when (msg.type) {
        MessageType.REGISTER -> {
            val name = msg.content ?: return
            if (globalData.users.contains(name)) {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "TAKEN"))))
            } else {
                globalData.users.add(name)
                saveData()
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REGISTER_RESPONSE, "Server", "OK"))))
            }
        }
        MessageType.LOGIN -> {
            val name = msg.content ?: return
            onlineUsers[name] = session
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.LOGIN_RESPONSE, "Server", "OK"))))
            sendFriendList(name, session)
            sendRequestList(name, session)
            notifyFriendsStatus(name, true)
        }
        MessageType.ADD_FRIEND -> {
            val user = msg.sender
            val target = msg.content ?: return
            if (globalData.users.contains(target)) {
                if (globalData.friends[user]?.contains(target) == true) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }
                globalData.requests.getOrPut(target) { mutableSetOf() }.add(user)
                saveData()
                val targetSession = onlineUsers[target]
                if (targetSession != null) sendRequestList(target, targetSession)
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης $target δεν βρέθηκε!"))))
            }
        }
        MessageType.ACCEPT_REQUEST -> {
            val user = msg.sender
            val requester = msg.content ?: return
            globalData.requests[user]?.remove(requester)
            globalData.friends.getOrPut(user) { mutableSetOf() }.add(requester)
            globalData.friends.getOrPut(requester) { mutableSetOf() }.add(user)
            saveData()
            sendFriendList(user, session)
            sendRequestList(user, session)
            val reqSession = onlineUsers[requester]
            if (reqSession != null) sendFriendList(requester, reqSession)
        }
        MessageType.REJECT_REQUEST -> {
            val user = msg.sender
            val requester = msg.content ?: return
            globalData.requests[user]?.remove(requester)
            saveData()
            sendRequestList(user, session)
        }
        MessageType.CHALLENGE_FRIEND -> {
            val host = msg.sender
            val target = msg.content ?: return // target name
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                // We send a specific message to target. Content could be a random seed.
                val seed = (1..99999).random().toString()
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RECEIVED, "Server", "$host|$seed"))))
            }
        }
        MessageType.CHALLENGE_RESULT -> {
            // content = "target_name|score|progress"
            val parts = msg.content?.split("|") ?: return
            val target = parts[0]
            val score = parts[1]
            val progress = parts[2]
            val targetSession = onlineUsers[target]
            if (targetSession != null) {
                targetSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.CHALLENGE_RESULT, "Server", "${msg.sender}|$score|$progress"))))
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
                room.extraQuestions = setup["extra"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()
                room.isSuddenDeathEnabled = setup["isSuddenDeath"] as? Boolean ?: true
                room.isSpeedMode = setup["isSpeedMode"] as? Boolean ?: false
                room.currentQuestionIndex = 0
                room.isSuddenDeathActive = false
                room.isGameRunning = true
                room.players.forEach { it.score = 0; it.correctCount = 0; it.wrongCount = 0; it.isEliminated = false; it.hasAnswered = false; it.totalTime = 0 }
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
                val timeSpent = parts.getOrNull(1)?.toLongOrNull() ?: 0
                player.totalTime += timeSpent
                val activePlayers = room.players.filter { !it.isEliminated }
                if (activePlayers.all { it.hasAnswered }) room.waitingForAnswers = false
            }
        }
        else -> {}
    }
}

suspend fun sendFriendList(user: String, session: DefaultWebSocketServerSession) {
    val friends = globalData.friends[user] ?: return
    val infoList = friends.map { FriendInfo(it, onlineUsers.containsKey(it)) }
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.FRIEND_LIST, "Server", gson.toJson(infoList)))))
}

suspend fun sendRequestList(user: String, session: DefaultWebSocketServerSession) {
    val requesters = globalData.requests[user] ?: return
    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.REQUEST_LIST, "Server", gson.toJson(requesters.toList())))))
}

suspend fun notifyFriendsStatus(user: String, isOnline: Boolean) {
    onlineUsers.forEach { (name, session) ->
        if (globalData.friends[name]?.contains(user) == true) {
            sendFriendList(name, session)
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
            while (room.players.count { !it.isEliminated } > 1 && sdIdx < room.extraQuestions.size && room.isGameRunning) {
                val q = room.extraQuestions[sdIdx]
                sendQuestionAndWait(room, q)
                val correctIdx = (q["correctAnswerIndex"] as Double).toInt()
                room.players.filter { !it.isEliminated }.forEach { if (it.lastAnswerIndex != correctIdx) it.isEliminated = true }
                if (room.players.count { !it.isEliminated } == 0) room.players.forEach { if (it.score == topScore) it.isEliminated = false }
                sdIdx++
            }
        }
    }

    if (room.isGameRunning) {
        val finalRank = if (room.isSpeedMode) {
            room.players.sortedWith(compareByDescending<Player> { it.score }.thenBy { it.totalTime })
        } else {
            room.players.sortedByDescending { it.score }
        }

        val winner = finalRank.firstOrNull()
        val rankingText = finalRank.withIndex().joinToString("\n") {
            val timeStr = if (room.isSpeedMode) " [${it.value.totalTime/1000.0}s]" else ""
            "${it.index + 1}. ${it.value.name}: ${it.value.score}$timeStr (Σ:${it.value.correctCount} Λ:${it.value.wrongCount})"
        }

        room.isGameRunning = false
        room.players.forEach { p ->
            val resultMsg = if (p.name == winner?.name) "ΜΠΡΑΒΟ ΜΠΟΙ ΝΙΚΗΣΕΣ" else "ΔΕΝ ΠΕΙΡΑΖΕΙ , ΑΛΛΗ ΜΙΑ ΗΤΤΑ ΣΤΗΝ ΖΩΗ ΣΟΥ"
            try { p.session.send(Frame.Text(gson.toJson(GameMessage(MessageType.GAME_OVER, "Server", "$resultMsg\n\nΚΑΤΑΤΑΞΗ:\n$rankingText")))) } catch(e: Exception) {}
        }
    }
}

suspend fun sendQuestionAndWait(room: GameRoom, question: Map<String, Any>) {
    val correctIdx = (question["correctAnswerIndex"] as Double).toInt()
    room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
    room.waitingForAnswers = true
    room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))

    var elapsed = 0
    while (elapsed < room.timerSeconds && room.waitingForAnswers && room.isGameRunning) { delay(1000); elapsed++ }
    room.waitingForAnswers = false

    if (room.isGameRunning) {
        room.players.forEach { p ->
            if (p.lastAnswerIndex == correctIdx) {
                if (!room.isSuddenDeathActive) p.score += 1
                p.correctCount += 1
            } else if (p.lastAnswerIndex != -1) {
                p.wrongCount += 1
            }
        }

        val options = question["options"] as List<String>
        room.broadcast(GameMessage(MessageType.RESULT, "Server", "Σωστή απάντηση: ${options[correctIdx]}"))
        delay(4000)
        room.broadcast(GameMessage(MessageType.LEADERBOARD, "Server", ""))
        delay(1000)
    }
}
