 Task :compileKotlin FAILED
#14 40.87 e: file:///home/gradle/src/server.kt:132:13 Unresolved reference: launch
#14 40.87 e: file:///home/gradle/src/server.kt:132:22 Suspension functions can be called only within coroutine body
#14 40.87 e: file:///home/gradle/src/server.kt:133:13 Unresolved reference: launch
#14 40.87 e: file:///home/gradle/src/server.kt:133:22 Suspension functions can be called only within coroutine body
#14 40.87 e: file:///home/gradle/src/server.kt:134:13 Unresolved reference: launch
#14 40.87 e: file:///home/gradle/src/server.kt:134:22 Suspension functions can be called only within coroutine body
#14 40.87 
#14 40.87 FAILURE: Build failed with an exception.
#14 40.87 
#14 40.87 * What went wrong:
#14 40.87 Execution failed for task ':compileKotlin'.
#14 40.87 > A failure occurred while executing org.jetbrains.kotlin.compilerRunner.GradleCompilerRunnerWithWorkers$GradleKotlinCompilerWorkAction
#14 40.87    > Compilation error. See log for more details
#14 40.87 
#14 40.87 * Try:
#14 40.87 > Run with --stacktrace option to get the stack trace.
#14 40.87 > Run with --info or --debug option to get more log output.
#14 40.87 > Run with --scan to get full insights.
#14 40.87 > Get more help at https://help.gradle.org.
#14 40.87 
#14 40.87 BUILD FAILED in 40s
#14 40.87 1 actionable task: 1 executed
#14 ERROR: process "/bin/sh -c gradle shadowJar --no-daemon" did not complete successfully: exit code: 1
------
 > importing cache manifest
------
------
 > [build 4/4] RUN gradle shadowJar --no-daemon:
40.87    > Compilation error. See log for more details
40.87 
40.87 * Try:
40.87 > Run with --stacktrace option to get the stack trace.
40.87 > Run with --info or --debug option to get more log output.
40.87 > Run with --scan to get full insights.
40.87 > Get more help at https://help.gradle.org.
40.87 
40.87 BUILD FAILED in 40s
40.87 1 actionable task: 1 executed
------
Dockerfile:5
--------------------
   3 |     COPY --chown=gradle:gradle . /home/gradle/src
   4 |     WORKDIR /home/gradle/src
   5 | >>> RUN gradle shadowJar --no-daemon
   6 |     
   7 |     # Stage 2: Run
--------------------
error: failed to solve: process "/bin/sh -c gradle shadowJar --no-daemon" did not complete successfully: exit code: 1
