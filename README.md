# Kalan Scheduler

Kalan Scheduler is a lightweight, type-safe job scheduling library for Kotlin. It provides a simple API to run tasks after a delay or at a fixed rate without needing a full-blown spring framework or quartz setup.

## Features
- Simple delayed task execution
- Fixed-rate periodic tasks
- Thread-safe job management
- Minimal dependency footprint
- Execution tracking with unique run IDs

## Usage
```kotlin
val scheduler = KalanScheduler()

scheduler.schedule("greeting", 1000) { executionId ->
    println("Execution $executionId: Hello after 1 second!")
}

scheduler.scheduleAtFixedRate("heartbeat", 0, 5000) { executionId ->
    println("Execution $executionId: Heartbeat...")
}
```