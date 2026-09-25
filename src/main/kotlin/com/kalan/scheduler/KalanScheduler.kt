package com.kalan.scheduler

import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.ZoneId
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class KalanScheduler(corePoolSize: Int = 1, threadFactory: ThreadFactory = DefaultKalanThreadFactory(), private val jobRepository: JobRepository = InMemoryJobRepository()) {
    private val scheduler = ScheduledThreadPoolExecutor(corePoolSize, threadFactory)
    private val activeJobs = ConcurrentHashMap<String, ScheduledFuture<*>>()
    private val running = AtomicBoolean(true)
    private val listeners = CopyOnWriteArrayList<JobEventListener>()
    private val currentGlobalExecutions = AtomicInteger(0)
    private val jobGroups = ConcurrentHashMap<String, MutableSet<String>>()
    
    private val priorityConcurrentCounts = ConcurrentHashMap<Int, AtomicInteger>()

    private val priorityQueue = PriorityBlockingQueue<Job>()
    private val workerExecutor = ThreadPoolExecutor(
        corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS, 
        LinkedBlockingQueue(), threadFactory
    )

    private val dispatcherThread: Thread

    var maxGlobalConcurrency: Int = Int.MAX_VALUE
    var errorHandler: (Throwable) -> Unit = { it.printStackTrace() }
    var maxPerPriorityConcurrency: Int = Int.MAX_VALUE

    init {
        dispatcherThread = Thread({
            while (running.get() || priorityQueue.isNotEmpty()) {
                try {
                    val job = priorityQueue.poll(500, TimeUnit.MILLISECONDS) ?: continue
                    val executor = job.customExecutor ?: workerExecutor
                    executor.execute { runJobInternal(job) }
                } catch (e: InterruptedException) {
                    if (!running.get()) break
                    Thread.currentThread().interrupt()
                    break
                }
            }
        }, "kalan-priority-dispatcher").apply { start() }
    }

    fun addEventListener(listener: JobEventListener) {
        listeners.add(listener)
    }

    fun removeEventListener(listener: JobEventListener) {
        listeners.remove(listener)
    }

    private fun notifyListeners(event: JobEvent) {
        listeners.forEach { it.onEvent(event) }
    }

    private fun wrapExecution(job: Job, action: () -> Unit): () -> Unit {
        return {
            val timeout = job.timeoutMs
            if (timeout == null) {
                try {
                    action()
                } catch (e: Throwable) {
                    handleFailure(job, e)
                }
            } else {
                val executor = job.customExecutor ?: workerExecutor
                val future = CompletableFuture.runAsync({ 
                    try {
                        action()
                    } catch (e: Throwable) {
                        throw e
                    }
                }, executor)
                
                try {
                    future.get(timeout, TimeUnit.MILLISECONDS)
                } catch (e: TimeoutException) {
                    future.cancel(true)
                    handleFailure(job, TimeoutException("Job ${job.id} timed out after $timeout ms"))
                } catch (e: ExecutionException) {
                    handleFailure(job, e.cause ?: e)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    handleFailure(job, e)
                } catch (e: Throwable) {
                    handleFailure(job, e)
                }
            }
        }
    }

    private fun handleFailure(job: Job, e: Throwable) {
        job.incrementFailure()
        notifyListeners(JobEvent(JobEvent.Type.FAILED, job.id, e))
        errorHandler(e)

        val policy = job.retryPolicy
        if (policy != null && job.getFailureCount() <= policy.maxRetries) {
            scheduler.schedule({
                try {
                    priorityQueue.put(job)
                } catch (retryEx: Throwable) {
                    handleFailure(job, retryEx)
                }
            }, policy.delayMs, TimeUnit.MILLISECONDS)
        }
    }

    private fun runJobInternal(job: Job) {
        val wrappedAction = wrapExecution(job) {
            notifyListeners(JobEvent(JobEvent.Type.STARTED, job.id))
            job.execute()
        }

        if (!isDependencySatisfied(job.id)) {
            scheduler.schedule({ priorityQueue.put(job) }, 100, TimeUnit.MILLISECONDS)
            return
        }

        if (currentGlobalExecutions.get() >= maxGlobalConcurrency) {
            scheduler.schedule({ priorityQueue.put(job) }, 100, TimeUnit.MILLISECONDS)
            return
        }

        val pCount = priorityConcurrentCounts.computeIfAbsent(job.priority.value) { AtomicInteger(0) }
        if (pCount.get() >= maxPerPriorityConcurrency) {
            scheduler.schedule({ priorityQueue.put(job) }, 100, TimeUnit.MILLISECONDS)
            return
        }

        if (!job.tryAcquireSlot()) {
            scheduler.schedule({ priorityQueue.put(job) }, 100, TimeUnit.MILLISECONDS)
            return
        }

        currentGlobalExecutions.incrementAndGet()
        pCount.incrementAndGet()
        try {
            wrappedAction()
        } finally {
            currentGlobalExecutions.decrementAndGet()
            pCount.decrementAndGet()
            job.releaseSlot()
            if (job.intervalMs == null && job.isCompleted()) {
                notifyListeners(JobEvent(JobEvent.Type.COMPLETED, job.id))
                activeJobs.remove(job.id)
            }
        }
    }

    fun schedule(id: String, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), dependsOn: String? = null, retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?) {
        if (!running.get()) return
        
        if (dependsOn != null && hasCircularDependency(id, dependsOn)) {
            throw IllegalArgumentException("Circular dependency detected for job $id depending on $dependsOn")
        }

        val job = Job(id, action, Instant.now().plusMillis(delayMs), priority = JobPriority(priority), timeoutMs = timeoutMs, tags = tags, metadata = metadata, dependsOn = dependsOn, retryPolicy = retryPolicy, concurrencyLimit = concurrencyLimit, executionStrategy = strategy, customExecutor = customExecutor)
        jobRepository.save(job)

        val future = scheduler.schedule({ 
            try {
                priorityQueue.put(job)
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, delayMs, TimeUnit.MILLISECONDS)
        
        activeJobs[id] = future
    }

    private fun hasCircularDependency(startJobId: String, dependsOnId: String): Boolean {
        var current = dependsOnId
        val visited = mutableSetOf<String>()
        visited.add(startJobId)
        
        while (current != null) {
            if (visited.contains(current)) return true
            visited.add(current)
            current = jobRepository.findById(current)?.dependsOn
        }
        return false
    }

    fun scheduleAsync(id: String, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), dependsOn: String? = null, retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?): CompletableFuture<Any?> {
        val resultFuture = CompletableFuture<Any?>()
        
        schedule(id, delayMs, priority, timeoutMs, tags, metadata, dependsOn, retryPolicy, concurrencyLimit, strategy, customExecutor) {
            try {
                val res = action(it)
                resultFuture.complete(res)
                res
            } catch (e: Throwable) {
                resultFuture.completeExceptionally(e)
                throw e
            }
        }
        
        return resultFuture
    }

    fun scheduleAt(id: String, startTime: Instant, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), dependsOn: String? = null, retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?) {
        val delay = Duration.between(Instant.now(), startTime).toMillis()
        schedule(id, if (delay < 0) 0 else delay, priority, timeoutMs, tags, metadata, dependsOn, retryPolicy, concurrencyLimit, strategy, customExecutor, action)
    }

    fun scheduleAtFixedRate(id: String, initialDelayMs: Long, periodMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), maxRepetitions: Int? = null, retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), periodMs, JobPriority(priority), timeoutMs, tags, metadata, maxRepetitions, retryPolicy = retryPolicy, concurrencyLimit = concurrencyLimit, executionStrategy = strategy, customExecutor = customExecutor)
        jobRepository.save(job)

        val wrappedAction = wrapExecution(job) { 
            notifyListeners(JobEvent(JobEvent.Type.STARTED, job.id))
            val result = job.execute()
            if (result is JobResult.MaxRepetitionsReached) {
                cancel(id)
            }
        }
        val future = scheduler.scheduleAtFixedRate({
            try {
                if (job.executionStrategy == JobExecutionStrategy.SKIP_IF_RUNNING && !job.tryAcquireSlot()) {
                    return@scheduleAtFixedRate
                }
                
                if (currentGlobalExecutions.get() >= maxGlobalConcurrency || !job.tryAcquireSlot()) return@scheduleAtFixedRate
                
                currentGlobalExecutions.incrementAndGet()
                try {
                    wrappedAction()
                } finally {
                    currentGlobalExecutions.decrementAndGet()
                    job.releaseSlot()
                }
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun scheduleWithFixedDelay(id: String, initialDelayMs: Long, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), maxRepetitions: Int? = null, retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), delayMs, JobPriority(priority), timeoutMs, tags, metadata, maxRepetitions, retryPolicy = retryPolicy, concurrencyLimit = concurrencyLimit, executionStrategy = strategy, customExecutor = customExecutor)
        jobRepository.save(job)

        val wrappedAction = wrapExecution(job) { 
            notifyListeners(JobEvent(JobEvent.Type.STARTED, job.id))
            val result = job.execute()
            if (result is JobResult.MaxRepetitionsReached) {
                cancel(id)
            }
        }
        val future = scheduler.scheduleWithFixedDelay({
            try {
                if (currentGlobalExecutions.get() >= maxGlobalConcurrency || !job.tryAcquireSlot()) return@scheduleWithFixedDelay
                
                currentGlobalExecutions.incrementAndGet()
                try {
                    wrappedAction()
                } finally {
                    currentGlobalExecutions.decrementAndGet()
                    job.releaseSlot()
                }
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, delayMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun scheduleCron(id: String, cronExpr: CronExpression, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), retryPolicy: RetryPolicy? = null, concurrencyLimit: Int? = null, strategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE, customExecutor: Executor? = null, action: (String) -> Any?) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now(), null, JobPriority(priority), timeoutMs, tags, metadata, retryPolicy = retryPolicy, concurrencyLimit = concurrencyLimit, executionStrategy = strategy, customExecutor = customExecutor)
        jobRepository.save(job)

        fun scheduleNext() {
            val nextRun = cronExpr.nextExecution(ZonedDateTime.now(ZoneId.systemDefault()))
            val delay = Duration.between(ZonedDateTime.now(ZoneId.systemDefault()), nextRun).toMillis()
            
            val future = scheduler.schedule({
                try {
                    priorityQueue.put(job)
                    scheduleNext()
                } catch (e: Throwable) {
                    errorHandler(e)
                }
            }, if (delay < 0) 0 else delay, TimeUnit.MILLISECONDS)
            
            activeJobs[id] = future
        }

        scheduleNext()
    }

    fun executeNow(id: String) {
        val job = jobRepository.findById(id) ?: throw IllegalArgumentException("Job not found: $id")
        priorityQueue.put(job)
    }

    fun runJobsParallel(ids: List<String>, timeout: Long, unit: TimeUnit): List<CompletableFuture<Any?>> {
        return ids.map { id ->
            val job = jobRepository.findById(id) ?: throw IllegalArgumentException("Job not found: $id")
            val executor = job.customExecutor ?: workerExecutor
            CompletableFuture.supplyAsync({
                job.execute().let { result ->
                    if (result is JobResult.Success) result.value else throw RuntimeException("Job $id failed with result $result")
                }
            }, executor)
        }
    }

    fun updateJob(id: String, newAction: (String) -> Any?) {
        jobRepository.findById(id)?.action = newAction
    }

    fun updateJobPriority(id: String, priority: Int) {
        jobRepository.findById(id)?.priority = JobPriority(priority)
    }

    fun updateJobTimeout(id: String, timeoutMs: Long?) {
        jobRepository.findById(id)?.timeoutMs = timeoutMs
    }

    fun pauseJob(id: String) {
        jobRepository.findById(id)?.setPaused(true)
    }

    fun resumeJob(id: String) {
        jobRepository.findById(id)?.setPaused(false)
    }

    fun pauseAllJobs() {
        jobRepository.findAll().forEach { pauseJob(it.id) }
    }

    fun resumeAllJobs() {
        jobRepository.findAll().forEach { resumeJob(it.id) }
    }

    fun cancel(id: String) {
        activeJobs.remove(id)?.cancel(false)
        jobRepository.remove(id)
        notifyListeners(JobEvent(JobEvent.Type.CANCELLED, id))
    }

    fun cancelAll() {
        activeJobs.keys.toList().forEach { cancel(it) }
    }

    fun cancelByTag(tag: String) {
        jobRepository.findByTag(tag)
            .map { it.id }
            .forEach { cancel(it) }
    }

    fun cancelByMetadata(key: String, value: Any) {
        jobRepository.findByMetadata(key, value)
            .map { it.id }
            .forEach { cancel(it) }
    }

    fun isJobActive(id: String): Boolean {
        val future = activeJobs[id]
        return future != null && !future.isDone
    }

    fun getJobStatus(id: String): JobStatus {
        val job = jobRepository.findById(id)
        if (job != null && job.isCompleted()) return JobStatus.COMPLETED
        
        val future = activeJobs[id]
        return when {
            future == null -> JobStatus.NOT_FOUND
            future.isCancelled -> JobStatus.CANCELLED
            future.isDone -> JobStatus.COMPLETED
            else -> JobStatus.RUNNING
        }
    }

    fun getExecutionCount(id: String): Int {
        return jobRepository.findById(id)?.getExecutionCount() ?: 0
    }

    fun getLastExecutionTime(id: String): Instant? {
        return jobRepository.findById(id)?.getLastExecutionTime()
    }

    fun getLastResult(id: String): Any? {
        return jobRepository.findById(id)?.getLastResult()
    }

    fun getJobInfo(id: String): JobInfo?
        = jobRepository.findById(id)?.let {
            JobInfo(id, getJobStatus(id), it.getExecutionCount(), it.getLastExecutionTime(), it.intervalMs, it.priority.value, it.tags, it.metadata, it.timeoutMs, it.getLastResult(), it.executionStrategy)
        }

    fun listAllJobs(): List<JobInfo> {
        return jobRepository.findAll().mapNotNull { getJobInfo(it.id) }
    }

    fun listJobsByTag(tag: String): List<JobInfo> {
        return jobRepository.findByTag(tag)
            .mapNotNull { getJobInfo(it.id) }
    }

    fun shutdown() {
        running.set(false)
        dispatcherThread.interrupt()
        scheduler.shutdownNow()
        workerExecutor.shutdownNow()
        jobRepository.clear()
        activeJobs.clear()
        jobGroups.clear()
        if (jobRepository is AsyncJobRepository) {
            jobRepository.shutdown()
        }
    }

    fun getActiveJobCount(): Int = activeJobs.size

    fun getTotalJobCount(): Int = jobRepository.findAll().size

    fun getActiveJobIds(): Set<String> = activeJobs.keys.toSet()

    fun scheduleJob(id: String, block: JobBuilder.() -> Unit) {
        val builder = JobBuilder(id).apply(block)
        val meta = builder.metadata.toMap()
        
        val jobId = id
        when {
            builder.cronExpression != null -> scheduleCron(jobId, builder.cronExpression!!, builder.priority.value, builder.timeoutMs, builder.tags, meta, builder.retryPolicy, builder.concurrencyLimit, builder.executionStrategy, builder.customExecutor, builder.action)
            builder.fixedRate != null -> scheduleAtFixedRate(jobId, builder.initialDelay, builder.fixedRate!!, builder.priority.value, builder.timeoutMs, builder.tags, meta, builder.maxRepetitions, builder.retryPolicy, builder.concurrencyLimit, builder.executionStrategy, builder.customExecutor, builder.action)
            builder.fixedDelay != null -> scheduleWithFixedDelay(jobId, builder.initialDelay, builder.fixedDelay!!, builder.priority.value, builder.timeoutMs, builder.tags, meta, builder.maxRepetitions, builder.retryPolicy, builder.concurrencyLimit, builder.executionStrategy, builder.customExecutor, builder.action)
            builder.atTime != null -> scheduleAt(jobId, builder.atTime!!, builder.priority.value, builder.timeoutMs, builder.tags, meta, builder.dependsOn, builder.retryPolicy, builder.concurrencyLimit, builder.executionStrategy, builder.customExecutor, builder.action)
            else -> schedule(jobId, builder.initialDelay, builder.priority.value, builder.timeoutMs, builder.tags, meta, builder.dependsOn, builder.retryPolicy, builder.concurrencyLimit, builder.executionStrategy, builder.customExecutor, builder.action)
        }
        
        builder.group?.let { addJobToGroup(it, jobId) }
    }

    fun isDependencySatisfied(jobId: String): Boolean {
        val job = jobRepository.findById(jobId) ?: return true
        val depId = job.dependsOn ?: return true
        return jobRepository.findById(depId)?.isCompleted() ?: false
    }

    fun addJobToGroup(groupId: String, jobId: String) {
        jobGroups.computeIfAbsent(groupId) { ConcurrentHashMap.newKeySet() }.add(jobId)
    }

    fun removeJobFromGroup(groupId: String, jobId: String) {
        jobGroups[groupId]?.remove(jobId)
    }

    fun pauseGroup(groupId: String) {
        jobGroups[groupId]?.forEach { pauseJob(it) }
    }

    fun resumeGroup(groupId: String) {
        jobGroups[groupId]?.forEach { resumeJob(it) }
    }

    fun cancelGroup(groupId: String) {
        jobGroups[groupId]?.forEach { cancel(it) }
        jobGroups.remove(groupId)
    }

    fun getJobsInGroup(groupId: String): List<JobInfo> {
        return jobGroups[groupId]?.mapNotNull { getJobInfo(it) } ?: emptyList()
    }

    fun getHealthStatus(): SchedulerHealth {
        return SchedulerHealth(
            isRunning = running.get(),
            activeJobCount = activeJobs.size,
            queuedJobCount = priorityQueue.size,
            workerPoolActiveThreads = workerExecutor.activeCount,
            workerPoolQueueSize = workerExecutor.queue.size
        )
    }
}

class JobBuilder(val id: String) {
    var initialDelay: Long = 0
    var fixedRate: Long? = null
    var fixedDelay: Long? = null
    var atTime: Instant? = null
    var cronExpression: CronExpression? = null
    var priority: JobPriority = JobPriority.NORMAL
    var timeoutMs: Long? = null
    var tags: Set<String> = emptySet()
    var metadata: MutableMap<String, Any> = mutableMapOf()
    var maxRepetitions: Int? = null
    var dependsOn: String? = null
    var retryPolicy: RetryPolicy? = null
    var concurrencyLimit: Int? = null
    var executionStrategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE
    var customExecutor: Executor? = null
    var group: String? = null
    lateinit var action: (String) -> Any?

    fun execute(block: (String) -> Any?) {
        this.action = block
    }

    fun every(ms: Long) {
        this.fixedRate = ms
    }

    fun everyHour() {
        this.fixedRate = 3600000L
    }

    fun everyDay() {
        this.fixedRate = 86400000L
    }

    fun withDelay(ms: Long) {
        this.fixedDelay = ms
    }

    fun at(time: Instant) {
        this.atTime = time
    }

    fun cron(expr: CronExpression) {
        this.cronExpression = expr
    }

    fun startAfter(ms: Long) {
        this.initialDelay = ms
    }

    fun withPriority(priority: Int) {
        this.priority = JobPriority(priority)
    }

    fun withPriority(priority: JobPriority) {
        this.priority = priority
    }

    fun withTimeout(ms: Long) {
        this.timeoutMs = ms
    }

    fun withTags(vararg tags: String) {
        this.tags = tags.toSet()
    }

    fun withMetadata(metadata: Map<String, Any>) {
        this.metadata.putAll(metadata)
    }

    fun addMetadata(key: String, value: Any) {
        this.metadata[key] = value
    }

    fun repeatAtMost(times: Int) {
        this.maxRepetitions = times
    }

    fun dependsOn(jobId: String) {
        this.dependsOn = jobId
    }

    fun withRetryPolicy(maxRetries: Int, delayMs: Long) {
        this.retryPolicy = RetryPolicy(maxRetries, delayMs)
    }

    fun retry(max: Int, every: Long) {
        withRetryPolicy(max, every)
    }

    fun withConcurrencyLimit(limit: Int) {
        this.concurrencyLimit = limit
    }

    fun limitConcurrency(limit: Int) {
        withConcurrencyLimit(limit)
    }

    fun withExecutionStrategy(strategy: JobExecutionStrategy) {
        this.executionStrategy = strategy
    }

    fun withCustomExecutor(executor: Executor) {
        this.customExecutor = executor
    }

    fun inGroup(groupId: String) {
        this.group = groupId
    }
}

enum class JobStatus {
    RUNNING, COMPLETED, CANCELLED, NOT_FOUND
}

data class JobInfo(
    val id: String,
    val status: JobStatus,
    val executionCount: Int,
    val lastExecutionTime: Instant?,
    val intervalMs: Long?,
    val priority: Int,
    val tags: Set<String>,
    val metadata: Map<String, Any>,
    val timeoutMs: Long?,
    val lastResult: Any?,
    val executionStrategy: JobExecutionStrategy
)

data class SchedulerHealth(
    val isRunning: Boolean,
    val activeJobCount: Int,
    val queuedJobCount: Int,
    val workerPoolActiveThreads: Int,
    val workerPoolQueueSize: Int
)

class DefaultKalanThreadFactory : ThreadFactory {
    private val counter = java.util.concurrent.atomic.AtomicInteger(0)
    override fun newThread(r: Runnable): Thread {
        return Thread(r, "kalan-scheduler-worker-${counter.getAndIncrement()}")
    }
}

data class JobEvent(
    val type: Type,
    val jobId: String,
    val throwable: Throwable? = null
) {
    enum class Type {
        STARTED, COMPLETED, FAILED, CANCELLED
    }
}

interface JobEventListener {
    fun onEvent(event: JobEvent)
}

data class CronExpression(
    val minute: Int = -1, // -1 for any
    val hour: Int = -1,
    val dayOfMonth: Int = -1,
    val month: Int = -1,
    val dayOfWeek: Int = -1
) {
    fun nextExecution(now: ZonedDateTime): ZonedDateTime {
        var next = now.plusSeconds(1)
        while (true) {
            if (matches(next)) return next
            if (next.hour == 23 && next.minute == 59 && next.second == 59) {
                next = next.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
            } else {
                next = next.plusSeconds(1)
            }
            if (next.year > now.year + 1) throw IllegalArgumentException("No matching execution time found within one year")
        }
    }

    private fun matches(dt: ZonedDateTime): Boolean {
        if (minute != -1 && dt.minute != minute) return false
        if (hour != -1 && dt.hour != hour) return false
        if (dayOfMonth != -1 && dt.dayOfMonth != dayOfMonth) return false
        if (month != -1 && dt.monthValue != month) return false
        if (dayOfWeek != -1 && dt.dayOfWeek.value != dayOfWeek) return false
        return true
    }
}