package org.jevy.bookkeeper.writer

import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CategoryWriterTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "anthropic/claude-sonnet-4-6",
    )

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added",
    )

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private val sheetsClient = mockk<SheetsClient>(relaxed = true).also {
        every { it.readAllRows("Transactions!1:1") } returns listOf(header)
    }
    private val writer = CategoryWriter(config, sheetsClient)

    @Test
    fun `findRow returns correct row by scanning`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any), // header (row 1)
            listOf("txn-100" as Any),        // row 2
            listOf("txn-200" as Any),        // row 3
            listOf("txn-300" as Any),        // row 4
        )

        val row = writer.findRow("txn-300")

        assertEquals(4, row)
    }

    @Test
    fun `findRow returns null when transaction not found`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )

        val row = writer.findRow("txn-missing")

        assertNull(row)
    }

    @Test
    fun `writeCategory throws RowNotFoundException when row not found`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-missing")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )

        assertThrows<RowNotFoundException> { writer.writeCategory(tx) }
    }

    @Test
    fun `writeCategory skips when row already has category`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-100")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("Existing Category" as Any))

        writer.writeCategory(tx)

        verify(exactly = 0) { sheetsClient.writeCell(any(), any()) }
    }

    @Test
    fun `writeCategory writes category and date when row is empty`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-100")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("" as Any))

        writer.writeCategory(tx)

        verify { sheetsClient.writeCell("Transactions!C2", "Groceries") }
        verify { sheetsClient.writeCell(match { it.startsWith("Transactions!P2") }, any()) }
    }

    @Test
    fun `run sends to DLQ and tombstones uncategorized when row not found`() {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val dlqProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)

        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createProducer(any()) } returns dlqProducer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer

        val tx = Transaction.newBuilder()
            .setTransactionId("txn-missing")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        val tp = TopicPartition(TopicNames.CATEGORIZED, 0)
        val record = ConsumerRecord(TopicNames.CATEGORIZED, 0, 0L, "txn-missing", tx)
        val records = ConsumerRecords(mapOf(tp to listOf(record)))

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else throw InterruptedException("stop")
        }

        // Row not found — findRow returns null
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )

        val writer = CategoryWriter(config, sheetsClient)

        try {
            writer.run()
        } catch (_: InterruptedException) {}

        // Sent to write-failed DLQ
        verify { dlqProducer.send(match { it.topic() == TopicNames.WRITE_FAILED && it.key() == "txn-missing" }) }
        // Tombstoned from uncategorized to break the loop
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-missing" && it.value() == null }) }
        // Batch still committed
        verify(atLeast = 1) { consumer.commitSync() }
    }
}
