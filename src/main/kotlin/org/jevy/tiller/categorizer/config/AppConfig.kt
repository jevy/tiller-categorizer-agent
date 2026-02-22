package org.jevy.tiller.categorizer.config

data class AppConfig(
    val kafkaBootstrapServers: String,
    val schemaRegistryUrl: String,
    val googleSheetId: String,
    val googleCredentialsJson: String,
    val anthropicApiKey: String,
    val maxTransactionAgeDays: Long,
    val maxTransactions: Int,
    val additionalContextPrompt: String?,
    val anthropicModel: String,
) {
    companion object {
        fun fromEnv(): AppConfig = AppConfig(
            kafkaBootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS"),
            schemaRegistryUrl = requireEnv("SCHEMA_REGISTRY_URL"),
            googleSheetId = requireEnv("GOOGLE_SHEET_ID"),
            googleCredentialsJson = requireEnv("GOOGLE_CREDENTIALS_JSON"),
            anthropicApiKey = System.getenv("ANTHROPIC_API_KEY") ?: "",
            maxTransactionAgeDays = System.getenv("MAX_TRANSACTION_AGE_DAYS")?.toLongOrNull() ?: 365L,
            maxTransactions = System.getenv("MAX_TRANSACTIONS")?.toIntOrNull() ?: 0,
            additionalContextPrompt = System.getenv("ADDITIONAL_CONTEXT_PROMPT")?.takeIf { it.isNotBlank() },
            anthropicModel = System.getenv("ANTHROPIC_MODEL") ?: "claude-sonnet-4-6",
        )

        private fun requireEnv(name: String): String =
            System.getenv(name) ?: throw IllegalStateException("Required environment variable $name is not set")
    }
}
