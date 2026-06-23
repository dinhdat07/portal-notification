package config

type TelegramConfig struct {
	BotToken string
	APIURL   string
}

func LoadTelegramConfig() TelegramConfig {
	return TelegramConfig{
		BotToken: getEnv("TELEGRAM_BOT_TOKEN", ""),
		APIURL:   getEnv("TELEGRAM_API_URL", "https://api.telegram.org"),
	}
}
