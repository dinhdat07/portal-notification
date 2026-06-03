package config

type FirebaseConfig struct {
	CredentialsFile string
}

func LoadFirebaseConfig() FirebaseConfig {
	return FirebaseConfig{
		CredentialsFile: getEnv("FIREBASE_CREDENTIALS_FILE", ""),
	}
}
