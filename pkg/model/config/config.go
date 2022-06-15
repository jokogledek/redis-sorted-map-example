package config

type Config struct {
	Redis    RedisConfig    `yaml:"redis"`
	Database DatabaseConfig `yaml:"database"`
	Files    FileConfig     `yaml:"files"`
}

type FileConfig struct {
	Csv string `yaml:"csv"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Key      string `yaml:"key"`
	Buffer   int    `yaml:"buffer"`
	MaxQueue int    `yaml:"max_queue"`
}

type DatabaseConfig struct {
	Username string `yaml:"username"`
	Pass     string `yaml:"pass"`
	DBName   string `yaml:"dbname"`
	Host     string `yaml:"host"`
}
