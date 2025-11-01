module.exports = {
  apps: [
    {
      name: 'log-service',
      script: './build/log-service',
      interpreter: 'none',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '5G',
      env: {
        PORT: 9010,
        BIND_ADDR: '0.0.0.0',
        DB_PATH: './logs.db',
        FLUSH_LINES: 500,
        FLUSH_MS: 2000,
        MAX_QUEUE: 100000,
        MAX_RETRIES: 3,
        CRM_BOT_TOKEN: '8406722018:AAE76vUtNg_xaCcNbAP45WImEgJUxIJsPUY',
        CRM_BOT_CHAT_IDS: '483779758,7938128354',
      },
    },
  ],
};
