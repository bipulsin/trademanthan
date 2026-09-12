module.exports = {
  apps: [
    {
      name: 'divtest',
      script: 'server.js',
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        DIVTEST_PORT: 3847,
      },
    },
  ],
};
