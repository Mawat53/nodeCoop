// server.js
const express = require('express');
const db = require('./src/config/database');
const memberRoutes = require('./src/routes/memberRoutes');
const shareRoutes = require('./src/routes/shareRoutes');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

// Initialize database pool
db.initialize();

app.use(express.json());

// Routes
app.use('/api', memberRoutes, shareRoutes);

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});

process.on('SIGINT', () => {
    db.close().then(() => {
        process.exit();
    });
});