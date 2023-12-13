const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const app = express();
const port = 3000;

// Path to your database file
const dbPath = 'MySpotifyData.db';
app.use(express.static('public'));

// Open the SQLite database
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error(err.message);
    return;
  }
  console.log('Connected to the SQLite database.');
});

// API endpoint to fetch a track by track_id
app.get('/api/tracks/:track_name', (req, res) => {
    const trackId = req.params.track_name;
    db.get("SELECT * FROM tracks WHERE track_name = ?", [trackId], (err, row) => {
        if (err) {
            res.status(500).send(err.message);
            return;
        }
        if (row) {
            res.json(row);
        } else {
            res.status(404).send('Track not found');
        }
    });
});

// Start the server
app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});

// Handle closing the server
process.on('SIGINT', () => {
    db.close();
    process.exit();
});
