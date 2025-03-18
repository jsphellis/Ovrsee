const express = require('express');
const session = require('express-session');
const { tiktokCallback } = require('./tiktokCallback');
const { tikTokStrategy } = require('./tiktokAuth');
const passport = require('passport');
const crypto = require('crypto');
const dotenv = require('dotenv');

// Load environment variables from env.yaml
dotenv.config();

const app = express();

// Set up session middleware
app.use(session({
  secret: process.env.SESSION_SECRET, // Replace with a strong session secret key
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false } // Set to true if using HTTPS
}));

// Initialize Passport and the TikTok strategy
passport.use(tikTokStrategy);
app.use(passport.initialize());

app.get('/', (req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  res.set('Surrogate-Control', 'no-store');

  const uid = req.query.uid;
  const key = req.query.key;

  if (!uid) {
    return res.status(400).send('Missing UID');
  }

  if (!key || key !== process.env.SECURE_KEY) {
    return res.status(401).send('Unauthorized request');
  }

  req.session.uid = uid;

  // Redirect to TikTok OAuth authorization URL
  passport.authenticate('tiktok', {
    session: false,
    scope: ['user.info.basic', 'user.info.profile', 'user.info.stats', 'video.list'],
  })(req, res);
});

// TikTok callback route
app.get('/callback', tiktokCallback);

exports.tiktokAuth = app;
