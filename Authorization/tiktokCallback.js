const passport = require('passport');
const { tikTokStrategy } = require('./tiktokAuth');
const admin = require('firebase-admin');
const axios = require('axios'); // You will need this to make HTTP requests to TikTok API

// Initialize Passport and the TikTok strategy
passport.use(tikTokStrategy);

// Ensure Firebase is initialized
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(require('./firebase_credentials.json')),
  });
  console.log('Firebase Admin SDK initialized');
}

const db = admin.firestore();

// Set ignoreUndefinedProperties globally
db.settings({ ignoreUndefinedProperties: true });

const fetchTikTokVideos = async (accessToken, openId) => {
  const url = 'https://open.tiktokapis.com/v2/video/list/';
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  };

  // Remove 'fields' from data and move it to the URL as query parameters
  const params = new URLSearchParams({
    fields: 'cover_image_url,id,title,video_description,duration,embed_link,like_count,comment_count,share_count,view_count,create_time',
  }).toString();

  const data = {
    open_id: openId,
    max_count: 20,
  };

  try {
    console.log('Making request to TikTok API with open_id:', openId);
    const response = await axios.post(`${url}?${params}`, data, { headers });

    if (response.data && response.data.data && response.data.data.videos) {
      console.log('Successfully fetched videos:', response.data.data.videos);
      return response.data.data.videos;
    } else if (response.data && response.data.error) {
      throw new Error(`TikTok API Error: ${response.data.error.message}`);
    } else {
      throw new Error('No videos found or unexpected response structure');
    }
  } catch (error) {
    console.error('Error fetching videos from TikTok:', error.response ? error.response.data : error.message);
    throw error;
  }
};




exports.tiktokCallback = (req, res, next) => {
  passport.authenticate('tiktok', { session: false }, async (err, user, info) => {
    if (err) {
      console.error('TikTok authentication error:', err);
      return res.status(500).send(`Authentication Error: ${err.message}`);
    }

    if (!user) {
      console.warn('TikTok authentication failed to return user');
      return res.status(400).send('Authentication Failed: No user data');
    }

    const uid = req.session.uid;
    console.log('Retrieved UID from session:', uid);

    if (!uid) {
      console.error('No UID found in session');
      return res.status(400).send('Session expired or invalid. Please try again.');
    }

    console.log('TikTok authentication successful for user:', user.profile.id);

    const userRef = db.collection('users').doc(uid);
    const tikTokRef = userRef
      .collection('SocialMediaPlatforms')
      .doc('TikTok')
      .collection('Accounts')
      .doc(user.profile.username);

    try {
      const tokens = {
        access_token: user.accessToken,
        refresh_token: user.refreshToken,
        open_id: user.profile.id,
        expires_in: user.profile.expires_in ?? null,
        refresh_expires_in: user.profile.refresh_expires_in ?? null,
        scope: user.profile.scope ?? null,
        token_type: 'Bearer',
      };

      // Remove any keys with `null` values
      Object.keys(tokens).forEach((key) => tokens[key] === null && delete tokens[key]);

      await tikTokRef.set(
        {
          tokens: tokens,
          profileImage: user.profile.profileImage,
          username: user.profile.username,
          displayName: user.profile.displayName,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      console.log('TikTok data successfully saved to Firestore');

      // *** New code to update the TikTok account count ***
      const accountsRef = userRef.collection('SocialMediaPlatforms').doc('TikTok').collection('Accounts');
      const accountCount = (await accountsRef.get()).size; // Get the number of accounts in the Accounts collection

      // Update the SocialMediaPlatforms -> TikTok document with account count
      await userRef.collection('SocialMediaPlatforms').doc('TikTok').set(
        {
          account_count: accountCount,
          updated_at: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      console.log(`Updated account count for TikTok: ${accountCount}`);

      // Fetch and store videos after successful authorization
      try {
        const videos = await fetchTikTokVideos(user.accessToken, user.profile.id);
        console.log(`Fetched ${videos.length} videos for user: ${user.profile.username}`);

        const videosRef = tikTokRef.collection('Videos');
        const batch = db.batch(); // Use a batch to commit multiple writes at once

        videos.forEach((video) => {
          const videoRef = videosRef.doc(video.id);
          batch.set(videoRef, {
            title: video.title,
            description: video.video_description,
            create_time: video.create_time,
            share_url: video.embed_link,
            thumbnail_url: video.cover_image_url,
            is_up: true, // Since we are fetching them now, assume the videos are "up"
            is_tracked: false, // Default tracking to false
          });
        });

        await batch.commit();
        console.log('Videos successfully saved to Firestore');
      } catch (videoError) {
        console.error('Error fetching or saving videos:', videoError);
      }

      res.redirect('https://ovrsee.app/dashboard'); // Replace with your actual success page URL
    } catch (error) {
      console.error('Error saving TikTok data to Firestore:', error);
      res.status(500).send('Error saving TikTok data to Firestore');
    }
  })(req, res, next);
};
