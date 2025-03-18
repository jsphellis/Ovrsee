const fs = require('fs');
const yaml = require('js-yaml');
const passport = require('passport');
const CustomTikTokStrategy = require('./customTikTokStrategy');

try {
  const env = yaml.load(fs.readFileSync('env.yaml', 'utf8'));
  process.env.TIKTOK_CLIENT_KEY = env.TIKTOK_CLIENT_KEY;
  process.env.TIKTOK_CLIENT_SECRET = env.TIKTOK_CLIENT_SECRET;

  console.log('TikTok Client Key:', process.env.TIKTOK_CLIENT_KEY);
  console.log('TikTok Client Secret:', process.env.TIKTOK_CLIENT_SECRET);
} catch (e) {
  console.error('Error loading env.yaml:', e);
}

const tikTokStrategy = new CustomTikTokStrategy({
  clientID: process.env.TIKTOK_CLIENT_KEY,
  clientSecret: process.env.TIKTOK_CLIENT_SECRET,
  callbackURL: 'https://us-central1-ovrseeredux.cloudfunctions.net/tiktokAuth/callback', 
  scope: ['user.info.basic', 'user.info.profile', 'user.info.stats', 'video.list'],
  version: 'v2',
  authorizationParams: {
    force_login: '1'
  }
},
function(accessToken, refreshToken, profile, done) {
  done(null, { accessToken, refreshToken, profile });
});

tikTokStrategy._oauth2.getOAuthAccessToken = function(code, params, callback) {
  console.log('Exchanging code for access token...');

  this._request("POST", this._getAccessTokenUrl(), {
    "Content-Type": "application/x-www-form-urlencoded"
  }, this._encodeData({
    ...params,
    client_id: this._clientId,
    client_secret: this._clientSecret,
    code,
    redirect_uri: this._callbackURL
  }), null, (err, data) => {
    if (err) {
      console.error("Failed to obtain access token", err);
      callback(err);
    } else {
      try {
        const parsedData = JSON.parse(data);
        console.log('Access Token:', parsedData.access_token);
        console.log('Refresh Token:', parsedData.refresh_token);
        callback(null, parsedData.access_token, parsedData.refresh_token, parsedData);
      } catch (e) {
        console.error('Failed to parse token response:', e);
        callback(e);
      }
    }
  });
};

module.exports = { tikTokStrategy };
