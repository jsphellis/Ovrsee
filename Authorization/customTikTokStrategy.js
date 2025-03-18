const TikTokStrategy = require('passport-tiktok-auth').Strategy;

class CustomTikTokStrategy extends TikTokStrategy {
  constructor(options, verify) {
    super(options, verify);
    this._authorizationParams = options.authorizationParams || {};
  }

  authorizationParams(options) {
    return {
      ...super.authorizationParams(options),
      ...this._authorizationParams
    };
  }
}

module.exports = CustomTikTokStrategy;
