const admin = require("firebase-admin");
const serviceAccount = require("../../serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

exports.sendNotification = async (deviceToken, payload) => {
  try {
    const res = await admin.messaging().sendToDevice(deviceToken, payload);
    console.log("Notification sent:", res);
    return res;
  } catch (err) {
    console.error("Notification failed:", err);
    throw err;
  }
};
