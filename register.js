var aiota = require("aiota-utils");
var path = require("path");
var amqp = require("amqp");
var jsonValidate = require("jsonschema").validate;
var MongoClient = require("mongodb").MongoClient;

var config = null;

function validate(instance, schema)
{
	var v = jsonValidate(instance, schema);

	return (v.errors.length == 0 ? { isValid: true } : { isValid: false, error: v.errors });
}

function createDeviceResponse(db, header, payload, callback)
{
	db.collection("actions", function(err, collection) {
		if (err) {
			callback({ error: err });
			return;
		}

		var now = Date.now();
		var timeout = now + 86400000;
		var maxResends = 3;
		var resendTimeout = 10000;
		var resendAfter = now + resendTimeout;
		
		var obj = { deviceId: header.deviceId, encryption: header.encryption, requestId: header.requestId, action: "response", params: payload, status: 0, createdAt: now, timeoutAt: timeout, progress: [ { timestamp: now, status: "created" } ], resends: { numResends: 0, maxResends: maxResends, resendAfter: resendAfter, resendTimeout: resendTimeout } };
		
		collection.insert(obj, function(err, result) {
			if (err) {
				reply = { error: err, errorCode: 200003 };
			}
			else {
				reply = payload;
			}
			
			callback(reply);
		});
	});
}

function handleRegister(db, msg, app, callback)
{
	var schema = { 
		type: "object",
		properties: {
			name: { type: "string", required: true },
			product: { type: "string", required: true }
		}
	};
		
	var v = validate(msg.body, schema);

	if (v.isValid) {
		if (msg["body"].name == "") {
			callback({ error: "Invalid device name!", errorCode: 100019 });
			return;
		}
		
		if (msg["body"].product == "") {
			callback({ error: "Invalid product name!", errorCode: 100020 });
			return;
		}
		
		db.collection("devices", function(err, collection) {
			if (err) {
				callback({ error: err });
				return;
			}

			var verificationCode = Math.floor(Math.random() * 100000);

			var set = { deviceName: msg.body.name, productName: msg.body.product };
			set["apps." + msg.header.encryption.tokencardId] = { name: app.name, version: app.version, status: "pending", session: { id: msg.header.requestId + ":" + verificationCode, timeoutAt: 0 }, nonce: Math.floor(Math.random() * 4294967296), lastRequest: Date.now() };

			collection.update({ _id: msg.header.deviceId }, { $set: set }, { upsert: true }, function(err, result) {
				if (err) {
					callback({ error: err, errorCode: 200004 });
					return;
				}
				
				callback({ responseType: "verify", verificationCode: verificationCode });
			});
		});
	}
	else {
		callback({ error: v.error, errorCode: 100003 });
	}
}

function handleUnregister(db, msg, callback)
{
	db.collection("devices", function(err, collection) {
		if (err) {
			callback({ error: err, errorCode: 200001 });
			return;
		}

		var unset = {};
		unset["apps." + msg.header.encryption.tokencardId] = 1;

		collection.update({ _id: msg.header.deviceId }, { $unset: unset }, function(err, result) {
			if (err) {
				callback({ error: err, errorCode: 200004 });
				return;
			}
			
			callback({ status: "OK" });
		});
	});
}

function handleVerify(db, msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			requestId: { type: "string", required: true },
			verificationCode: { 
				type: "array",
				items: { type: "integer", minimum: 0 },
				minItems: 2,
				maxItems: 2,
				required: true
			}
		}
	};
		
	var v = validate(msg.body, schema);

	if (v.isValid) {
		if (msg.body.verificationCode[1] == msg.body.verificationCode[0] + 1) {
			db.collection("devices", function(err, collection) {
				if (err) {
					callback({ error: err, errorCode: 200001 });
					return;
				}
				
				collection.findOne({ _id: msg.header.deviceId }, { apps: 1 }, function(err, device) {
					if (err) {
						callback({ error: err, errorCode: 200002 });
						return;
					}
	
					if (device && device.hasOwnProperty("apps")) {
						if (device.apps.hasOwnProperty(msg.header.encryption.tokencardId)) {
							var app = device.apps[msg.header.encryption.tokencardId];
							
							schema = {
								type: "object",
								properties: {
									name: { type: "string", required: true },
									version: { 
										type: "object", 
										properties: {
											major: { type: "integer", required: true },
											minor: { type: "integer", required: true }
										},
										required: true
									},
									status: { type: "string", enum: [ "pending" ], required: true },
									session: {
										type: "object",
										properties: {
											id: { type: "string", required: true },
											timeoutAt: { type: "integer", minimum: 0, required: true }
										},
										required: true
									},
									lastRequest: { type: "integer", minimum: 0, required: true },
								}
							};
			
							v = validate(app, schema);
			
							if (v.isValid) {
								if ((app.session.id == msg.body.requestId + ":" + msg.body.verificationCode[0])) {
									var set = {};
									
									set["apps." + msg.header.encryption.tokencardId + ".status"] = "registered";
									set["apps." + msg.header.encryption.tokencardId + ".session"] = { id: "", timeoutAt: 0 };
						
									collection.update({ _id: msg.header.deviceId }, { $set: set }, function(err, result) {
										if (err) {
											callback({ error: err, errorCode: 200004 });
											return;
										}
										
										db.collection("actions", function(err, collection) {
											if (err) {
												callback({ error: err, errorCode: 200001 });
												return;
											}
											
											collection.update({ deviceId: msg.header.deviceId, "encryption.tokencardId": msg.header.encryption.tokencardId, requestId: msg.body.requestId }, { $push: { progress: { timestamp: Date.now(), status: "received verification code" } } }, function(err, result) {
												callback(err ? { error: err, errorCode: 200004 } : { status: "OK" });
											});
										});
									});
								}
								else {
									callback({ error: "The application status is not pending with this request id and verification code.", errorCode: 100031 });
								}
							}
							else {
								callback({ error: v.error, errorCode: 100003 });
							}
						}
						else {
							callback({ error: "The application has not been registered on this device.", errorCode: 100012 });
						}
					}
					else {
						callback({ error: "The device does not exist.", errorCode: 100011 });
					}
				});			
			});
		}
		else {
			callback({ error: "Invalid verification code", errorCode: 100030 });
		}
	}
	else {
		callback({ error: v.error, errorCode: 100003 });
	}
}

function handleRegisterRequest(db, msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			header: {
				type: "object",
				properties: {
					requestId: { type: "string", required: true },
					deviceId: { type: "string", required: true },
					type: { type: "string", enum: [ "register", "unregister", "verify" ], required: true },
					timestamp: { type: "integer", minimum: 0, required: true },
					ttl: { type: "integer", minimum: 0, required: true },
					encryption: {
						type: "object",
						properties: {
							method: { type: "string", required: true },
							tokencardId: { type: "string", required: true }
						},
						required: true
					}
				},
				required: true
			},
			body: {
				type: "object",
				required: true
			}
		}
	};

	var v = validate(msg, schema);

	if (v.isValid) {
		// Get the application definition
		db.collection("applications", function(err, collection) {
			if (err) {
				callback({ error: err, errorCode: 200001 });
				return;
			}
			
			collection.findOne({ _id: msg.header.encryption.tokencardId }, { _id: 0, name: 1, version: 1 }, function(err, app) {
				if (err) {
					callback({ error: err, errorCode: 200002 });
					return;
				}
				else {
					if (app) {
						schema = { 
							type: "object",
							properties: {
								name: { type: "string", required: true },
								version: { 
									type: "object",
									properties: {
										major: { type: "integer", required: true },
										minor: { type: "integer", required: true }
									},
									required: true
								}
							}
						};
	
						v = validate(app, schema);
					
						if (v.isValid) {
							// Check that the message has not expired
							var now = Date.now();
							
							if ((msg.header.ttl > 0) && (now > (msg.header.timestamp + msg.header.ttl * 1000))) {
								createDeviceResponse(db, msg.header, { error: "This message has expired.", errorCode: 100017 }, function(reply) {
									callback(reply);
								});
							}
							else {
								switch(msg.header.type) {
								case "register":	handleRegister(db, msg, app, function(reply) {
														createDeviceResponse(db, msg.header, reply, function(reply) {
															callback(reply);
														});
													});
													break;
								case "unregister":	handleUnregister(db, msg, function(reply) {
														if (reply.hasOwnProperty("error")) {
															createDeviceResponse(db, msg.header, reply, function(reply) {
																callback(reply);
															});
														}
														else {
															callback(reply);
														}
													});
													break;
								case "verify":		handleVerify(db, msg, function(reply) {
														if (reply.hasOwnProperty("error")) {
															createDeviceResponse(db, msg.header, reply, function(reply) {
																callback(reply);
															});
														}
														else {
															callback(reply);
														}
													});
													break;
								default:			createDeviceResponse(db, msg.header, { error: "The message type parameter ('system/" + msg.header.type + "') is not valid.", errorCode: 100018 }, function(reply) {
														callback(reply);
													});
								}
							}
						}
						else {
							callback({ error: v.error, errorCode: 100003 });
						}
					}
					else {
						callback({ error: "The application does not exist.", errorCode: 100016 });
					}
				}
			});
		});
	}
	else {
		callback({ error: v.error, errorCode: 100003 });
	}
}

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, aiotaDB) {
	if (err) {
		aiota.log(path.basename(__filename), "", null, err);
	}
	else {
		aiota.getConfig(aiotaDB, function(c) {
			if (c == null) {
				aiota.log(path.basename(__filename), "", aiotaDB, "Error getting config from database");
			}
			else {
				config = c;

				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, db) {
					if (err) {
						aiota.log(path.basename(__filename), config.server, aiotaDB, err);
					}
					else {
						var bus = amqp.createConnection(config.amqp);
						
						bus.on("ready", function() {
							var cl = { group: "system", type: "register" };
							bus.queue(aiota.getQueue(cl), { autoDelete: false, durable: true }, function(queue) {
								queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
									handleRegisterRequest(db, msg, function(result) {
										queue.shift();
									});
								});
							});
						});
		
						setInterval(function() { aiota.heartbeat(path.basename(__filename), config.server, aiotaDB); }, 10000);
		
						process.on("SIGTERM", function() {
							aiota.terminateProcess(path.basename(__filename), config.server, aiotaDB, function() {
								process.exit(1);
							});
						});
					}
				});
			}
		});
	}
});