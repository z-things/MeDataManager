'use strict';
var util = require('util');
var _ = require('lodash');
var async = require('async');
var schedule = require('node-schedule');
var mongoose = require('mongoose');
var VirtualDevice = require('./../virtual-device').VirtualDevice;
var logger = require('../mlogger/mlogger.js');
var DEVICE_DATA_SCHEMA = {
    "uuid": mongoose.SchemaTypes.String,
    "userId": mongoose.SchemaTypes.String,
    "type": mongoose.SchemaTypes.String,
    "timestamp": mongoose.SchemaTypes.Date,
    "offset": mongoose.SchemaTypes.Number,
    "data": mongoose.SchemaTypes.Mixed
};
var OPERATION_SCHEMAS = {
    getData: {
        "type": "object",
        "properties": {
            "uuid": {"type": "string"},
            "dataType": {"type": "string"},
            "timestamp": {
                "type": "object",
                "properties": {
                    "$gte": {"type": "string"},
                    "$lt": {"type": "string"}
                }
            }
        }
    },
    putData: {
        "type": "object",
        "properties": {
            "uuid": {"type": "string"},
            "userId": {"type": "string"},
            "type": {"type": "string"},
            "timestamp": {"type": "string"},
            "offset": {"type": "integer"},
            "data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "value": {"type": [
                            "object",
                            "array",
                            "number",
                            "boolean",
                            "string",
                            "null"
                        ]}
                    },
                    "required": ["name", "value"]
                }
            }
        },
        "required": ["uuid", "userId", "type", "timestamp", "offset", "data"]
    }
};
var ONE_HOUR_MS = 1000 * 60 * 60;
var ONE_DAY_MS = ONE_HOUR_MS * 24;
var DEVICE_ITEMS = {
    "050608070001": {
        "dis_temp": {
            $avg: "$data.dis_temp"
        }
    },
    "040B08040004": {
        "power": {
            $avg: "$data.power"
        }
    }

};
var rebuild = function (dataType, data) {
    var date = new Date();
    var retDataArray = [];
    if ("dailyReport" === dataType) {
        var curHour = date.getHours();
        retDataArray = new Array(curHour + 1);
        if (util.isArray(data)) {
            data.forEach(function (item, index) {
                var data = new Date(item.timestamp);
                var hour = data.getHours();
                retDataArray[hour] = item;
            })
        }
    }
    else if ("monthlyReport" === dataType) {
        var curDay = date.getDate();
        retDataArray = new Array(curDay);
        if (util.isArray(data)) {
            data.forEach(function (item, index) {
                var data = new Date(item.timestamp);
                var day = data.getDate();
                retDataArray[day - 1] = item;
            })
        }
    }
    else if ("yearlyReport" === dataType) {
        var curMoth = date.getMonth();
        retDataArray = new Array(curMoth + 1);
        if (util.isArray(data)) {
            data.forEach(function (item, index) {
                var data = new Date(item.timestamp);
                var month = data.getMonth();
                retDataArray[month - 1] = item;
            })
        }
    }
    else if ("all" === dataType) {
        retDataArray = data;
    }
    return retDataArray;
};
var buildVirtualData = function (message, callback) {
    var retData = [];
    var curDate = new Date();
    if ("9f78e9e6-4a01-4fbf-8c4e-6009ddd3ffff" === message.uuid
        || "9f78e9e6-4a01-4fbf-8c4e-6009ddd3fccc" === message.uuid) {
        if ("dailyReport" === message.dataType) {
            var curHour = curDate.getHours();
            curDate.setHours(0, 0, 0, 0);
            for (var h = 0; h <= curHour; ++h) {
                curDate.setHours(h + 8, 0, 0, 0);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000004",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "currentPower": 1000 + Math.floor(Math.random() * 200),
                    "totalEnergy": Math.random() * 10
                });
            }

        }
        else if ("monthlyReport" === message.dataType) {
            var curDay = curDate.getDate();
            curDate.setHours(0, 0, 0, 0);
            curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
            curDate.setMonth(curDate.getMonth(), 1);
            for (var d = 1; d <= curDay; ++d) {
                curDate.setMonth(curDate.getMonth(), d);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000004",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "currentPower": 1000 + Math.floor(Math.random() * 200),
                    "totalEnergy": 14 + Math.random() * 10
                });
            }
        }
        else if ("yearlyReport" === message.dataType) {
            var curMoth = curDate.getMonth();
            curDate.setHours(0, 0, 0, 0);
            curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
            curDate.setMonth(0, 1);
            for (var m = 0; m <= curMoth; ++m) {
                curDate.setMonth(m, 1);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000004",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "currentPower": 1000 + Math.floor(Math.random() * 200),
                    "totalEnergy": 100 + Math.random() * 100
                });
            }
        }
        else {
            retData.push({
                "timestamp": "2017-01-01T00:00:00.974Z",
                "type": "040B01000004",
                "dataType": message.dataType,
                "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                "uuid": message.uuid,
                "timezone": 28800000,
                "analyzerId": "1",
                "currentPower": 1000 + Math.floor(Math.random() * 200),
                "totalEnergy": 1000 + Math.random() * 100
            });
        }
    }
    else {
        if ("dailyReport" === message.dataType) {
            var curHour = curDate.getHours();
            curDate.setHours(0, 0, 0, 0);
            for (var h = 0; h <= curHour; ++h) {
                curDate.setHours(h + 8, 0, 0, 0);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000005",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "pacToGrid": 1000 + Math.floor(Math.random() * 200),
                    "eDisChargeTotal": 567 + Math.random() * 100,
                    "eChargeTotal": 589 + Math.random() * 100,
                    "pacToUser": 1000 + Math.floor(Math.random() * 200),
                    "eToGridTotal": 1345 + Math.random() * 100
                });
            }

        }
        else if ("monthlyReport" === message.dataType) {
            var curDay = curDate.getDate();
            curDate.setHours(0, 0, 0, 0);
            curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
            curDate.setMonth(curDate.getMonth(), 1);
            for (var d = 1; d <= curDay; ++d) {
                curDate.setMonth(curDate.getMonth(), d);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000005",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "pacToGrid": 1000 + Math.floor(Math.random() * 200),
                    "eDisChargeTotal": 567 + Math.random() * 100,
                    "eChargeTotal": 589 + Math.random() * 100,
                    "pacToUser": 1000 + Math.floor(Math.random() * 200),
                    "eToGridTotal": 1345 + Math.random() * 100
                });
            }
        }
        else if ("yearlyReport" === message.dataType) {
            var curMoth = curDate.getMonth();
            curDate.setHours(0, 0, 0, 0);
            curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
            curDate.setMonth(0, 1);
            for (var m = 0; m <= curMoth; ++m) {
                curDate.setMonth(m, 1);
                retData.push({
                    "timestamp": curDate.toISOString(),
                    "type": "040B01000005",
                    "dataType": message.dataType,
                    "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                    "uuid": message.uuid,
                    "timezone": 28800000,
                    "analyzerId": "1",
                    "pacToGrid": 1000 + Math.floor(Math.random() * 200),
                    "eDisChargeTotal": 567 + Math.random() * 100,
                    "eChargeTotal": 589 + Math.random() * 100,
                    "pacToUser": 1000 + Math.floor(Math.random() * 200),
                    "eToGridTotal": 1345 + Math.random() * 100
                });
            }
        }
        else {
            retData.push({
                "timestamp": "2017-01-01T00:00:00.974Z",
                "type": "040B01000005",
                "dataType": message.dataType,
                "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
                "uuid": message.uuid,
                "timezone": 28800000,
                "analyzerId": "1",
                "pacToGrid": 1000 + Math.floor(Math.random() * 200),
                "eDisChargeTotal": 567 + Math.random() * 100,
                "eChargeTotal": 589 + Math.random() * 100,
                "pacToUser": 1000 + Math.floor(Math.random() * 200),
                "eToGridTotal": 1345 + Math.random() * 100
            });
        }
    }
    callback(retData);
};
/**
 * @constructor
 * */
function dataManager(conx, uuid, token, configurator) {
    this.db = null;
    this.dataModel = null;
    this.dailyDataModel = null;
    this.monthlyDataModel = null;
    this.yearlyDataModel = null;
    this.allDataModel = null;
    VirtualDevice.call(this, conx, uuid, token, configurator);
}
util.inherits(dataManager, VirtualDevice);

/**
 * 设备管理器初始化，将系统已经添加的设备实例化并挂载到Meshblu网络
 * */
dataManager.prototype.init = function () {
    var self = this;
    var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"));
    mongoose.Promise = global.Promise;
    db.once('error', function (error) {
        logger.error(200005, error);
    });

    db.once('open', function () {
        self.db = db;
        self.dataModel = self.db.model("data", DEVICE_DATA_SCHEMA);
        self.dailyDataModel = self.db.model("daily_data", DEVICE_DATA_SCHEMA);
        self.monthlyDataModel = self.db.model("monthly_data", DEVICE_DATA_SCHEMA);
        self.yearlyDataModel = self.db.model("yearly_data", DEVICE_DATA_SCHEMA);
        self.allDataModel = self.db.model("all_data", DEVICE_DATA_SCHEMA);
    });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~getData
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~getData} peerCallback: 远程RPC回调函数
 * */
dataManager.prototype.getData = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: []};
    self.messageValidate(message, OPERATION_SCHEMAS.getData, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            async.waterfall([
                    function (innerCallback) {
                        var msg = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "getDevice",
                                cmdCode: "0003",
                                parameters: {
                                    uuid: message.uuid
                                }
                            }
                        };
                        if (!util.isNullOrUndefined(message.userId)) {
                            msg.payload.parameters.userId = message.userId;
                        }
                        self.message(msg, function (response) {
                            if (response.retCode === 200) {
                                var deviceInfo = response.data;
                                if (util.isArray(response.data)) {
                                    deviceInfo = _.first(response.data);
                                }
                                innerCallback(null, deviceInfo);
                            } else {
                                innerCallback({errorId: response.retCode, errorMsg: response.description});
                            }
                        });
                    }
                ],
                function (error, device) {
                    if (error) {
                        responseMessage.retCode = error.errorId;
                        responseMessage.description = error.errorMsg;
                        peerCallback(responseMessage);
                    }
                    else{
                        var offset = 0;
                        if(!util.isNullOrUndefined(device.timeZone)
                            && !util.isNullOrUndefined(device.timeZone.offset)){
                            offset = parseInt(device.timeZone.offset);
                            var startTime = new Date(message.timestamp.$gte).getTime() - offset;
                            var endTime = new Date(message.timestamp.$lt).getTime() - offset;
                            message.timestamp.$gte = new Date(startTime).toISOString();
                            message.timestamp.$lt = new Date(endTime).toISOString();
                        }
                        var dataModel = null;
                        if ("dailyReport" === message.dataType) {
                            dataModel = self.dailyDataModel;
                        }
                        else if ("monthlyReport" === message.dataType) {
                            dataModel = self.monthlyDataModel;
                        }
                        else if ("yearlyReport" === message.dataType) {
                            dataModel = self.yearlyDataModel;
                        }
                        else if ("allReport" === message.dataType) {
                            dataModel = self.allDataModel;
                        }
                        if (util.isNullOrUndefined(dataModel)) {
                            responseMessage.retCode = 200001;
                            responseMessage.description = "invalid data type:" + message.dataType;
                            peerCallback(responseMessage);
                        }
                        else {

                            var conditions = {
                                uuid:message.uuid,
                                timestamp:{
                                    $gte:new Date(message.timestamp.$gte),
                                    $lt:new Date(message.timestamp.$lt)
                                }
                            };
                            if("allReport" === message.dataType){
                                delete conditions.timestamp;
                            }
                            dataModel.find(conditions).sort({timestamp: 1}).exec(function (error, results) {
                                if (error) {
                                    var logError = {errorId: 212000, errorMsg: JSON.stringify(error)};
                                    logger.error(212000, error);
                                    responseMessage.code = logError.errorId;
                                    responseMessage.message = logError.errorMsg;
                                }
                                else {
                                    _.forEach(results, function (dataItem) {
                                        var record = {
                                            uuid: dataItem.uuid,
                                            type: dataItem.type,
                                            dataType: message.dataType,
                                            timestamp: new Date(dataItem.timestamp).toISOString()
                                        };
                                        if(0 !== offset){
                                            var time = new Date(dataItem.timestamp).getTime();
                                            record.timestamp = new Date(time + offset).toISOString()
                                        }
                                        _.merge(record, dataItem.data);
                                        responseMessage.data.push(record);
                                    });
                                }
                                if (peerCallback && _.isFunction(peerCallback)) {
                                    peerCallback(responseMessage);
                                }
                            });
                        }
                    }
                });
        }
    });
};


/**
 * 远程RPC回调函数
 * @callback onMessage~putData
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 添加原始采集数据
 * @param {object} message:消息体
 * @param {onMessage~putData} peerCallback: 远程RPC回调函数
 * */
dataManager.prototype.putData = function (message, peerCallback) {
    var self = this;
    ///logger.debug("AnalyzerPlugin.putData: " + JSON.stringify(message));
    var responseMessage = {retCode: 200, description: "Success.", data: {}};
    self.messageValidate(message, OPERATION_SCHEMAS.putData, function (error) {
        if (error) {
            logger.debug(error);
            responseMessage = error;
            peerCallback(error);
        }
        else {
            async.waterfall([
                    function (innerCallback) {
                        var msg = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "getDevice",
                                cmdCode: "0003",
                                parameters: {
                                    uuid: message.uuid
                                }
                            }
                        };
                        if (!util.isNullOrUndefined(message.userId)) {
                            msg.payload.parameters.userId = message.userId;
                        }
                        self.message(msg, function (response) {
                            if (response.retCode === 200) {
                                var deviceInfo = response.data;
                                if (util.isArray(response.data)) {
                                    deviceInfo = _.first(response.data);
                                }
                                innerCallback(null, deviceInfo);
                            } else {
                                innerCallback({errorId: response.retCode, errorMsg: response.description});
                            }
                        });
                    },
                    function (device, innerCallback) {
                        var curData = {
                            uuid: message.uuid,
                            userId: message.userId,
                            type: message.type,
                            timestamp: new Date(message.timestamp),
                            offset: message.offset,
                            data: {}
                        };
                        _.forEach(message.data, function (item) {
                            curData.data[item.name] = parseFloat(item.value);
                        });
                        if (util.isNullOrUndefined(device.extra)) {
                            device.extra = {};
                        }
                        var preData = device.extra.preItems;
                        if (util.isNullOrUndefined(preData)) {
                            device.extra.preItems = curData.data;
                            curData = null;//如果没有缓存上次状态，则本次数据不入库
                        }
                        else {
                            preData = JSON.parse(JSON.stringify(preData));
                            device.extra.preItems = curData.data;//原始数据入库（上一次状态）
                            if ("040B08040004" === curData.type) {
                                curData.data["energyUsed"] -= preData["energyUsed"];
                                curData.data["energySaved"] -= preData["energySaved"];
                            }
                            else if ("040B01000001" === curData.type) {
                                curData.data["power"] = curData.data["sumEffectiveVolt"] * curData.data["sumEffectiveCurrent"] / 100;
                                if (17 === curData.data["direct"]) {
                                    curData.data["power"] = 0 - curData.data["power"];
                                }
                                curData.data["toGrid"] -= preData["toGrid"];
                                curData.data["toUser"] -= preData["toUser"];
                                delete curData.data.direct;
                                delete curData.data.sumEffectiveVolt;
                                delete curData.data.sumEffectiveCurrent;
                            }
                            else if ("040B08040004" === curData.type) {
                                curData.data["energyUsed"] -= preData.data["energyUsed"];
                                curData.data["energySaved"] -= preData.data["energySaved"];
                            }

                            else if ("040B08040005" === curData.type) {
                                curData.data["eDisChargeTotal"] -= preData.data["eDisChargeTotal"];
                                curData.data["eChargeTotal"] -= preData.data["eChargeTotal"];
                                curData.data["eToGridTotal"] -= preData.data["eToGridTotal"];
                            }

                        }
                        var msg = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "deviceUpdate",
                                cmdCode: "0004",
                                parameters: {
                                    "uuid": device.uuid,
                                    "extra.preItems": device.extra.preItems
                                }
                            }
                        };
                        self.message(msg, function (response) {
                            logger.debug(response);
                        });
                        innerCallback(null, curData);
                    },
                    function (data, innerCallback) {
                        if (!util.isNullOrUndefined(data)) {
                            var mongooseEntity = new self.dataModel(data);
                            mongooseEntity.save(function (error) {
                                if (error) {
                                    innerCallback({
                                        errorId:212000,
                                        errorMsg:error
                                    });
                                }
                                else{
                                    innerCallback(null);
                                }
                            });
                        }
                        else {
                            innerCallback(null);
                        }
                    }
                ],
                function (error) {
                    if (error) {
                        responseMessage.retCode = error.errorId;
                        responseMessage.description = error.errorMsg;
                    }
                    if (peerCallback && _.isFunction(peerCallback)) {
                        peerCallback(responseMessage);
                    }
                }
            );
        }
    });
};

module.exports = {
    Service: dataManager,
    OperationSchemas: OPERATION_SCHEMAS
};