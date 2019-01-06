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
var DEVICE_ITEMS_SCHEMAS = {
  "050608070001": {
    "type": "object",
    "properties": {
      "dis_temp": {"type": "number"}
    },
    "required": ["dis_temp"]
  },
  "040B08040004": {
    "type": "object",
    "properties": {
      "power": {"type": "number"},
      "energyUsed": {"type": "number"},
      "energySaved": {"type": "number"}
    },
    "required": ["power", "energyUsed", "energySaved"]
  },
  "040B01000001": {
    "type": "object",
    "properties": {
      "effectiveVolt": {"type": "number"},
      "effectiveCurrent": {"type": "number"},
      "direct": {"type": "number"},
      "toGrid": {"type": "number"},
      "toUser": {"type": "number"}
    },
    "required": ["effectiveVolt", "effectiveCurrent", "direct", "toGrid", "toUser"]
  },
  "040B01000004": {
    "type": "object",
    "properties": {
      "currentPower": {"type": "number"},
      "totalEnergy": {"type": "number"}
    },
    "required": ["currentPower", "totalEnergy"]
  },
  "040B01000005": {
    "type": "object",
    "properties": {
      "pacToGrid": {"type": "number"},
      "pacToUser": {"type": "number"},
      "eDisChargeTotal": {"type": "number"},
      "eChargeTotal": {"type": "number"},
      "eToGridTotal": {"type": "number"}
    },
    "required": ["pacToGrid", "pacToUser", "eDisChargeTotal", "eChargeTotal", "eToGridTotal"]
  }
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
            "value": {
              "type": [
                "object",
                "array",
                "number",
                "boolean",
                "string",
                "null"
              ]
            }
          },
          "required": ["name", "value"]
        }
      }
    },
    "required": ["uuid", "type", "timestamp", "offset", "data"]
  },
  getInverterDatas: {
    "type": "object",
    "properties": {
      "stationUuid": {"type": "string"},
      "dataType": {"type": "string"},
      "deviceType": {"type": "string"},
      "data.status": {"type": "number"},
      "timestamp": {"type": "string"},
      "timestampStart": {"type": "string"},
      "timestampStop": {"type": "string"}
    },
    "required": ["dataType"]
  }
};
var ONE_HOUR_MS = 1000 * 60 * 60;
var ONE_DAY_MS = ONE_HOUR_MS * 24;
const SOLAR_OBJECT_TYPES = [
  "010001000004", "01000C000004",
  "01000D000004", "04110E0E0001",
  "04110F0F0001", "010100000000",
  "060A08000000"
];

var isSolarObject = function (deviceType) {
  var found = _.findIndex(SOLAR_OBJECT_TYPES, function (item) {
    return item === deviceType;
  });
  return -1 !== found;
};
var solarDataVerify = function (device, curDatas, preDatas) {
  var ret = true;
  switch (device.type.id) {
    case "010001000004":
    case "01000C000004":
    case "01000D000004": {
      if (curDatas["energyToday"] - preDatas["energyToday"] > 300
      ) {
        logger.debug("curDatas[energyToday]= " + curDatas["energyToday"]
          + " preDatas[energyToday]= " + preDatas["energyToday"]);
        logger.warn("Inverter uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
      if (curDatas["energyTotal"] - preDatas["energyTotal"] > 300
      ) {
        logger.debug("curDatas[energyTotal]= " + curDatas["energyTotal"]
          + " preDatas[energyTotal]= " + preDatas["energyTotal"]);
        logger.warn("Inverter uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
    }
      break;
    case "010100000000": {
      if (curDatas["inverterEnergyTotal"] - preDatas["inverterEnergyTotal"] > 5000) {
        logger.debug("curDatas[inverterEnergyTotal]= " + curDatas["inverterEnergyTotal"]
          + " preDatas[inverterEnergyTotal]= " + preDatas["inverterEnergyTotal"]);
        logger.warn("Plant uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
      if (curDatas["inverterEnergyToday"] - preDatas["inverterEnergyToday"] > 5000) {
        logger.debug("curDatas[inverterEnergyToday]= " + curDatas["inverterEnergyToday"]
          + " preDatas[inverterEnergyToday]= " + preDatas["inverterEnergyToday"]);
        logger.warn("Plant uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
    }
      break;
    case "060A08000000": {
      if (curDatas["inverterEnergyTotal"] - preDatas["inverterEnergyTotal"] > 10000) {
        logger.debug("curDatas[inverterEnergyTotal]= " + curDatas["inverterEnergyTotal"]
          + " preDatas[inverterEnergyTotal]= " + preDatas["inverterEnergyTotal"]);
        logger.warn("User uuid:" + device.uuid + ", name:" + device.name + ",data invalidate.");
        ret = false;
      }
      if (curDatas["inverterEnergyToday"] - preDatas["inverterEnergyToday"] > 10000) {
        logger.debug("curDatas[inverterEnergyToday]= " + curDatas["inverterEnergyToday"]
          + " preDatas[inverterEnergyToday]= " + preDatas["inverterEnergyToday"]);
        logger.warn("User uuid:" + device.uuid + ", name:" + device.name + ",data invalidate.");
        ret = false;
      }
    }
      break;
    case "04110E0E0001": {
      if ((curDatas["ePositiveActive"] - preDatas["ePositiveActive"]) > 500
      ) {
        logger.debug("curDatas[ePositiveActive]= " + curDatas["ePositiveActive"]
          + " preDatas[ePositiveActive]= " + preDatas["ePositiveActive"]
          + " ratio=" + device.extra.ratio);
        logger.debug((curDatas["ePositiveActive"] - preDatas["ePositiveActive"]));
        logger.warn("Energy meter uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
      if ((curDatas["eReverseActive"] - preDatas["eReverseActive"]) > 1000
      ) {
        logger.debug("curDatas[eReverseActive]= " + curDatas["eReverseActive"]
          + " preDatas[eReverseActive]= " + preDatas["eReverseActive"]
          + " ratio=" + device.extra.ratio);
        logger.debug((curDatas["eReverseActive"] - preDatas["eReverseActive"]));
        logger.warn("Energy meter uuid:" + device.uuid + ", location:" + device.location.locationName + ",data invalidate.");
        ret = false;
      }
    }
      break;
  }
  return ret;
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
  else if ("annualReport" === dataType) {
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
    else if ("annualReport" === message.dataType) {
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
    else if ("annualReport" === message.dataType) {
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
var ITEMS_MAPPING = {
  "060A08000000": function (curData, preData) {
    var data = {};
    data["installedCapacity"] = curData["installedCapacity"];
    data["PVCount"] = curData["PVCount"];
    data["gatewayCount"] = curData["gatewayCount"];
    data["inverterCount"] = curData["inverterCount"];

    data["inverterNormal"] = curData["inverterNormal"];
    data["inverterAbnormal"] = curData["inverterAbnormal"];
    data["inverterStandby"] = curData["inverterStandby"];
    data["inverterUnknown"] = curData["inverterUnknown"];
    data["inverterShutDown"] = curData["inverterShutDown"];
    data["inverterOffLine"] = curData["inverterOffLine"];

    data["plantNormal"] = curData["plantNormal"];
    data["plantWarn"] = curData["plantWarn"];
    data["plantFault"] = curData["plantFault"];
    data["plantStopped"] = curData["plantStopped"];
    data["plantUnknown"] = curData["plantUnknown"];

    data["inverterEnergyTotal"] = curData["inverterEnergyTotal"] - preData["inverterEnergyTotal"];
    data["inverterEnergyToday"] = curData["inverterEnergyToday"] - preData["inverterEnergyToday"];
    data["PPvTotal"] = curData["PPvTotal"];
    data["PAcTotal"] = curData["PAcTotal"];
    data["FGrid"] = curData["FGrid"];
    data["energyPositiveActive"] = curData["energyPositiveActive"] - preData["energyPositiveActive"];
    data["energyReverseActive"] = curData["energyReverseActive"] - preData["energyReverseActive"];
    return data;
  },
  "010100000000": function (curData, preData) {
    var data = {};
    data["installedCapacity"] = curData["installedCapacity"];
    data["PVCount"] = curData["PVCount"];
    data["gatewayCount"] = curData["gatewayCount"];
    data["inverterCount"] = curData["inverterCount"];

    data["inverterNormal"] = curData["inverterNormal"];
    data["inverterAbnormal"] = curData["inverterAbnormal"];
    data["inverterStandby"] = curData["inverterStandby"];
    data["inverterUnknown"] = curData["inverterUnknown"];
    data["inverterShutDown"] = curData["inverterShutDown"];
    data["inverterOffLine"] = curData["inverterOffLine"];


    data["inverterEnergyTotal"] = curData["inverterEnergyTotal"] - preData["inverterEnergyTotal"];
    data["inverterEnergyToday"] = curData["inverterEnergyToday"] - preData["inverterEnergyToday"];
    data["PPvTotal"] = curData["PPvTotal"];
    data["PAcTotal"] = curData["PAcTotal"];
    data["FGrid"] = curData["FGrid"];
    data["energyPositiveActive"] = curData["energyPositiveActive"] - preData["energyPositiveActive"];
    data["energyReverseActive"] = curData["energyReverseActive"] - preData["energyReverseActive"];
    return data;
  },
  "010001000004": function (curData, preData) {
    var data = {};
    data["status"] = curData["status"];
    data["temperature"] = curData["temperature"];
    data["energyTotal"] = curData["energyTotal"] - preData["energyTotal"];
    data["energyToday"] = curData["energyToday"] - preData["energyToday"];
    data["pac"] = curData["pac"];
    data["rac"] = curData["rac"];
    data["eRac"] = curData["eRac"] - preData["eRac"];
    data["ppv"] = curData["ppv"];
    data["epvTotal"] = curData["epvTotal"] - preData["epvTotal"];
    data["epvTotal1"] = curData["epvTotal1"] - preData["epvTotal1"];
    data["epvTotal2"] = curData["epvTotal2"] - preData["epvTotal2"];
    data["vpv1"] = curData["vpv1"];
    data["pv1Cur"] = curData["pv1Cur"];
    data["vpv2"] = curData["vpv2"];
    data["pv2Cur"] = curData["pv2Cur"];
    data["vpv3"] = curData["vpv3"];
    data["pv3Cur"] = curData["pv3Cur"];
    data["vpv4"] = curData["vpv4"];
    data["pv4Cur"] = curData["pv4Cur"];
    data["vac1"] = curData["vac1"];
    data["iac1"] = curData["iac1"];
    data["vac2"] = curData["vac2"];
    data["iac2"] = curData["iac2"];
    data["vac3"] = curData["vac3"];
    data["iac3"] = curData["iac3"];
    return data;
  },
  "01000C000004": function (curData, preData) {
    var data = {};
    data["status"] = curData["status"];
    data["errorCode"] = curData["errorCode"];
    data["temperature"] = curData["temperature"];
    data["energyTotal"] = curData["energyTotal"] - preData["energyTotal"];
    data["energyYear"] = curData["energyYear"] - preData["energyYear"];
    data["energyMonth"] = curData["energyMonth"] - preData["energyMonth"];
    data["energyToday"] = curData["energyToday"] - preData["energyToday"];
    data["pac"] = curData["pac"];
    data["rac"] = curData["rac"];
    data["ap"] = curData["ap"];
    data["ppv"] = curData["ppv"];
    data["pvCount"] = curData["pvCount"];
    data["vpv1"] = curData["vpv1"];
    data["pv1Cur"] = curData["pv1Cur"];
    data["vpv2"] = curData["vpv2"];
    data["pv2Cur"] = curData["pv2Cur"];
    data["vpv3"] = curData["vpv3"];
    data["pv3Cur"] = curData["pv3Cur"];
    data["vpv4"] = curData["vpv4"];
    data["pv4Cur"] = curData["pv4Cur"];
    data["vac1"] = curData["vac1"];
    data["iac1"] = curData["iac1"];
    data["vac2"] = curData["vac2"];
    data["iac2"] = curData["iac2"];
    data["vac3"] = curData["vac3"];
    data["iac3"] = curData["iac3"];
    return data;
  },
  "01000D000004": function (curData, preData) {
    var data = {};
    data["status"] = curData["status"];
    data["temperature"] = curData["temperature"];
    data["energyTotal"] = curData["energyTotal"] - preData["energyTotal"];
    data["energyToday"] = curData["energyToday"] - preData["energyToday"];
    data["pac"] = curData["pac"];
    data["rac"] = curData["rac"];
    data["ap"] = curData["ap"];
    data["ppv"] = curData["ppv"];
    data["pvCount"] = curData["pvCount"];
    data["vpv1"] = curData["vpv1"];
    data["pv1Cur"] = curData["pv1Cur"];
    data["vpv2"] = curData["vpv2"];
    data["pv2Cur"] = curData["pv2Cur"];
    data["vpv3"] = curData["vpv3"];
    data["pv3Cur"] = curData["pv3Cur"];
    data["vpv4"] = curData["vpv4"];
    data["pv4Cur"] = curData["pv4Cur"];
    data["vac1"] = curData["vac1"];
    data["iac1"] = curData["iac1"];
    data["vac2"] = curData["vac2"];
    data["iac2"] = curData["iac2"];
    data["vac3"] = curData["vac3"];
    data["iac3"] = curData["iac3"];
    return data;
  },
  "04110E0E0001": function (curData, preData) {
    var data = {};
    data["eCombinationActive"] = curData["eCombinationActive"] - preData["eCombinationActive"];
    data["ePositiveActive"] = curData["ePositiveActive"] - preData["ePositiveActive"];
    data["eReverseActive"] = curData["eReverseActive"] - preData["eReverseActive"];
    data["ePositiveApparent"] = curData["ePositiveApparent"] - preData["ePositiveApparent"];
    data["eReverseApparent"] = curData["eReverseApparent"] - preData["eReverseApparent"];
    data["totalPowerFactor"] = curData["totalPowerFactor"];
    data["gridFrequency"] = curData["gridFrequency"];
    return data;
  },
  "04110F0F0001": function (curData, preData) {
    var data = {};
    data["windSpeed"] = curData["windSpeed"];
    data["windDirection"] = curData["windDirection"];
    data["rainfall"] = curData["rainfall"];
    data["irradiance"] = curData["irradiance"];
    data["soilTemperature"] = curData["soilTemperature"];
    data["soilHumidity"] = curData["soilHumidity"];
    data["airTemperature"] = curData["airTemperature"];
    data["airPressure"] = curData["airPressure"];
    data["airHumidity"] = curData["airHumidity"];
    data["illuminance"] = curData["illuminance"];
    data["evaporation"] = curData["evaporation"];
    return data;
  },
  "050608070001": function (curData, preData) {
    var data = {};
    data["temperature"] = curData["dis_temp"];
    return data;
  },
  "040B08040004": function (curData, preData) {
    var data = {};
    data["power"] = curData["power"];
    data["energyUsed"] = curData["energyUsed"] - preData["energyUsed"];
    data["energySaved"] = curData["energySaved"] - preData["energySaved"];
    return data;
  },
  "040B01000001": function (curData, preData) {
    var data = {};
    if (16 === curData["direct"]) {
      data["toUserPower"] = curData["effectiveVolt"] * curData["effectiveCurrent"] / 100;
      data["toGridPower"] = 0;
    }
    else {
      data["toUserPower"] = 0;
      data["toGridPower"] = curData["effectiveVolt"] * curData["effectiveCurrent"] / 100;
    }
    data["toGrid"] = curData["toGrid"] - preData["toGrid"];
    data["toUser"] = curData["toUser"] - preData["toUser"];
    delete curData.direct;
    delete curData.effectiveVolt;
    delete curData.effectiveCurrent;
    return data;
  },
  "040B01000004": function (curData, preData) {
    var data = {};
    data["currentPower"] = curData["currentPower"];
    data["totalEnergy"] = curData["totalEnergy"] - preData["totalEnergy"];
    return data;
  },
  "040B01000005": function (curData, preData) {
    var data = {};
    data["pacToGrid"] = curData["pacToGrid"];
    data["pacToUser"] = curData["pacToUser"];
    data["eDisChargeTotal"] = curData["eDisChargeTotal"] - preData["eDisChargeTotal"];
    data["eChargeTotal"] = curData["eDisChargeTotal"] - preData["eChargeTotal"];
    data["eToGridTotal"] = curData["eDisChargeTotal"] - preData["eToGridTotal"];
    return data;
  }
};

/**
 * @constructor
 * */
function DataManager(conx, uuid, token, configurator) {
  this.db = null;
  this.rawDataModel = null;
  this.dataModel = null;
  this.dailyDataModel = null;
  this.monthlyDataModel = null;
  this.annualReportDataModel = null;
  this.allDataModel = null;
  this.getDevice = function (condition, callback) {
    var self = this;
    var msg = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "getDevice",
        cmdCode: "0003",
        parameters: condition
      }
    };
    self.message(msg, function (response) {
      if (response.retCode === 200) {
        var deviceInfo = response.data;
        if (util.isArray(response.data)) {
          deviceInfo = response.data[0];
        }
        callback(null, deviceInfo);
      }
      else if (response.retCode === 200003) {
        callback({errorId: response.retCode, errorMsg: response.description});
      }
      else {
        callback(null, null);
      }
    });
  };
  this.getDevices = function (condition, callback) {
    var self = this;
    var msg = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "getDevice",
        cmdCode: "0003",
        parameters: condition
      }
    };
    self.message(msg, function (response) {
      if (response.retCode === 200) {
        var devices = response.data;
        callback(null, devices);
      }
      else if (response.retCode === 200003) {
        callback({errorId: response.retCode, errorMsg: response.description});
      }
      else {
        callback(null, null);
      }
    });
  };
  VirtualDevice.call(this, conx, uuid, token, configurator);
}

util.inherits(DataManager, VirtualDevice);

/**
 * 设备管理器初始化，将系统已经添加的设备实例化并挂载到Meshblu网络
 * */
DataManager.prototype.init = function () {
  var self = this;
  const options = {
    useMongoClient: true,
    reconnectTries: Number.MAX_VALUE, // Never stop trying to reconnect
    reconnectInterval: 500, // Reconnect every 500ms
    poolSize: 10, // Maintain up to 10 socket connections
    // If not connected, return errors immediately rather than waiting for reconnect
    bufferMaxEntries: 0
  };
  var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"), options);
  mongoose.Promise = global.Promise;
  db.once('error', function (error) {
    logger.error(200005, error);
  });

  db.once('open', function () {
    self.db = db;
    self.rawDataModel = self.db.model("raw_data", DEVICE_DATA_SCHEMA);
    self.dataModel = self.db.model("data", DEVICE_DATA_SCHEMA);
    self.dailyDataModel = self.db.model("daily_data", DEVICE_DATA_SCHEMA);
    self.monthlyDataModel = self.db.model("monthly_data", DEVICE_DATA_SCHEMA);
    self.annualReportDataModel = self.db.model("yearly_data", DEVICE_DATA_SCHEMA);
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
DataManager.prototype.getData = function (message, peerCallback) {
  var self = this;
  logger.debug(message);
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
          else {
            var offset = 0;
            if (!util.isNullOrUndefined(device.timeZone)
              && !util.isNullOrUndefined(device.timeZone.offset)) {
              offset = parseInt(device.timeZone.offset);
              var startTime = new Date(message.timestamp.$gte).getTime() - offset;
              var endTime = new Date(message.timestamp.$lt).getTime() - offset;
              message.timestamp.$gte = new Date(startTime).toISOString();
              message.timestamp.$lt = new Date(endTime).toISOString();
            }
            var dataModel = null;
            if ("dataReport" === message.dataType) {
              dataModel = self.dataModel;
            }
            else if ("dailyReport" === message.dataType) {
              dataModel = self.dailyDataModel;
            }
            else if ("monthlyReport" === message.dataType) {
              dataModel = self.monthlyDataModel;
            }
            else if ("annualReport" === message.dataType) {
              dataModel = self.annualReportDataModel;
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
                uuid: message.uuid,
                timestamp: {
                  $gte: new Date(message.timestamp.$gte),
                  $lt: new Date(message.timestamp.$lt)
                }
              };
              if ("allReport" === message.dataType) {
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
                    if (0 !== offset) {
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

DataManager.prototype.getOrgData = function (message, peerCallback) {
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
          else {
            var dataModel = null;
            if ("dataReport" === message.dataType) {
              dataModel = self.dataModel;
            }
            else if ("dailyReport" === message.dataType) {
              dataModel = self.dailyDataModel;
            }
            else if ("monthlyReport" === message.dataType) {
              dataModel = self.monthlyDataModel;
            }
            else if ("annualReport" === message.dataType) {
              dataModel = self.annualReportDataModel;
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
                uuid: message.uuid,
                timestamp: {
                  $gte: new Date(message.timestamp.$gte),
                  $lt: new Date(message.timestamp.$lt)
                }
              };
              if ("allReport" === message.dataType) {
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
                  responseMessage.data = results;
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
DataManager.prototype.putData = function (message, peerCallback) {
  var self = this;
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
            var dataRecord = {
              uuid: message.uuid,
              userId: message.userId,
              type: message.type,
              timestamp: new Date(message.timestamp),
              offset: message.offset,
              data: {}
            };
            var curData = {};
            _.forEach(message.data, function (item) {
              curData[item.name] = parseFloat(item.value);
            });
            //保存原始数据
            message.data = curData;
            var rawDataEntity = new self.rawDataModel(message);
            rawDataEntity.save(function (error) {
              if (error) {
                logger.error(212000, error);
              }
            });
            if (util.isNullOrUndefined(device.extra)) {
              device.extra = {};
            }
            if (util.isNullOrUndefined(device.extra.preItems)) {
              //第一次采集不存报表记录(无上次采集记录)
              dataRecord = null;
            }
            else {
              if (isSolarObject(device.type.id)) {
                var ret = solarDataVerify(device, curData, device.extra.preItems);
                if (!ret) {
                  dataRecord = null;//无效数据本次不保存采集结果
                }
              }
              if (!util.isNullOrUndefined(dataRecord)) {
                var mapping = ITEMS_MAPPING[dataRecord.type];
                var preData = device.extra.preItems;
                if (!util.isNullOrUndefined(mapping) && util.isFunction(mapping)) {
                  dataRecord.data = mapping(curData, preData);
                  //排除负数
                  _.forEach(dataRecord.data, function (value, key) {
                    if (0 > value) {
                      dataRecord.data[key] = 0;
                    }
                  })
                }
              }
            }
            var msg = {
              devices: self.configurator.getConfRandom("services.device_manager"),
              payload: {
                cmdName: "deviceUpdate",
                cmdCode: "0004",
                parameters: {
                  "uuid": device.uuid,
                  "extra.preItems": curData
                }
              }
            };
            self.message(msg, function (response) {
              //logger.debug(response);
            });
            innerCallback(null, dataRecord);
          },
          function (data, innerCallback) {
            if (!util.isNullOrUndefined(data)) {
              var mongooseEntity = new self.dataModel(data);
              mongooseEntity.save(function (error) {
                if (error) {
                  innerCallback({
                    errorId: 212000,
                    errorMsg: error
                  });
                }
                else {
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

DataManager.prototype.getInverterDatas = function (message, peerCallback) {
  var self = this;
  logger.debug(message);
  var responseMessage = {retCode: 200, description: "Success.", data: []};
  self.messageValidate(message, OPERATION_SCHEMAS.getInverterDatas, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
    }
    else {
      async.waterfall([
          function (innerCallback) {
            if (!message.stationUuid || "abc43e47-9de9-435c-8004-bf54550235dd" === message.stationUuid) {
              innerCallback(null, null);
            }
            else {
              self.getDevices({"owner": message.stationUuid}, function (error, devices) {
                if (error) {
                  innerCallback(error);
                  return;
                }
                async.map(devices, function (solarCollector, cb) {
                  self.getDevices({"owner": solarCollector.uuid}, function (error, devices) {
                    if (util.isNullOrUndefined(devices)) {
                      devices = [];
                    }
                    cb(null, devices);
                  })
                }, function (error, slaves) {
                  innerCallback(error, _.flatten(slaves))
                });
              });
            }
          },
          function (slaves, innerCallback) {
            var dataModel = null;
            if ("dataReport" === message.dataType) {
              dataModel = self.dataModel;
            }
            else if ("dailyReport" === message.dataType) {
              dataModel = self.dailyDataModel;
            }
            else if ("monthlyReport" === message.dataType) {
              dataModel = self.monthlyDataModel;
            }
            else if ("annualReport" === message.dataType) {
              dataModel = self.annualReportDataModel;
            }
            else if ("allReport" === message.dataType) {
              dataModel = self.allDataModel;
            }
            if (util.isNullOrUndefined(dataModel)) {
              innerCallback({errorId: 200001, errorMsg: "invalid data type:" + message.dataType});
            }
            else {
              var conditions = {
                "type": {
                  "$in": ["010001000004", "01000C000004", "01000D000004"]
                }
              };
              if (!util.isNullOrUndefined(message["data.status"])) {
                conditions["data.status"] = message["data.status"];
              }
              if (message.timestamp) {
                dataModel = self.dailyDataModel;  //对于时间点的状态查询均是日报粒度查询
                var time = new Date(message.timestamp).getTime();
                if ("monthlyReport" === message.dataType) {
                  if (new Date(time).getDate() === new Date().getDate()) {  //当天取中间时间
                    time += _.floor(new Date().getHours() / 2) * ONE_HOUR_MS;
                  }
                  else {
                    time += 12 * ONE_HOUR_MS; //往日取中午12点
                  }
                }
                else if ("annualReport" === message.dataType) {
                  if (new Date(time).getMonth() === new Date().getMonth()) {  //当月取中间时间
                    time += (_.floor(new Date().getDate() / 2)-1) * ONE_DAY_MS + 12 * ONE_HOUR_MS;
                  }
                  else {
                    var tempDate = new Date(time);
                    tempDate.setMonth(tempDate.getMonth()+1);
                    tempDate.setDate(0);
                    time += (_.floor(tempDate.getDate() / 2)-1) * ONE_DAY_MS + 12 * ONE_HOUR_MS;
                  }
                }
                conditions["timestamp"] = new Date(time);
              }
              else {
                var startTime = new Date(message.timestampStart).getTime();
                var endTime = new Date(message.timestampStop).getTime();
                conditions["timestamp"] = {
                  "$gte": new Date(startTime),
                  "$lt": new Date(endTime)
                };
              }
              if ("allReport" === message.dataType) {
                delete conditions.timestamp;
              }
              logger.debug(conditions);
              dataModel.find(conditions).exec(function (error, results) {
                if (error) {
                  logger.error(212000, error);
                  innerCallback({errorId: 212000, errorMsg: JSON.stringify(error)});
                }
                else {
                  if (slaves) {
                    var retDatas = [];
                    _.forEach(results, function (dataItem) {
                      var slaveDevice = _.find(slaves, function (item) {
                        return dataItem.uuid === item.uuid;
                      });
                      if (!slaveDevice) {
                        return;
                      }
                      slaveDevice.extra.items = dataItem.data;
                      delete slaveDevice.extra.preItems;
                      delete slaveDevice.extra.modbusRtuCmd;
                      delete slaveDevice.geo;
                      delete slaveDevice.status;
                      delete slaveDevice.meshblu;
                      delete slaveDevice.myToken;
                      delete slaveDevice.token;
                      delete slaveDevice.timeZone;
                      delete slaveDevice.onlineSince;
                      retDatas.push(slaveDevice);
                    });
                    innerCallback(null, retDatas);
                  }
                  else {
                    async.map(results, function (dataItem, cb) {
                      self.getDevice({uuid: dataItem.uuid}, function (err, slaveDevice) {
                        if (slaveDevice) {
                          slaveDevice.extra.items = dataItem.data;
                          delete slaveDevice.extra.preItems;
                          delete slaveDevice.extra.modbusRtuCmd;
                          delete slaveDevice.geo;
                          delete slaveDevice.status;
                          delete slaveDevice.meshblu;
                          delete slaveDevice.myToken;
                          delete slaveDevice.token;
                          delete slaveDevice.timeZone;
                          delete slaveDevice.onlineSince;
                        }
                        cb(null, slaveDevice);
                      })
                    }, function (error, results) {
                      var retDatas = [];
                      _.forEach(results, function (item) {
                        if (item) retDatas.push(item);
                      });
                      innerCallback(error, retDatas);
                    });
                  }
                }
              });
            }
          }
        ],
        function (error, retDatas) {
          if (error) {
            responseMessage.retCode = error.errorId;
            responseMessage.description = error.errorMsg;
          }
          else {
            responseMessage.data = retDatas
          }
          peerCallback(responseMessage);
        });
    }
  });
};

module.exports = {
  Service: DataManager,
  OperationSchemas: OPERATION_SCHEMAS
};