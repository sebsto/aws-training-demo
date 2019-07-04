
// Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

//
// Please change these value to adapt to your configuration
//
var FILTER_CONFIG_BUCKET = 'BUCKETNAME';
var FILTER_CONFIG_FILE = 'filter_config.json';

//
// Nothing to change beyond this point
//
var FILTER_CONFIG = '';

console.log('Loading Lambda function');

// initialize libraries and global values
var aws = require('aws-sdk');
var fs = require('fs');


async function init() {
  return new Promise((resolve, reject) => {
    if (FILTER_CONFIG === '') {
      var s3 = new aws.S3({ apiVersion: '2006-03-01' });
      var params = { Bucket: FILTER_CONFIG_BUCKET, Key: FILTER_CONFIG_FILE };
      var body = s3.getObject(params).createReadStream().on('data', function (data) {
        FILTER_CONFIG = JSON.parse(data);
        console.log('Config : %j', FILTER_CONFIG);
        resolve(FILTER_CONFIG);
      });
    }
  });
}

async function sendNotification(msg, topicARN, endPointARN) {
  return new Promise((resolve, reject) => {

    var sns = new aws.SNS({
      apiVersion: '2010-03-31',
      region: FILTER_CONFIG.sns.region
    });

    var params = {}
    if (topicARN !== '') {
      params = {
        Message: msg,
        //MessageStructure: 'json',
        TopicArn: topicARN
      };
    } else {
      params = {
        Message: msg,
        //MessageStructure: 'json',
        TargetArn: endPointARN
      };
    }

    console.log(params);

    sns.publish(params, function (err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        reject(err);
      } else {
        console.log(data);           // successful response
        resolve(data);
      }
    });

  });
}

async function download(bucket, key) {
  return new Promise((resolve, reject) => {

    // extract file name from key name
    var fileName = key.match(/.*\/(.*).json.gz$/)[1]
    console.log("RegExp file name = " + fileName);
    var file = fs.createWriteStream('/tmp/' + fileName + '.json.gz');

    // pipe from S3 to local file
    var s3 = new aws.S3({ apiVersion: '2006-03-01' });
    var params = { Bucket: bucket, Key: key };
    var stream = s3.getObject(params).createReadStream();
    stream.pipe(file);

    stream.on('error', function (error) {
      console.log(error);
      reject(error);
    });

    stream.on('end', function () {
      console.log('End of read stream');
      resolve(file);
    });
  });
}

async function extract(file) {
  return new Promise((resolve, reject) => {

    // find the GZ file name
    var gzFileName = file.path.match(/.*\/(.*).json.gz$/)[1];
    var jsFileName = '/tmp/' + gzFileName + '.json';

    // unzip the file to local file system
    var zlib = require('zlib');
    var unzip = zlib.createGunzip();
    var inp = fs.createReadStream(file.path);
    var out = fs.createWriteStream(jsFileName);

    console.log("Going to extract \n" + file.path + "\nto\n" + jsFileName);
    inp.pipe(unzip).pipe(out);

    out.on('error', function (error) {
      console.log(error);
      reject(error);
    });

    out.on('close', function () {
      console.log('End of write stream');
      resolve(jsFileName);
    });
  });
}

async function filter(file) {
  return new Promise((resolve, reject) => {

    console.log("Going to filter\n" + file);

    // filter every single record from the log file
    // returns an array containing every single matching records
    var cloudTrailLog = require(file);
    var records = cloudTrailLog.Records.filter(function (x) {
      return x.eventSource.match(new RegExp(FILTER_CONFIG.source));
    });
    var records2 = records.filter(function (x) {
      return x.eventName.match(new RegExp(FILTER_CONFIG.regexp));
    });
    console.log("Filtered Records:");
    console.log(records2);

    resolve(records2);
  });
}

async function notify(records) {

  var deferredTasks = Array();
  
  // for each record, send an SNS notification
  for (var i = 0; i < records.length; i++) {
    console.log('Sending notification #' + i + 1)
    var message = "Event  : " + records[i].eventName + "\n" +
      "Source : " + records[i].eventSource + "\n" +
      "Params : " + JSON.stringify(records[i].requestParameters, null, '') + "\n" +
      "Region : " + records[i].awsRegion + "\n"
    var task = sendNotification(message, FILTER_CONFIG.sns.topicARN, '');
    deferredTasks.push(task);
  }

  if (records.length > 0) {
    console.log("Done sending notifications");
  }

  return Promise.all(deferredTasks);
}

exports.handler = async (event) => {

  return new Promise(async (resolve, reject) => {

    console.log('Received event:');
    console.log(JSON.stringify(event, null, '  '));

    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;

    try {

      await init();

      // Download from S3, Gunzip, Filter and send notifications
      const file = await download(bucket, key);
      const name = await extract(file);
      const records = await filter(name);
      await notify(records);

    } catch (error) {
      // Handle any error from all above steps
      console.error(
        'Error while handling ' + bucket + '/' + key +
        '\nError: ' + error
      );
      reject(error);
    }

    console.log('Finished handling ' + bucket + '/' + key);
    resolve("OK");
  });
};

