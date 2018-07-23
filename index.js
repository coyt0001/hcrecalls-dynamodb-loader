/**
 * @file index.js
 * @description Entry point for data-loader. Contains logic for extracting HC Recalls data from API, cleaning and uploading it to DynamoDB on AWS.
 * @author HC-SC
 * Last Modified: July 19, 2017
 */

const
  fs = require('fs'),
  R = require('ramda'),
  util  =require('util'),
  AWS = require('aws-sdk'),
  prompt = require('prompt-promise'),
  fetch = require('node-fetch'),
  dynamoConfig = require('./dynamo.config.js'),
  
  TABLE_NAME = "RecallsTestData-EN",

  /**
   * @function _sleep
   * @description Use in async function with 'await' keyword for paused execution.
   * @param {Number} delay How long to delay (ms)
   * @returns {Promise}
   */
  _sleep = async delay => new Promise((resolve) => { setTimeout(resolve, delay) });

/**
 * @class DataLoader
 * @description Handles batch writing of JSON data to DynamoDB
 * @prop {Array} data Test data to be inserted into table
 * @prop {String} table Name of table to insert data into
 * @prop {Number} category Category being loaded
 * @prop {String} table Name of table to insert data into
 * @prop {Object} textInterval Interval container for loading text
 * @prop {Function} loadingText textInterval callback, writes loading message to the console
 * @prop {String} dots String representing dots in loader
 * @prop {Object} dynamoHelper AWS DynamoDB Object
 * @prop {Function} batchWriteItem Promisified DynamoDB.batchWriteItem method
 * @prop {Function} listTables Promisified DynamoDB.listTables method
 * @prop {Function} createTable Promisified DynamoDB.createTable method
 * @prop {Function} start Formats request and begins data insertion
 */
class DataLoader {
  /**
   * @constructor DataLoader
   * @param {Array} data Test data to be inserted into table
   * @param {String} table Name of table to insert data into
   * @param {Object} config AWS Endpoint configuration
   * @param {Number} category Category being loaded
   */
  constructor(data, table, config, category) {
    this.data = data;
    this.table = table;
    this.category = category;
    this.textInterval = null;
    this.dots = "...";

    // Initialize AWS DynamoDB helper
    const dynamoHelper = this.dynamoHelper = new AWS.DynamoDB(config);

    // Wrap dynamo methods in async wrappers to handle callbacks & assign as properties of DataLoader

    this.batchWriteItem = async item => new Promise((resolve, reject) => {
      dynamoHelper.batchWriteItem(item, function(err, data) {
        return err
          ? reject(err)
          : resolve(data);
      });
    });

    this.listTables = async () => new Promise((resolve, reject) => {
      dynamoHelper.listTables({}, (err, data) => {
        return err
          ? reject(err)
          : resolve(data.TableNames);
      });
    });

    this.createTable = async params => new Promise((resolve, reject) => {
      dynamoHelper.createTable(params, (err, data) => {
        return err
          ? reject(err)
          : resolve(data);
      });
    });
  }

  /**
   * @method loadingText
   * @description textInterval callback, writes loading message to the console
   */
  loadingText() {
    switch(this.dots) {
      case "":
      case ".":
      case "..":
        this.dots = `${this.dots}.`;
        break;
      default:
        this.dots = "";
        break;
    }

    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(`Loading data into table${this.dots}`);
  }

  /**
   * @method mapRequest
   * @description Maps request object for DynamoDB
   * @param {Object} Item JSON to be mapped into request format
   * @returns {Object}
   */
  mapRequest(Item) {
    for (let prop in Item) {
      Item[prop] = AWS.DynamoDB.Converter.input(Item[prop]);
    }
    
    return Item;
  }

  /**
   * @method start
   * @description Formats request and begins data insertion
   * @prop {Boolean} dry Dry run flag
   */
  async start(dry) {
    // Check if table exists. If not, create it
    const tableList = await this.listTables();
    // console.log(util.inspect(tableList, { depth: null }));

    if (!tableList.includes(TABLE_NAME)) {
      const createTable = await prompt(`Table, '${TABLE_NAME}' can't be found, would you like to create it? [y/n] `);

      if (/^y(?:es)?$/i.test(createTable)) {
        await this.createTable({
          TableName : TABLE_NAME,
          KeySchema: [       
            { AttributeName: "recallId", KeyType: "HASH"},
          ],
          AttributeDefinitions: [
            { AttributeName: "recallId", AttributeType: "S" }
          ],
          ProvisionedThroughput: {       
            ReadCapacityUnits: 1, 
            WriteCapacityUnits: 1
          }
        });
  
        console.log(`⚠️  Due to a known AWS error, it is not possible to detect when this table has finished being created.\n - Please manually check that '${TABLE_NAME}' has been properly created in the DynamoDB dashboard before proceeding.`);
      }
      else {
        console.log(' - ABORTING.');
      }

      return;
    }

    // Map data into batch request
    let multiRequest = false;

    const request = {
      RequestItems: {
        [this.table]: R.map(Item => ({ PutRequest: { Item: this.mapRequest(Item) } }), this.data)
      }
    };

    if (request.RequestItems[this.table].length > 25) {
      console.log("Large request detected, chunking into safe requests...");
      // Multiple batch requests are required.
      multiRequest = [];

      while (request.RequestItems[this.table].length > 0) {
        const _req = {
          RequestItems: {
            [this.table]: request.RequestItems[this.table].splice(0, 25)
          }
        };

        // console.log(`New multi request chunk length: ${_req.RequestItems[this.table].length}`);

        multiRequest.push(_req);
      }
    }

    // Check if dry run, return and console log request(s)
    if (dry) {
      const _debugFilename = `./_raw/${this.category}-DEBUG.json`;
      console.log(`Dry run enabled, writing request object to '${_debugFilename}'...`);
      return fs.writeFileSync(_debugFilename, JSON.stringify(multiRequest.length ? multiRequest : request), 'utf8');
    }

    // Start loading text
    this.textInterval = setInterval(this.loadingText, 250);

    // Insert data and await result
    try {
      if (multiRequest.length) {
        const results = [];

        for (let _req of multiRequest) {
          results.push(await this.batchWriteItem(_req));
          await _sleep(1500);
        }

        clearInterval(this.textInterval);
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        console.log("Data insertion complete!\nResults:\n", results);

        await Promise.all(R.map(async res => {
          console.log("Result:\n", util.inspect(res, { depth: null }));

          if (res.UnprocessedItems.length) {
            console.log("⚠️ Warning!!⚠️ \nOne or more items was unable to be processed by AWS:\n", util.inspect(res.UnprocessedItems, { depth: null }));
            if (result.UnprocessedItems.length) {
              const _unprocessedLoader = new DataLoader(result.UnprocessedItems, this.table, this.dynamoConfig);
              await _unprocessedLoader.start();
            }
          }
        }, results));

        return;
      }

      const result = await this.batchWriteItem(request);

      clearInterval(this.textInterval);
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log("Data insertion complete!\n");
      
      const unprocessedItems = !!R.keys(result.UnprocessedItems).length;

      if (unprocessedItems) {
        console.log("⚠️ Warning!!⚠️ \nOne or more items was unable to be processed by AWS (trying again):\n", util.inspect(result, { depth: null }));
        if (result.UnprocessedItems.length) {
          const _unprocessedLoader = new DataLoader(result.UnprocessedItems, this.table, this.dynamoConfig);
          await _unprocessedLoader.start();
        }
      }
    }
    catch(err) {
      clearInterval(this.textInterval);
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log("Error inserting data:\n", util.inspect(err, { depth: 3 }));
    }

    return;
  }
}

/**
 * @function startLoading
 * @description Upload data items into DynamoDB
 */
async function startLoading(dry) {
  try {
    for (let i = 1; i <= 4; i++) {
      const 
        data = JSON.parse(fs.readFileSync(`./_raw/${i}-stripped.json`, 'utf8')),
        loader = new DataLoader(data, TABLE_NAME, dynamoConfig, i);
    
      await loader.start(dry);
    }
  }
  catch(err) {
    console.log("Error reading test data:\n", util.inspect(err, { depth: 3 }));
  }
}

/**
 * @function getRecentData
 * @description Pulls down basic recent recall data from HC Recalls API and writes it to files in _raw
 */
async function getRecentData() {
  try {
    if (!fs.existsSync("./_raw")) {
      console.log("No './_raw' directory found, creating it now...");

      fs.mkdirSync("./_raw");
    }

    // For each category type, get some recent recalls data and write it to a file in the _raw directory
    for (let i = 1; i <= 4; i++) {
      let _recent = await fetch(`https://healthycanadians.gc.ca/recall-alert-rappel-avis/api/search?search=&lang=en&cat=${i}&lim=250&off=0`);
      _recent = (await _recent.json()).results;
      // console.log(_recent);

      fs.writeFileSync(`./_raw/${i}.json`, JSON.stringify(_recent));
      console.log(` - Created file: './_raw/${i}.json'.`);
    }
  }
  catch(err) {
    console.log("Error fetching recent data:\n", util.inspect(err, { depth: 3 }));
  }
}

/**
 * @function getFullData
 * @description Takes recent results and maps them to full detailed results.
 */
async function getFullData() {
  try {
    for (let i = 1; i <= 4; i++) {
      let data = JSON.parse(fs.readFileSync(`./_raw/${i}.json`, 'utf8'));

      data = R.map(async item => {
        try {
          let _full = await fetch(`https://healthycanadians.gc.ca/recall-alert-rappel-avis/api/${item.recallId}/en`);
          _full = await _full.json();
          // console.log(_full);
          return _full;
        }
        catch(err) {
          console.log(err);
          return item;
        }
        // console.log(item);
        // return item;
      }, data.results);

      data = await Promise.all(data);

      fs.writeFileSync(`./_raw/${i}.json`, JSON.stringify(data));
    }
  }
  catch(err) {
    console.log("Error mapping test data:\n", util.inspect(err, { depth: 3 }));
  }
}

/**
 * @function htmlStripper
 * @description Used to recursivly map through a JSON and strip HTML out of string properties
 * @param {Object} data JSON object to be processed
 */
function htmlStripper(data) {
  if (typeof data === 'string') {
    data = data.replace(/(<([^>]+)>)|\r?\n|\r/ig, "");
  }
  else if (Array.isArray(data)) {
    data = R.map(htmlStripper, data);
  }
  else if (typeof data === 'object') {
    for (let prop in data) {
      data[prop] = htmlStripper(data[prop]);
    }
  }

  return data;
}

/**
 * @function cleanData
 * @description Reads data from _raw/{1-4}.js, strips HTML from them and writes them to _raw/{1-4}-stripped.json
 */
function cleanData() {
  try {
    for (let i = 1; i <= 4; i++) {
      let data = JSON.parse(fs.readFileSync(`./_raw/${i}.json`, 'utf8'));

      data = R.map(htmlStripper, data);

      fs.writeFileSync(`./_raw/${i}-stripped.json`, JSON.stringify(data));
    }
  }
  catch(err) {
    console.log("Error mapping test data:\n", util.inspect(err, { depth: 3 }));
  }
}

/**
 * @function initialize
 * @description Package entry point. Parses args and executes appropriate functions.
 */
async function initialize() {
  // Get args
  let {
    recent,
    full,
    clean,
    dry,
    upload,
    all
  } = require('minimist')(process.argv.slice(2));

  // If all is selected, enable all flags minus dry.
  if (all) {
    recent = full = clean = upload = all;
  }

  while (!recent && !full && !clean && !upload && !dry) {
    // If no options are selected, prompt asking which function to run.
    switch((await prompt("No command selected, please choose one of the following:\n[1] Pull down recent recall data from HC Recalls API\n[2] Transform captured recent data into detailed data.\n[3] Clean HTML from data\n[4] Upload to DynamoDB\n[5] Perform a dry run of uploading to DynamoDB\n[e|exit] Exits\n[a|all] Runs through entire process.\n--->  ")).toLowerCase()) {
      case "1":
        recent = true;
        break;
      case "2":
        full = true;
        break;
      case "3":
        clean = true;
        break;
      case "4":
        upload = true;
        break;
      case "5":
        dry = upload = true;
        break;
      case "all":
        recent = full = clean = dry = upload = true;
        break;
      case "exit":
        return process.exit();
        break;
      default:
        console.log("Invalid choice...");
        break;
    }
  }
  
  // Pull recent data from HC Recalls API
  recent && await getRecentData();

  // Transform recent data into full data
  full && await getFullData();
  
  // Clear raw data
  clean && await cleanData();
  
  // Start loader
  (upload || dry) && await startLoading(!!dry);

  return process.exit();
}

initialize();
