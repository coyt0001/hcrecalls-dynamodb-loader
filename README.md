# HC Recalls Data Loader

This package is used for uploading and reuploading HC Recalls data into DynamoDB on AWS.

## Setup

Requires node >=v8.1.0 and npm.

- Clone the repository and run `npm install`.
- Follow the instructions in `dynamo.config.template.js` to create local config.
- See the commands below for basic usage

## Commands

| Command | Description |
| ------------- | ------------- |
| <code>test</code> | Runs test scripts. (There are currently no tests) |
| <code>start</code> | Runs data loader |
| <code>DELETE-ALL</code> | Clears out test table to prepare for reupload. |

### Start

Starts the script. If no args are passed it will prompt for direction.

#### Arguments

Use these arguments when invoking start to achieve desired results:

```sh
npm start -- [--recent --full --clean --dry --upload --all]
```

| Argument | Description |
| ------------- | ------------- |
| <code>--recent</code> | Downloads recent data from HC Recalls API and stores in local files |
| <code>--full</code> | Takes recent recalls data and replaces it with detailed results. |
| <code>--clean</code> | Cleans HTML from recalls data files. |
| <code>--dry</code> | Forms upload request without uploading. |
| <code>--upload</code> | Uploads all items from cleansed data to DynamoDB |
| <code>--all</code> | Runs through entire process (recent>full>clean>upload). **Note:** --dry must be used with --all in order for a full and dry run. |

### DELETE-ALL

Deletes all items out of the RecallsTestData-EN table.
