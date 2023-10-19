require("dotenv").config();
const requestLib = require('request');

class DruidHelper {
    constructor(druidOptions) {
        let DRUID_API_URI = undefined;
        let DRUID_QUERY_URI = undefined;
        this.defaultOffset = druidOptions && druidOptions.defaultOffset || 50000;
        this.maxNumberOfIterationsInScanOffsets = druidOptions && druidOptions.maxNumberOfIterationsInScanOffsets || 200;
        this.maxTopNThreshold = druidOptions && druidOptions.maxTopNThreshold || 1000;
        this.druidHelperPrefix = "DruidHelper :: ";
        this.httpCompressData = true;
        if (druidOptions && druidOptions.DRUID_API_URI && druidOptions.DRUID_API_URI.length > 0) {
            DRUID_API_URI = druidOptions.DRUID_API_URI;
        } else {
            DRUID_API_URI = process.env.DRUID_API_URI;
        }

        if (druidOptions && druidOptions.DRUID_QUERY_URI && druidOptions.DRUID_QUERY_URI.length > 0) {
            DRUID_QUERY_URI = druidOptions.DRUID_QUERY_URI;
        } else {
            DRUID_QUERY_URI = process.env.DRUID_QUERY_URI;
        }

        if (druidOptions && druidOptions.DRUID_USERNAME && druidOptions.DRUID_USERNAME.length > 0) {
            this.username = druidOptions.DRUID_USERNAME;
        } else {
            this.username = process.env.DRUID_USERNAME;
        }

        if (druidOptions && druidOptions.DRUID_PASSWORD && druidOptions.DRUID_PASSWORD.length > 0) {
            this.password = druidOptions.DRUID_PASSWORD;
        } else {
            this.password = process.env.DRUID_PASSWORD;
        }

        if (DRUID_QUERY_URI) {
            if (this.username && this.password) {
                let druidSplit = DRUID_QUERY_URI.split("//");
                this.DRUID_QUERY_URI = druidSplit[0] + "//" + this.username + ":" + this.password + "@" + druidSplit[1];
            } else {
                this.DRUID_QUERY_URI = DRUID_QUERY_URI;
            }
        } else {
            console.log(this.druidHelperPrefix + "DRUID_QUERY_URI must be defined");
        }

        if (DRUID_API_URI) {
            if (this.username && this.password) {
                let druidSplit = DRUID_API_URI.split("//");
                this.DRUID_API_URI = druidSplit[0] + "//" + this.username + ":" + this.password + "@" + druidSplit[1];
            } else {
                this.DRUID_API_URI = DRUID_API_URI;
            }
        } else {
            console.log(this.druidHelperPrefix + "DRUID_API_URI must be defined");
        }

        //Druid post request
        this.postRequestBody = {
            headers: {
                'Content-type': 'application/json',
                'Accept': 'application/json'
            },
            json: true,
            gzip: this.httpCompressData ? true : undefined,
            rejectUnauthorized: false
        };

    }

    ingestSpecFile(specObjectToIngest, options) {
        let logPrefix = options.logPrefix || '';
        return new Promise((resolve, reject) => {
            console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Info Ingesting file");
            const postRequestBody = Object.assign({}, this.postRequestBody, {
                url: this.DRUID_API_URI + "indexer/v1/task",
                body: specObjectToIngest
            });
            requestLib.post(postRequestBody, (error, response, body) => {
                if (error) {
                    console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Error in task creation" + JSON.stringify(error));
                    console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Error:Response in task creation" + JSON.stringify(response));
                    reject(error);
                } else {
                    if (options.compact) {
                        if (body.error) {
                            console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Error:Response in task creation" + JSON.stringify(response));
                            reject(body.error);
                        } else {
                            resolve(body);
                        }
                    } else {
                        if (specObjectToIngest.spec.ioConfig.inputSource.uris.length > 0 && specObjectToIngest.spec.ioConfig.inputSource.uris[0] !== "") {
                            if (body && body.task) {
                                let taskId = body.task;
                                console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile: Done");
                                resolve(taskId);
                            } else {
                                console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Error:Response in task creation" + JSON.stringify(response));
                                reject("error");
                            }

                        } else {
                            console.log(this.druidHelperPrefix + logPrefix + " :ingestSpecFile:Error:Response in task creation" + JSON.stringify(response));
                            resolve("error");
                        }
                    }
                }
            });
        });
    }

    checkStatusOfTask(druidTaskId, options) {
        let logPrefix = options.logPrefix || '';
        return new Promise((resolve, reject) => {
            const postRequestBody = Object.assign({}, this.postRequestBody, {
                url: this.DRUID_API_URI + "indexer/v1/task/" + druidTaskId + "/status"
            });
            requestLib.get(postRequestBody, (error, response, body) => {
                if (error) {
                    console.log(this.druidHelperPrefix + logPrefix + " :checkStatusOfTask:Error in task status check" + JSON.stringify(error));
                    console.log(this.druidHelperPrefix + logPrefix + " :checkStatusOfTask:Error in task status check" + JSON.stringify(response));
                    reject(error);
                } else {
                    let result = {};
                    if (body && body.status) {
                        if (body.status.status) {
                            result.status = body.status.status;
                        }
                        if (body.status.duration) {
                            result.duration = body.status.duration;
                        }
                        if (body.status.errorMsg) {
                            result.errorMsg = body.status.errorMsg;
                        }
                    }
                    resolve({druidTaskId: druidTaskId, taskStatusObj: result});
                }
            });
        });
    }

    async fetchAllResults(druidJsonQuery) {
        return new Promise(async (resolve, reject) => {
            if (druidJsonQuery.queryType === "topN") {
                if (druidJsonQuery.filter.fields) {
                    let matchedIndex = druidJsonQuery.filter.fields.map(function (obj) {
                        return obj.type;
                    }).indexOf("in");
                    // If the query contains multiple filter fields use the first one as the main grouper
                    if (matchedIndex != -1) {
                        if (druidJsonQuery.filter.fields[matchedIndex].values.length > this.maxTopNThreshold) {
                            let inArrayProperties = druidJsonQuery.filter.fields[matchedIndex].values;
                            let returnedObject = [{
                                timestamp: undefined,
                                result: []
                            }];
                            let i, j, tempArray, chunk = this.maxTopNThreshold;
                            for (i = 0, j = inArrayProperties.length; i < j; i += chunk) {
                                try {
                                    tempArray = inArrayProperties.slice(i, i + chunk);
                                    druidJsonQuery.filter.fields[matchedIndex].values = tempArray;
                                    let resultsFromThisArray = await this.fetchResults(druidJsonQuery);
                                    if (resultsFromThisArray && resultsFromThisArray[0] && resultsFromThisArray[0].result) {
                                        returnedObject[0].result = returnedObject[0].result.concat(resultsFromThisArray[0].result);
                                    }
                                } catch (error) {
                                    console.log(this.druidHelperPrefix + "fetchAllResults", JSON.stringify(error));
                                    reject(error);
                                }
                            }
                            resolve(returnedObject);
                        } else {
                            try {
                                resolve(this.fetchResults(druidJsonQuery));
                            } catch (error) {
                                console.log(this.druidHelperPrefix + "fetchAllResults", JSON.stringify(error));
                                reject(error);
                            }
                        }
                    }
                } else if (druidJsonQuery.filter.type && druidJsonQuery.filter.type === "in") {
                    if (druidJsonQuery.filter.values.length > this.maxTopNThreshold) {
                        let inArrayProperties = druidJsonQuery.filter.values;
                        let returnedObject = [{
                            timestamp: undefined,
                            result: []
                        }];
                        let i, j, tempArray, chunk = this.maxTopNThreshold;
                        for (i = 0, j = inArrayProperties.length; i < j; i += chunk) {
                            try {
                                tempArray = inArrayProperties.slice(i, i + chunk);
                                druidJsonQuery.filter.values = tempArray;
                                let resultsFromThisArray = await this.fetchResults(druidJsonQuery);
                                if (resultsFromThisArray && resultsFromThisArray[0] && resultsFromThisArray[0].result) {
                                    returnedObject[0].result = returnedObject[0].result.concat(resultsFromThisArray[0].result);
                                }
                            } catch (error) {
                                console.log(this.druidHelperPrefix + "fetchAllResults", JSON.stringify(error));
                                reject(error);
                            }
                        }
                        resolve(returnedObject);
                    } else {
                        try {
                            resolve(this.fetchResults(druidJsonQuery));
                        } catch (error) {
                            console.log(this.druidHelperPrefix + "fetchAllResults", JSON.stringify(error));
                            reject(error);
                        }
                    }
                }
            } else {
                try {
                    resolve(this.fetchResults(druidJsonQuery));
                } catch (error) {
                    console.log(this.druidHelperPrefix + "fetchAllResults", JSON.stringify(error));
                    reject(error);
                }
            }
        });
    }

    fetchResults(druidJsonQuery) {
        return new Promise((resolve, reject) => {
            if (druidJsonQuery) {
                const postRequestBody = Object.assign({}, this.postRequestBody, {
                    url: this.DRUID_QUERY_URI + "v2/?pretty",
                    body: druidJsonQuery
                });
                requestLib.post(postRequestBody, (error, responseData) => {
                    if (error) {
                        console.log(this.druidHelperPrefix + "DruidHelper:fetchResults::error: " + JSON.stringify(error));
                        reject(error);
                    } else {
                        if (responseData && responseData.statusCode && responseData.statusCode !== 200) {
                            console.log(this.druidHelperPrefix + "DruidHelper:fetchResults::statusCode:error: " + JSON.stringify(responseData));
                            if (responseData.body && responseData.body.errorMessage) {
                                reject({
                                    errorMessage: responseData.body.errorMessage,
                                    statusCode: responseData.statusCode
                                });
                            } else {
                                reject({statusCode: responseData.statusCode});
                            }
                        } else {
                            if (responseData) {
                                if (responseData.body && responseData.body.error) {
                                    console.log(this.druidHelperPrefix + "DruidHelper:fetchResults::error: " + JSON.stringify(responseData.body.error));
                                    reject(responseData.body.error);
                                } else if (responseData.body) {
                                    resolve(responseData.body);
                                } else {
                                    console.log(this.druidHelperPrefix + "DruidHelper:fetchResults::error: " + JSON.stringify(responseData));
                                    reject(responseData);
                                }
                            } else {
                                let errorMessage = this.druidHelperPrefix + "fetchResults::error: No response: " + JSON.stringify(druidJsonQuery);
                                console.log(errorMessage);
                                reject(errorMessage);
                            }
                        }
                    }
                });
            } else {
                reject({errorMessage: "Please define query parameters", statusCode: 500});
            }
        });
    }

    fetchResultsSQL(druidJsonQuery) {
        return new Promise((resolve, reject) => {
            const postRequestBody = Object.assign({}, this.postRequestBody, {
                url: this.DRUID_QUERY_URI + "v2/sql",
                body: druidJsonQuery
            });
            requestLib.post(postRequestBody, (error, responseData) => {
                if (error) {
                    console.log(this.druidHelperPrefix + "fetchResultsSQL::error: " + JSON.stringify(error));
                    reject(error);
                } else {
                    if (responseData && responseData.statusCode && responseData.statusCode !== 200) {
                        console.log(this.druidHelperPrefix + "fetchResultsSQL::statusCode:error: " + JSON.stringify(responseData));
                        if (responseData.body && responseData.body.errorMessage) {
                            reject({errorMessage: responseData.body.errorMessage, statusCode: responseData.statusCode});
                        } else {
                            reject({statusCode: responseData.statusCode});
                        }
                    } else {
                        if (responseData) {
                            if (responseData.body && responseData.body.error) {
                                console.log(this.druidHelperPrefix + "fetchResults::error: " + JSON.stringify(responseData.body.error));
                                reject(responseData.body.error);
                            } else {
                                resolve(responseData.body);
                            }
                        } else {
                            let errorMessage = this.druidHelperPrefix + "fetchResults::error: No response: " + JSON.stringify(druidJsonQuery);
                            console.log(errorMessage);
                            reject(errorMessage);
                        }
                    }
                }
            });
        });
    }

    fetchAllResultsSQLScan(druidJsonQuery, offset) { //Use it only in Scan and GroupBy queries
        return new Promise(async (resolve, reject) => {
            let queryOffset = offset || this.defaultOffset;
            let queryLimit = queryOffset;
            let allDataFetched = false;
            let numberOfRequests = 0;
            let numberOfAllRows = 0;
            let countQueryData = undefined;
            // Build query to count the total number of rows
            let countQuery = buildCountQuery(druidJsonQuery.query);
            try {
                countQueryData = await this.fetchResultsSQL({query: countQuery});
            } catch (error) {
                console.log(this.druidHelperPrefix + "fetchAllResultsSQLScan:CountQuery", JSON.stringify(error));
                reject(error);
            }
            // Get number of rows for this query
            if (countQueryData && countQueryData[0] && countQueryData[0].numberOfAllRows) {
                numberOfAllRows = countQueryData[0].numberOfAllRows;
            }

            // There are no documents for this query
            if (numberOfAllRows === 0) {
                resolve([]);
                return;
            }

            let offsetForQuery = 0;
            let results = [];
            // Build new query for each offset and concat results in order to fetch all rows
            do {
                let druidQueryWithOffset = rebuildQueryWithLimitOffset(druidJsonQuery.query, offsetForQuery, queryLimit);
                try {
                    countQueryData = await this.fetchResultsSQL({query: druidQueryWithOffset});
                } catch (error) {
                    console.log(this.druidHelperPrefix + "fetchAllResultsSQLScan:FetchQueryWithOffset", JSON.stringify(error));
                    reject(error);
                }
                results = results.concat(countQueryData);
                if (results.length >= numberOfAllRows) {
                    allDataFetched = true;
                }
                offsetForQuery += queryOffset;
                numberOfRequests += 1;
            } while (allDataFetched === false || numberOfRequests >= this.maxNumberOfIterationsInScanOffsets);

            console.log(this.druidHelperPrefix + "fetchAllResultsSQLScan: " + numberOfAllRows + " rows fetched  with " + numberOfRequests + " requests");
            resolve(results);
            return;

            function rebuildQueryWithLimitOffset(sqlQuery, offset, limit) {
                // Concat limit and offset params
                sqlQuery = sqlQuery + " LIMIT " + limit + " OFFSET " + offset;
                return sqlQuery;
            }

            function buildCountQuery(sqlQuery) {
                // Add count(*) in select
                let fromSplitArray = sqlQuery.split("FROM");
                sqlQuery = "SELECT COUNT(*) AS numberOfAllRows FROM" + fromSplitArray[1];
                // Remove group by
                let groupBySplitArray = sqlQuery.split("GROUP BY");
                if (groupBySplitArray.length > 0) {
                    sqlQuery = groupBySplitArray[0];
                }
                return sqlQuery;
            }
        });
    }

    updateLookupTable(lookupTable, lookupTableJsonConfiguration, options) {
        let logPrefix = options && options.logPrefix ? options.logPrefix : '';
        return new Promise((resolve, reject) => {
            console.log(this.druidHelperPrefix + logPrefix + " :updateLookupTable:Info file");
            const postRequestBody = Object.assign({}, this.postRequestBody, {
                url: this.DRUID_API_URI + "coordinator/v1/lookups/config/__default/" + lookupTable,
                body: lookupTableJsonConfiguration
            });
            requestLib.post(postRequestBody, (error, response, body) => {
                if (error) {
                    console.log(this.druidHelperPrefix + logPrefix + " :updateLookupTable:Error in lookup creation" + JSON.stringify(error));
                    console.log(this.druidHelperPrefix + logPrefix + " :updateLookupTable:Error: Response in lookup creation" + JSON.stringify(response));
                    reject(error);
                } else {
                    if (body && body.error) {
                        console.log(this.druidHelperPrefix + logPrefix + " :updateLookupTable:Error: Response in lookup creation" + JSON.stringify(response));
                        reject(body.error);
                    } else {
                        resolve(body);
                    }
                }
            });
        });
    }

    getArrayAsString(arrayWithValues) {
        if (arrayWithValues && arrayWithValues.length > 0) {
            return "'" + arrayWithValues.join("','") + "'";
        } else {
            return "";
        }
    }
}

module.exports = DruidHelper;
